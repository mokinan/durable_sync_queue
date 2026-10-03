import 'dart:async';
import 'dart:math';

import 'backoff.dart';
import 'outcome.dart';
import 'queue_event.dart';
import 'queue_store.dart';
import 'queued_operation.dart';

/// Delivers one operation to the server.
///
/// Return an [Outcome]. Exceptions are mapped by `SyncQueue.onError`
/// (by default: retry later).
typedef OperationHandler = Future<Outcome> Function(QueuedOperation operation);

/// Summary of one [SyncQueue.drain] pass.
final class DrainReport {
  /// Creates a report.
  const DrainReport(
      {this.delivered = 0,
      this.deadLettered = 0,
      this.retryScheduled = 0,
      this.nextRetryAt});

  /// Operations accepted by the server.
  final int delivered;

  /// Operations rejected or out of attempts.
  final int deadLettered;

  /// Transient failures, each pausing its group.
  final int retryScheduled;

  /// When the earliest pending operation becomes due, if any remain.
  final DateTime? nextRetryAt;

  @override
  String toString() =>
      'DrainReport(delivered: $delivered, deadLettered: $deadLettered, retryScheduled: $retryScheduled, nextRetryAt: $nextRetryAt)';
}

/// A persistent, ordered, retrying operation queue.
///
/// Guarantees:
/// * **Order within a group.** An operation is never sent while an earlier
///   operation in the same group is still pending, so "update" cannot
///   overtake "create". Groups are independent: one stuck group does not
///   block the others.
/// * **Safe retries.** Each operation carries a stable idempotency key; send
///   it to your server so a retry after a lost response is deduplicated.
/// * **Honest bookkeeping.** The attempt counter is persisted *before* each
///   send, so `cancel` only discards operations the server has provably never
///   seen.
/// * **One pass at a time.** Concurrent [drain] calls share the same pass.
class SyncQueue {
  /// Creates a queue.
  ///
  /// [maxAttempts] bounds transient retries before an operation is
  /// dead-lettered. [onError] maps exceptions thrown by [handler].
  SyncQueue({
    required QueueStore store,
    required OperationHandler handler,
    BackoffPolicy? backoff,
    this.maxAttempts = 10,
    Outcome Function(Object error)? onError,
    DateTime Function()? clock,
    String Function()? generateId,
  })  : _store = store,
        _handler = handler,
        _backoff = backoff ?? ExponentialBackoff(),
        _onError = onError ?? ((error) => Outcome.retryLater('$error')),
        _clock = clock ?? DateTime.now,
        _generateId = generateId ?? _randomId;

  final QueueStore _store;
  final OperationHandler _handler;
  final BackoffPolicy _backoff;
  final Outcome Function(Object error) _onError;
  final DateTime Function() _clock;
  final String Function() _generateId;

  /// Attempts after which a transiently failing operation is dead-lettered.
  final int maxAttempts;

  final _operations = <String, QueuedOperation>{};
  final _events = StreamController<QueueEvent>.broadcast();
  final _pendingCount = StreamController<int>.broadcast();
  Future<void>? _loading;
  Future<DrainReport>? _draining;
  String? _inFlightId;
  int _sequence = 0;
  Timer? _timer;
  bool _autoRetry = false;

  /// Everything that happens to operations.
  Stream<QueueEvent> get events => _events.stream;

  /// Operations waiting to be delivered, oldest first.
  Future<List<QueuedOperation>> pending() async {
    await _load();
    return _sorted(OperationStatus.pending);
  }

  /// Operations that were rejected or ran out of attempts.
  Future<List<QueuedOperation>> deadLetters() async {
    await _load();
    return _sorted(OperationStatus.deadLettered);
  }

  /// Emits the number of pending operations now and after every change.
  Stream<int> watchPendingCount() async* {
    await _load();
    yield _countPending();
    yield* _pendingCount.stream;
  }

  /// Adds an operation and, if auto-retry is on, triggers a drain.
  ///
  /// [group] defines the ordering scope — typically the id of the entity the
  /// operation touches. Pass an [idempotencyKey] if you already have one.
  Future<QueuedOperation> enqueue(
    String type,
    Map<String, Object?> payload, {
    String group = 'default',
    String? idempotencyKey,
  }) async {
    await _load();
    final now = _clock();
    final operation = QueuedOperation(
      id: _generateId(),
      type: type,
      payload: payload,
      idempotencyKey: idempotencyKey ?? _generateId(),
      group: group,
      sequence: ++_sequence,
      createdAt: now,
      nextAttemptAt: now,
    );
    await _put(operation);
    _events.add(OperationEnqueued(operation));
    if (_autoRetry) unawaited(drain());
    return operation;
  }

  /// Removes an operation the server has never seen.
  ///
  /// Returns `false` — and keeps the operation — if it was already attempted
  /// or is being sent right now: the server may have applied it, so the
  /// caller must enqueue a compensating operation instead.
  Future<bool> cancel(String id) async {
    await _load();
    final operation = _operations[id];
    if (operation == null || id == _inFlightId) return false;
    if (operation.status == OperationStatus.pending && !operation.neverSent) {
      return false;
    }
    await _remove(id);
    _events.add(OperationCancelled(operation));
    return true;
  }

  /// Moves a dead-lettered operation back to pending, due immediately.
  Future<bool> retry(String id) async {
    await _load();
    final operation = _operations[id];
    if (operation == null || operation.status != OperationStatus.deadLettered) {
      return false;
    }
    await _put(operation.copyWith(
        status: OperationStatus.pending, nextAttemptAt: _clock(), attempts: 0));
    if (_autoRetry) unawaited(drain());
    return true;
  }

  /// Sends every due operation; concurrent callers share one pass.
  ///
  /// Pass `force: true` when connectivity returns: backoff delays are
  /// ignored, because the reason for waiting is gone. Ordering is still
  /// respected.
  Future<DrainReport> drain({bool force = false}) async {
    if (!force) {
      return _draining ??=
          _drain(force: false).whenComplete(() => _draining = null);
    }
    // A forced pass must not be swallowed by a regular one: wait until no
    // pass is running (another may start while we wait), then claim the
    // slot synchronously.
    while (_draining != null) {
      await _draining;
    }
    return _draining = _drain(force: true).whenComplete(() => _draining = null);
  }

  /// Drains now, after every enqueue, and again whenever a retry falls due.
  ///
  /// Call [drain] yourself when connectivity returns.
  void start() {
    _autoRetry = true;
    unawaited(drain());
  }

  /// Stops automatic draining and closes the streams.
  Future<void> dispose() async {
    _autoRetry = false;
    _timer?.cancel();
    await _draining;
    await _events.close();
    await _pendingCount.close();
  }

  Future<DrainReport> _drain({required bool force}) async {
    await _load();
    var delivered = 0, deadLettered = 0, retried = 0;
    final blocked = <String>{};

    while (true) {
      final next = _nextDue(blocked, force: force);
      if (next == null) break;

      // Persist the attempt before sending: from here on the server may
      // have seen this operation even if we never hear back.
      final attempt = next.copyWith(attempts: next.attempts + 1);
      await _put(attempt);

      _inFlightId = attempt.id;
      Outcome outcome;
      try {
        outcome = await _handler(attempt);
      } on Object catch (error) {
        outcome = _onError(error);
      } finally {
        _inFlightId = null;
      }

      switch (outcome) {
        case Delivered():
          delivered++;
          await _remove(attempt.id);
          _events.add(OperationDelivered(attempt));
        case Rejected(:final reason):
          deadLettered++;
          final dead = attempt.copyWith(
              status: OperationStatus.deadLettered, lastError: reason);
          await _put(dead);
          _events.add(OperationDeadLettered(dead));
        case RetryLater(:final reason, :final after):
          if (attempt.attempts >= maxAttempts) {
            deadLettered++;
            final dead = attempt.copyWith(
              status: OperationStatus.deadLettered,
              lastError: 'Gave up after ${attempt.attempts} attempts: $reason',
            );
            await _put(dead);
            _events.add(OperationDeadLettered(dead));
          } else {
            retried++;
            blocked.add(attempt.group);
            final scheduled = attempt.copyWith(
              nextAttemptAt:
                  _clock().add(after ?? _backoff.delay(attempt.attempts)),
              lastError: reason,
            );
            await _put(scheduled);
            _events.add(OperationRetryScheduled(scheduled));
          }
      }
    }

    final nextRetryAt = _earliestPending();
    _scheduleTimer(nextRetryAt);
    return DrainReport(
      delivered: delivered,
      deadLettered: deadLettered,
      retryScheduled: retried,
      nextRetryAt: nextRetryAt,
    );
  }

  /// The oldest pending operation that is due and heads its group.
  QueuedOperation? _nextDue(Set<String> blocked, {required bool force}) {
    final now = _clock();
    final due = _groupHeads()
        .values
        .where((op) =>
            !blocked.contains(op.group) &&
            (force || !op.nextAttemptAt.isAfter(now)))
        .toList()
      ..sort((a, b) => a.sequence.compareTo(b.sequence));
    return due.firstOrNull;
  }

  /// When the next group head becomes due. Operations queued behind a
  /// waiting head cannot run before it, so only heads count.
  DateTime? _earliestPending() {
    DateTime? earliest;
    for (final head in _groupHeads().values) {
      if (earliest == null || head.nextAttemptAt.isBefore(earliest)) {
        earliest = head.nextAttemptAt;
      }
    }
    return earliest;
  }

  Map<String, QueuedOperation> _groupHeads() {
    final heads = <String, QueuedOperation>{};
    for (final op in _sorted(OperationStatus.pending)) {
      heads.putIfAbsent(op.group, () => op);
    }
    return heads;
  }

  void _scheduleTimer(DateTime? at) {
    _timer?.cancel();
    if (!_autoRetry || at == null) return;
    final delay = at.difference(_clock());
    _timer = Timer(
        delay.isNegative ? Duration.zero : delay, () => unawaited(drain()));
  }

  List<QueuedOperation> _sorted(OperationStatus status) =>
      _operations.values.where((op) => op.status == status).toList()
        ..sort((a, b) => a.sequence.compareTo(b.sequence));

  int _countPending() => _operations.values
      .where((op) => op.status == OperationStatus.pending)
      .length;

  Future<void> _load() => _loading ??= () async {
        for (final op in await _store.loadAll()) {
          _operations[op.id] = op;
          _sequence = max(_sequence, op.sequence);
        }
      }();

  Future<void> _put(QueuedOperation operation) async {
    await _store.save(operation);
    _operations[operation.id] = operation;
    if (!_pendingCount.isClosed) _pendingCount.add(_countPending());
  }

  Future<void> _remove(String id) async {
    await _store.delete(id);
    _operations.remove(id);
    if (!_pendingCount.isClosed) _pendingCount.add(_countPending());
  }

  static final _random = Random.secure();

  /// RFC 4122 version 4 UUID.
  static String _randomId() {
    final bytes = List<int>.generate(16, (_) => _random.nextInt(256));
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    final hex = bytes.map((b) => b.toRadixString(16).padLeft(2, '0')).join();
    return '${hex.substring(0, 8)}-${hex.substring(8, 12)}-${hex.substring(12, 16)}-'
        '${hex.substring(16, 20)}-${hex.substring(20)}';
  }
}
