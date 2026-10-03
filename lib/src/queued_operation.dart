import 'package:meta/meta.dart';

/// Lifecycle of a queued operation.
enum OperationStatus {
  /// Waiting to be sent (possibly after a backoff delay).
  pending,

  /// Rejected permanently or out of attempts; kept for inspection.
  deadLettered,
}

/// A unit of work waiting to reach the server.
@immutable
final class QueuedOperation {
  /// Creates an operation. Usually built by `SyncQueue.enqueue`.
  const QueuedOperation({
    required this.id,
    required this.type,
    required this.payload,
    required this.idempotencyKey,
    required this.group,
    required this.sequence,
    required this.createdAt,
    required this.nextAttemptAt,
    this.attempts = 0,
    this.status = OperationStatus.pending,
    this.lastError,
  });

  /// Restores an operation saved with [toJson].
  factory QueuedOperation.fromJson(Map<String, Object?> json) =>
      QueuedOperation(
        id: json['id']! as String,
        type: json['type']! as String,
        payload: (json['payload']! as Map).cast<String, Object?>(),
        idempotencyKey: json['idempotencyKey']! as String,
        group: json['group']! as String,
        sequence: json['sequence']! as int,
        createdAt: DateTime.parse(json['createdAt']! as String),
        nextAttemptAt: DateTime.parse(json['nextAttemptAt']! as String),
        attempts: json['attempts']! as int,
        status: OperationStatus.values.byName(json['status']! as String),
        lastError: json['lastError'] as String?,
      );

  /// Unique id of this queue entry.
  final String id;

  /// What to do, e.g. `create_order`. Interpreted by your handler.
  final String type;

  /// JSON-encodable data for the handler.
  final Map<String, Object?> payload;

  /// Stable across retries; send it to the server so retries are deduplicated.
  final String idempotencyKey;

  /// Operations in the same group are delivered strictly in order; different
  /// groups are independent.
  final String group;

  /// Monotonic insertion order across the whole queue.
  final int sequence;

  /// When the operation was enqueued.
  final DateTime createdAt;

  /// Earliest time the next attempt may run.
  final DateTime nextAttemptAt;

  /// Attempts started so far. Incremented *before* each send, so a value
  /// above zero means the server may already have seen this operation.
  final int attempts;

  /// Current status.
  final OperationStatus status;

  /// Description of the last failure, if any.
  final String? lastError;

  /// Whether the server can be assumed never to have received this operation.
  bool get neverSent => attempts == 0;

  /// Returns a copy with the given fields replaced.
  QueuedOperation copyWith({
    DateTime? nextAttemptAt,
    int? attempts,
    OperationStatus? status,
    String? lastError,
  }) =>
      QueuedOperation(
        id: id,
        type: type,
        payload: payload,
        idempotencyKey: idempotencyKey,
        group: group,
        sequence: sequence,
        createdAt: createdAt,
        nextAttemptAt: nextAttemptAt ?? this.nextAttemptAt,
        attempts: attempts ?? this.attempts,
        status: status ?? this.status,
        lastError: lastError ?? this.lastError,
      );

  /// Serializes for persistence.
  Map<String, Object?> toJson() => {
        'id': id,
        'type': type,
        'payload': payload,
        'idempotencyKey': idempotencyKey,
        'group': group,
        'sequence': sequence,
        'createdAt': createdAt.toIso8601String(),
        'nextAttemptAt': nextAttemptAt.toIso8601String(),
        'attempts': attempts,
        'status': status.name,
        'lastError': lastError,
      };

  @override
  String toString() =>
      'QueuedOperation($type #$sequence, group: $group, attempts: $attempts, ${status.name})';
}
