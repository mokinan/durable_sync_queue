import 'dart:async';

import 'package:offline_sync_queue/offline_sync_queue.dart';
import 'package:test/test.dart';

/// A server that remembers idempotency keys and can be told to misbehave.
class FakeServer {
  final applied = <String>[];
  final _seenKeys = <String>{};
  final failNext = <String, Outcome>{};
  bool loseResponses = false;

  Future<Outcome> handle(QueuedOperation op) async {
    final forced = failNext.remove(op.payload['name']);
    if (forced != null) return forced;
    if (_seenKeys.add(op.idempotencyKey)) {
      applied.add('${op.type}:${op.payload['name']}');
    }
    if (loseResponses) throw TimeoutException('response lost');
    return const Outcome.delivered();
  }
}

class TestClock {
  DateTime now = DateTime(2026);
  DateTime call() => now;
  void advance(Duration d) => now = now.add(d);
}

void main() {
  late FakeServer server;
  late TestClock clock;
  late InMemoryQueueStore store;
  late SyncQueue queue;
  var ids = 0;

  SyncQueue build({int maxAttempts = 5}) => SyncQueue(
        store: store,
        handler: server.handle,
        backoff: const ConstantBackoff(Duration(seconds: 10)),
        maxAttempts: maxAttempts,
        clock: clock.call,
        generateId: () => 'id${++ids}',
      );

  setUp(() {
    server = FakeServer();
    clock = TestClock();
    store = InMemoryQueueStore();
    queue = build();
  });

  test('delivers in insertion order and empties the queue', () async {
    await queue.enqueue('create', {'name': 'a'});
    await queue.enqueue('create', {'name': 'b'});

    final report = await queue.drain();

    expect(report.delivered, 2);
    expect(server.applied, ['create:a', 'create:b']);
    expect(await queue.pending(), isEmpty);
    expect(report.nextRetryAt, isNull);
  });

  test('a retry after a lost response is applied once', () async {
    await queue.enqueue('pay', {'name': 'invoice-1'});
    server.loseResponses = true;

    final first = await queue.drain();
    expect(first.retryScheduled, 1);
    expect(server.applied, ['pay:invoice-1']);

    server.loseResponses = false;
    clock.advance(const Duration(seconds: 10));
    final second = await queue.drain();

    expect(second.delivered, 1);
    expect(server.applied, ['pay:invoice-1'], reason: 'same idempotency key');
  });

  test('a transient failure pauses only its own group', () async {
    await queue.enqueue('create', {'name': 'a1'}, group: 'a');
    await queue.enqueue('update', {'name': 'a2'}, group: 'a');
    await queue.enqueue('create', {'name': 'b1'}, group: 'b');
    server.failNext['a1'] = const Outcome.retryLater('503');

    final report = await queue.drain();

    expect(server.applied, ['create:b1'],
        reason: 'a2 must wait for a1; b is independent');
    expect(report.retryScheduled, 1);
    expect(report.nextRetryAt, clock.now.add(const Duration(seconds: 10)));

    clock.advance(const Duration(seconds: 10));
    await queue.drain();
    expect(server.applied, ['create:b1', 'create:a1', 'update:a2']);
  });

  test('nothing is sent before the retry is due', () async {
    await queue.enqueue('create', {'name': 'a'});
    server.failNext['a'] = const Outcome.retryLater('503');
    await queue.drain();

    clock.advance(const Duration(seconds: 9));
    expect((await queue.drain()).delivered, 0);

    clock.advance(const Duration(seconds: 1));
    expect((await queue.drain()).delivered, 1);
  });

  test('a forced drain ignores backoff but keeps order', () async {
    await queue.enqueue('create', {'name': 'a1'}, group: 'a');
    await queue.enqueue('update', {'name': 'a2'}, group: 'a');
    server.failNext['a1'] = const Outcome.retryLater('offline');
    await queue.drain();

    expect((await queue.drain()).delivered, 0);
    final forced = await queue.drain(force: true);

    expect(forced.delivered, 2);
    expect(server.applied, ['create:a1', 'update:a2']);
  });

  test('honours a server-provided retry delay', () async {
    await queue.enqueue('create', {'name': 'a'});
    server.failNext['a'] =
        const Outcome.retryLater('429', after: Duration(minutes: 1));
    final report = await queue.drain();
    expect(report.nextRetryAt, clock.now.add(const Duration(minutes: 1)));
  });

  test('a rejection dead-letters the operation and the group continues',
      () async {
    await queue.enqueue('create', {'name': 'bad'});
    await queue.enqueue('create', {'name': 'good'});
    server.failNext['bad'] = const Outcome.rejected('422 invalid');

    final report = await queue.drain();

    expect(report.deadLettered, 1);
    expect(server.applied, ['create:good']);
    final dead = await queue.deadLetters();
    expect(dead.single.lastError, '422 invalid');
    expect(await queue.pending(), isEmpty);
  });

  test('gives up after maxAttempts', () async {
    queue = build(maxAttempts: 2);
    await queue.enqueue('create', {'name': 'a'});
    server.loseResponses = true;

    await queue.drain();
    clock.advance(const Duration(seconds: 10));
    final report = await queue.drain();

    expect(report.deadLettered, 1);
    expect((await queue.deadLetters()).single.lastError,
        startsWith('Gave up after 2 attempts'));
  });

  test('a dead letter can be retried', () async {
    await queue.enqueue('create', {'name': 'a'});
    server.failNext['a'] = const Outcome.rejected('no');
    await queue.drain();
    final dead = (await queue.deadLetters()).single;

    expect(await queue.retry(dead.id), isTrue);
    expect((await queue.drain()).delivered, 1);
    expect(await queue.retry('missing'), isFalse);
  });

  test('handler exceptions are mapped by onError', () async {
    queue = SyncQueue(
      store: store,
      handler: (_) async => throw StateError('bad payload'),
      onError: (e) => Outcome.rejected('mapped: $e'),
      clock: clock.call,
    );
    await queue.enqueue('create', {'name': 'a'});
    await queue.drain();
    expect((await queue.deadLetters()).single.lastError,
        contains('mapped: Bad state: bad payload'));
  });

  group('cancel', () {
    test('drops an operation the server never saw', () async {
      final op = await queue.enqueue('create', {'name': 'a'});
      expect(await queue.cancel(op.id), isTrue);
      expect(await queue.drain(),
          isA<DrainReport>().having((r) => r.delivered, 'delivered', 0));
      expect(server.applied, isEmpty);
    });

    test('refuses once an attempt was made', () async {
      final op = await queue.enqueue('create', {'name': 'a'});
      server.loseResponses = true;
      await queue.drain();
      expect(await queue.cancel(op.id), isFalse,
          reason: 'the server may have applied it');
    });

    test('refuses while the operation is in flight', () async {
      final gate = Completer<Outcome>();
      queue = SyncQueue(
          store: store, handler: (_) => gate.future, clock: clock.call);
      final op = await queue.enqueue('create', {'name': 'a'});
      final draining = queue.drain();
      await Future<void>.delayed(Duration.zero);

      expect(await queue.cancel(op.id), isFalse);
      gate.complete(const Outcome.delivered());
      await draining;
    });
  });

  test('concurrent drains share one pass', () async {
    await queue.enqueue('create', {'name': 'a'});
    final reports =
        await Future.wait([queue.drain(), queue.drain(), queue.drain()]);
    expect(reports.map((r) => r.delivered), [1, 1, 1]);
    expect(server.applied, ['create:a']);
  });

  test('restores state from the store, keeping order', () async {
    await queue.enqueue('create', {'name': 'a'});
    await queue.enqueue('create', {'name': 'b'});

    final restarted = build();
    await restarted.enqueue('create', {'name': 'c'});
    await restarted.drain();

    expect(server.applied, ['create:a', 'create:b', 'create:c']);
  });

  test('emits events and pending counts', () async {
    final events = <String>[];
    queue.events.listen((e) => events.add(e.runtimeType.toString()));
    final counts = <int>[];
    final sub = queue.watchPendingCount().listen(counts.add);
    await Future<void>.delayed(Duration.zero);

    final op = await queue.enqueue('create', {'name': 'a'});
    await queue.enqueue('create', {'name': 'b'});
    await queue.cancel(op.id);
    await queue.drain();
    await Future<void>.delayed(Duration.zero);

    expect(events, [
      'OperationEnqueued',
      'OperationEnqueued',
      'OperationCancelled',
      'OperationDelivered'
    ]);
    expect(counts.first, 0);
    expect(counts.last, 0);
    expect(counts, contains(2));
    await sub.cancel();
  });

  test('start() drains on enqueue and when a retry falls due', () async {
    queue = SyncQueue(
      store: store,
      handler: server.handle,
      backoff: const ConstantBackoff(Duration(milliseconds: 20)),
    );
    server.failNext['a'] = const Outcome.retryLater('503');
    queue.start();

    final delivered = queue.events.firstWhere((e) => e is OperationDelivered);
    await queue.enqueue('create', {'name': 'a'});

    await delivered.timeout(const Duration(seconds: 2));
    expect(server.applied, ['create:a']);
    await queue.dispose();
  });
}
