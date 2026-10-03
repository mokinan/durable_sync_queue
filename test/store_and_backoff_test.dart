import 'dart:io';
import 'dart:math';

import 'package:durable_sync_queue/io.dart';
import 'package:durable_sync_queue/durable_sync_queue.dart';
import 'package:test/test.dart';

QueuedOperation _op(String id, int sequence) => QueuedOperation(
      id: id,
      type: 'create',
      payload: {
        'n': sequence,
        'nested': {'ok': true}
      },
      idempotencyKey: 'key-$id',
      group: 'g',
      sequence: sequence,
      createdAt: DateTime.utc(2026, 1, 1),
      nextAttemptAt: DateTime.utc(2026, 1, 1, 0, 0, sequence),
    );

void main() {
  group('QueuedOperation', () {
    test('round-trips through JSON', () {
      final op = _op('a', 3).copyWith(
          attempts: 2, status: OperationStatus.deadLettered, lastError: 'x');
      final restored = QueuedOperation.fromJson(op.toJson());
      expect(restored.toJson(), op.toJson());
      expect(restored.neverSent, isFalse);
      expect(restored.toString(), contains('deadLettered'));
    });
  });

  group('JsonFileQueueStore', () {
    late Directory dir;

    setUp(
        () async => dir = await Directory.systemTemp.createTemp('queue_test'));
    tearDown(() => dir.delete(recursive: true));

    test('persists across instances', () async {
      final file = File('${dir.path}/nested/queue.json');
      final store = JsonFileQueueStore(file);
      await Future.wait([
        store.save(_op('a', 1)),
        store.save(_op('b', 2)),
        store.save(_op('c', 3))
      ]);
      await store.delete('b');

      final reopened = await JsonFileQueueStore(file).loadAll();
      expect(reopened.map((o) => o.id), unorderedEquals(['a', 'c']));
      expect(reopened.firstWhere((o) => o.id == 'a').payload['nested'],
          {'ok': true});
    });

    test('starts empty when the file does not exist', () async {
      expect(await JsonFileQueueStore(File('${dir.path}/none.json')).loadAll(),
          isEmpty);
    });

    test('backs a SyncQueue across restarts', () async {
      final file = File('${dir.path}/queue.json');
      final sent = <String>[];
      Future<Outcome> handler(QueuedOperation op) async {
        sent.add(op.payload['name']! as String);
        return const Outcome.delivered();
      }

      final first =
          SyncQueue(store: JsonFileQueueStore(file), handler: handler);
      await first.enqueue('create', {'name': 'offline-1'});
      await first.enqueue('create', {'name': 'offline-2'});

      // "App restart": a fresh queue reads the file.
      final second =
          SyncQueue(store: JsonFileQueueStore(file), handler: handler);
      await second.drain();
      expect(sent, ['offline-1', 'offline-2']);
      expect(await JsonFileQueueStore(file).loadAll(), isEmpty);
    });
  });

  group('ExponentialBackoff', () {
    test('grows exponentially with jitter and is capped', () {
      final policy = ExponentialBackoff(
        base: const Duration(seconds: 1),
        max: const Duration(seconds: 30),
        random: Random(1),
      );
      for (final (attempt, ceiling) in [
        (1, 1000),
        (2, 2000),
        (3, 4000),
        (5, 16000),
        (10, 30000),
        (60, 30000)
      ]) {
        final ms = policy.delay(attempt).inMilliseconds;
        expect(ms, inInclusiveRange(ceiling ~/ 2, ceiling),
            reason: 'attempt $attempt');
      }
    });

    test('constant backoff is constant', () {
      expect(const ConstantBackoff(Duration(seconds: 3)).delay(9),
          const Duration(seconds: 3));
    });
  });
}
