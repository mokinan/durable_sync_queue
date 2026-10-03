import 'dart:async';

import 'package:offline_sync_queue/offline_sync_queue.dart';

/// Simulates an app that writes while the network is unreliable.
Future<void> main() async {
  var online = false;

  final queue = SyncQueue(
    store: InMemoryQueueStore(), // Use your database in a real app.
    backoff: const ConstantBackoff(Duration(milliseconds: 200)),
    handler: (op) async {
      if (!online) return const Outcome.retryLater('offline');
      // In a real app:
      // await dio.post('/orders', data: op.payload,
      //     options: Options(headers: {'Idempotency-Key': op.idempotencyKey}));
      print(
          'sent ${op.type} ${op.payload} (key ${op.idempotencyKey.substring(0, 8)}…)');
      return const Outcome.delivered();
    },
    onError: (error) => Outcome.retryLater('$error'),
  );

  queue.events.listen((event) => print('event: ${event.runtimeType}'));
  queue.start();

  // Operations on the same order stay in order; other orders are independent.
  await queue.enqueue('create_order', {'id': 'o-1', 'total': 4999},
      group: 'order:o-1');
  await queue.enqueue('add_note', {'id': 'o-1', 'note': 'Leave at door'},
      group: 'order:o-1');
  await queue.enqueue('create_order', {'id': 'o-2', 'total': 1250},
      group: 'order:o-2');

  await Future<void>.delayed(const Duration(milliseconds: 300));
  print('pending while offline: ${(await queue.pending()).length}');

  online = true;
  // e.g. when connectivity_plus reports a connection: skip the backoff.
  await queue.drain(force: true);
  print('pending after reconnect: ${(await queue.pending()).length}');

  await queue.dispose();
}
