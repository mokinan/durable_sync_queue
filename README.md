# durable_sync_queue

[![CI](https://github.com/mokinan/durable_sync_queue/actions/workflows/ci.yml/badge.svg)](https://github.com/mokinan/durable_sync_queue/actions/workflows/ci.yml)

A persistent, ordered, retrying operation queue for offline-first Dart and
Flutter apps.

Write to your local database, enqueue the server call, and let the queue
deliver it. Delivery is in order, with backoff when the network misbehaves,
and safe to retry because every operation carries a stable idempotency key.

```dart
final queue = SyncQueue(
  store: myStore,                        // your database, see below
  handler: (op) async {
    final response = await dio.post(
      '/orders',
      data: op.payload,
      options: Options(headers: {'Idempotency-Key': op.idempotencyKey}),
    );
    return const Outcome.delivered();
  },
  onError: (e) => e is DioException && (e.response?.statusCode ?? 500) < 500
      ? Outcome.rejected('${e.response?.statusCode}')
      : Outcome.retryLater('$e'),
)..start();

await queue.enqueue('create_order', order.toJson(), group: 'order:${order.id}');
```

## Why

"Save offline, sync later" is easy to demo and hard to get right. This
package handles the parts that cause real production bugs:

| Problem | What the queue does |
|---|---|
| The response is lost after the server committed, so a retry duplicates the write | A stable **idempotency key** per operation, reused on every attempt |
| An "update" reaches the server before its "create" | **Strict ordering within a group**; groups are independent, so one stuck entity doesn't block the rest |
| Thousands of clients retry at the same second after an outage | **Exponential backoff with jitter**, plus support for server `Retry-After` |
| One bad payload blocks the queue forever | **Permanent rejections are dead-lettered** and the group moves on; transient failures give up after `maxAttempts` |
| The user deletes something that may or may not have reached the server | `cancel()` only succeeds if the operation was **never attempted**, because the attempt counter is persisted before sending |
| Retries wait out their backoff even after the connection is back | `drain(force: true)` on reconnect skips backoff but keeps order |

## Install

```bash
dart pub add durable_sync_queue
```

## Concepts

**Groups.** Every operation belongs to a group (default: `'default'`).
Operations in a group are delivered strictly in insertion order. A transient
failure pauses only that group. Use the id of the entity being changed, for
example `group: 'invoice:42'`.

**Outcomes.** Your handler returns one of these:

| Outcome | Use for | Effect |
|---|---|---|
| `Outcome.delivered()` | 2xx | removed from the queue |
| `Outcome.rejected(reason)` | 4xx validation errors | dead-lettered; the group continues |
| `Outcome.retryLater(reason, after: …)` | timeouts, 5xx, 429, offline | the group pauses until the backoff (or `after`) elapses |

Exceptions thrown by the handler go through `onError`. By default they are
treated as `retryLater`.

**Draining.** `start()` drains immediately, after every `enqueue`, and
whenever a retry falls due. Call `drain(force: true)` when connectivity
returns. Concurrent drains share a single pass.

**Inspecting.** `pending()`, `deadLetters()`, `watchPendingCount()` (for a
"3 changes waiting to sync" badge), and the `events` stream.

## Storage

Implement `QueueStore` (three methods) on top of the database you already
use. Ideally, write the queue entry **in the same transaction** as the local
change, so the two can never diverge.

```dart
class DriftQueueStore implements QueueStore {
  DriftQueueStore(this.db);
  final AppDatabase db;

  @override
  Future<List<QueuedOperation>> loadAll() async =>
      (await db.select(db.queue).get()).map((r) => QueuedOperation.fromJson(jsonDecode(r.json))).toList();

  @override
  Future<void> save(QueuedOperation op) =>
      db.into(db.queue).insertOnConflictUpdate(QueueCompanion.insert(id: op.id, json: jsonEncode(op.toJson())));

  @override
  Future<void> delete(String id) => (db.delete(db.queue)..where((r) => r.id.equals(id))).go();
}
```

Calling `enqueue` **inside** a database transaction is supported. Automatic
drains run in the zone where the queue was created, so they never inherit
the transaction's zone. This matters for Drift, whose transactions are bound
to a zone.

Included:

- `InMemoryQueueStore`, for tests and prototypes.
- `JsonFileQueueStore` (`import 'package:durable_sync_queue/io.dart'`), which
  writes atomically (temp file plus rename). It suits small queues on
  `dart:io` platforms. It is kept out of the main library so the core stays
  web-compatible.

## Server side

Idempotency only works if the server participates. It should store the
response for each `Idempotency-Key` and return it for repeated requests
without executing them again. This is standard on payment APIs (Stripe,
Adyen) and simple to add elsewhere.

## Background

This package was extracted from the sync engine of
[flutter-fintech-wallet](https://github.com/mokinan/flutter-fintech-wallet),
where these rules are exercised end-to-end against a server that drops
responses on purpose.

## License

MIT
