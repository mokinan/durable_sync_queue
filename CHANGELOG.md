## 0.1.2

- Fix: automatic drains (after `enqueue`/`retry` and on retry timers) now run
  in the zone the queue was created in. Previously, enqueueing inside a
  database transaction (e.g. Drift's zone-bound transactions) made the drain
  reuse the committed transaction and hang.

## 0.1.1

- Fix: `drain(force: true)` could be absorbed by a regular pass that started
  while it was waiting, leaving operations in backoff after reconnecting.

## 0.1.0

- `SyncQueue` with per-group ordering, idempotency keys, exponential backoff
  with jitter, dead-lettering and `maxAttempts`.
- `drain(force: true)` to skip backoff when connectivity returns.
- `cancel()` that only discards operations the server has never seen.
- `QueueStore` interface with `InMemoryQueueStore` and `JsonFileQueueStore`
  (in `io.dart`).
- `events` stream and `watchPendingCount()`.
