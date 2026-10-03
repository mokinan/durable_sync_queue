## 1.0.0

- `SyncQueue` with per-group ordering, idempotency keys, exponential backoff
  with jitter, dead-lettering and `maxAttempts`.
- `drain(force: true)` to skip backoff when connectivity returns.
- `cancel()` that only discards operations the server has never seen.
- `QueueStore` interface with `InMemoryQueueStore` and `JsonFileQueueStore`
  (in `io.dart`).
- `events` stream and `watchPendingCount()`.
