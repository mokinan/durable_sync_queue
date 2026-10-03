/// A persistent, ordered, retrying operation queue for offline-first apps.
///
/// Write locally, [SyncQueue.enqueue] the server call, and let the queue
/// deliver it — in order, exactly once from the server's point of view
/// (via idempotency keys), with backoff when the network misbehaves.
library;

export 'src/backoff.dart';
export 'src/outcome.dart';
export 'src/queue_event.dart';
export 'src/queue_store.dart';
export 'src/queued_operation.dart';
export 'src/sync_queue.dart';
