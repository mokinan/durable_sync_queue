import 'queued_operation.dart';

/// Something that happened to an operation; listen via `SyncQueue.events`.
sealed class QueueEvent {
  const QueueEvent(this.operation);

  /// The operation concerned.
  final QueuedOperation operation;
}

/// An operation was added.
final class OperationEnqueued extends QueueEvent {
  /// Creates the event.
  const OperationEnqueued(super.operation);
}

/// The server accepted an operation.
final class OperationDelivered extends QueueEvent {
  /// Creates the event.
  const OperationDelivered(super.operation);
}

/// A transient failure; the operation will be retried at
/// `operation.nextAttemptAt`.
final class OperationRetryScheduled extends QueueEvent {
  /// Creates the event.
  const OperationRetryScheduled(super.operation);
}

/// An operation was rejected permanently or ran out of attempts.
final class OperationDeadLettered extends QueueEvent {
  /// Creates the event.
  const OperationDeadLettered(super.operation);
}

/// An operation was removed before delivery.
final class OperationCancelled extends QueueEvent {
  /// Creates the event.
  const OperationCancelled(super.operation);
}
