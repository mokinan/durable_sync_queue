/// What happened when the handler tried to deliver an operation.
sealed class Outcome {
  const Outcome();

  /// The server accepted the operation. It is removed from the queue.
  const factory Outcome.delivered() = Delivered;

  /// The server refused it for good (e.g. HTTP 4xx). The operation is
  /// dead-lettered and the rest of its group continues.
  const factory Outcome.rejected(String reason) = Rejected;

  /// A transient failure (timeout, 5xx, offline). The group pauses and the
  /// operation is retried after a backoff delay.
  const factory Outcome.retryLater(String reason, {Duration? after}) =
      RetryLater;
}

/// See [Outcome.delivered].
final class Delivered extends Outcome {
  /// Creates a delivered outcome.
  const Delivered();
}

/// See [Outcome.rejected].
final class Rejected extends Outcome {
  /// Creates a rejected outcome.
  const Rejected(this.reason);

  /// Why the server refused the operation.
  final String reason;
}

/// See [Outcome.retryLater].
final class RetryLater extends Outcome {
  /// Creates a retry outcome.
  const RetryLater(this.reason, {this.after});

  /// Why delivery failed.
  final String reason;

  /// Overrides the backoff, e.g. from a `Retry-After` header.
  final Duration? after;
}
