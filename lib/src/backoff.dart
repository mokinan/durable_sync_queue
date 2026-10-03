import 'dart:math';

/// Computes the delay before retry number `attempt` (1-based).
abstract interface class BackoffPolicy {
  /// The delay before the next attempt, after [attempt] failed attempts.
  Duration delay(int attempt);
}

/// Exponential backoff with full jitter, capped at [max].
///
/// `delay = random(0.5 … 1.0) × min(max, base × 2^(attempt-1))`. The jitter
/// keeps many clients that went offline together from retrying in lockstep.
class ExponentialBackoff implements BackoffPolicy {
  /// Creates a policy. Pass a seeded [random] in tests.
  ExponentialBackoff({
    this.base = const Duration(seconds: 2),
    this.max = const Duration(minutes: 5),
    Random? random,
  }) : _random = random ?? Random();

  /// Delay after the first failure (before jitter).
  final Duration base;

  /// Upper bound for any delay.
  final Duration max;

  final Random _random;

  @override
  Duration delay(int attempt) {
    final exponent = min(attempt - 1, 30);
    final raw = base.inMilliseconds * pow(2, exponent);
    final capped = min(raw, max.inMilliseconds.toDouble());
    final jittered = capped * (0.5 + _random.nextDouble() / 2);
    return Duration(milliseconds: jittered.round());
  }
}

/// A fixed delay; handy in tests.
class ConstantBackoff implements BackoffPolicy {
  /// Creates a policy that always waits [value].
  const ConstantBackoff(this.value);

  /// The delay used for every retry.
  final Duration value;

  @override
  Duration delay(int attempt) => value;
}
