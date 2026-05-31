import 'dart:math' as math;

/// How the delay before each retry is computed.
sealed class RetryStrategy {
  const RetryStrategy();
}

/// `initialMs * multiplier^(attempt - 1)`, capped at `maxMs`.
final class ExponentialBackoff extends RetryStrategy {
  const ExponentialBackoff({
    required this.initialMs,
    required this.maxMs,
    required this.multiplier,
  });

  final int initialMs;
  final int maxMs;
  final double multiplier;
}

/// The same `intervalMs` before every attempt.
final class FixedInterval extends RetryStrategy {
  const FixedInterval(this.intervalMs);
  final int intervalMs;
}

/// A strategy plus a ceiling on how many attempts to make.
final class RetryPolicy {
  const RetryPolicy({required this.strategy, required this.maxAttempts});

  /// A common starting point: exponential backoff from 1s, doubling, capped at
  /// 1 minute, over 5 attempts.
  factory RetryPolicy.defaults() => const RetryPolicy(
        strategy: ExponentialBackoff(
          initialMs: 1000,
          maxMs: 60000,
          multiplier: 2.0,
        ),
        maxAttempts: 5,
      );

  final RetryStrategy strategy;
  final int maxAttempts;

  /// The delay in milliseconds before [attempt] (1-based: attempt 1 is the
  /// first retry). Never negative; exponential delays are capped at `maxMs`.
  int calculateDelay(int attempt) {
    final n = math.max(attempt, 1);
    final strategy = this.strategy;
    switch (strategy) {
      case FixedInterval(:final intervalMs):
        return math.max(intervalMs, 0);
      case ExponentialBackoff(:final initialMs, :final maxMs, :final multiplier):
        final factor = math.pow(multiplier, n - 1).toDouble();
        final delay = (initialMs * factor).round();
        return math.max(math.min(delay, maxMs), 0);
    }
  }

  /// The full schedule of delays, one per allowed attempt.
  List<int> retryIntervals() => [
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
          calculateDelay(attempt),
      ];
}

/// Header carrying the current retry count.
const String retryCountHeader = 'x-retry-count';

/// Header carrying the maximum number of attempts.
const String maxRetriesHeader = 'x-max-retries';

/// Header carrying the timestamp of the first failure.
const String firstDeathHeader = 'x-first-death';

/// Header carrying the timestamp of the most recent retry.
const String lastRetryHeader = 'x-last-retry';

/// Header carrying the original error message.
const String originalErrorHeader = 'x-original-error';

/// Header carrying a human-readable reason for the retry.
const String retryReasonHeader = 'x-retry-reason';

/// Header preserving the message's original routing key.
const String originalRoutingKeyHeader = 'x-original-routing-key';

/// The retry state carried on a message's headers — wren's idiomatic take on
/// bunnyhop's `retry.rs`.
final class RetryMetadata {
  const RetryMetadata({
    required this.attempt,
    required this.maxAttempts,
    this.firstDeath,
    this.lastRetry,
    this.originalError,
    this.reason,
    this.originalRoutingKey,
  });

  /// Fresh metadata for a message that has not yet failed.
  factory RetryMetadata.fresh(int maxAttempts) =>
      RetryMetadata(attempt: 0, maxAttempts: maxAttempts);

  /// Read retry metadata from message headers. When `x-max-retries` is absent,
  /// [defaultMax] is used (the consumer's configured policy maximum).
  factory RetryMetadata.fromHeaders(
    Map<String, String> headers,
    int defaultMax,
  ) {
    int headerInt(String key, int fallback) {
      final raw = headers[key];
      if (raw == null) return fallback;
      return int.tryParse(raw) ?? fallback;
    }

    return RetryMetadata(
      attempt: headerInt(retryCountHeader, 0),
      maxAttempts: headerInt(maxRetriesHeader, defaultMax),
      firstDeath: headers[firstDeathHeader],
      lastRetry: headers[lastRetryHeader],
      originalError: headers[originalErrorHeader],
      reason: headers[retryReasonHeader],
      originalRoutingKey: headers[originalRoutingKeyHeader],
    );
  }

  final int attempt;
  final int maxAttempts;
  final String? firstDeath;
  final String? lastRetry;
  final String? originalError;
  final String? reason;
  final String? originalRoutingKey;

  /// Serialise metadata back into headers. Always emits the count and maximum;
  /// optional fields are emitted only when present.
  Map<String, String> toHeaders() {
    return {
      retryCountHeader: attempt.toString(),
      maxRetriesHeader: maxAttempts.toString(),
      if (firstDeath != null) firstDeathHeader: firstDeath!,
      if (lastRetry != null) lastRetryHeader: lastRetry!,
      if (originalError != null) originalErrorHeader: originalError!,
      if (reason != null) retryReasonHeader: reason!,
      if (originalRoutingKey != null)
        originalRoutingKeyHeader: originalRoutingKey!,
    };
  }

  /// Record another failure: bump the attempt count and note the reason.
  RetryMetadata recordFailure(String reason) => copyWith(
        attempt: attempt + 1,
        reason: reason,
      );

  /// Has this message used up its allowed attempts?
  bool get isExhausted => attempt >= maxAttempts;

  /// Return a copy with the given fields overridden.
  RetryMetadata copyWith({
    int? attempt,
    int? maxAttempts,
    String? firstDeath,
    String? lastRetry,
    String? originalError,
    String? reason,
    String? originalRoutingKey,
  }) {
    return RetryMetadata(
      attempt: attempt ?? this.attempt,
      maxAttempts: maxAttempts ?? this.maxAttempts,
      firstDeath: firstDeath ?? this.firstDeath,
      lastRetry: lastRetry ?? this.lastRetry,
      originalError: originalError ?? this.originalError,
      reason: reason ?? this.reason,
      originalRoutingKey: originalRoutingKey ?? this.originalRoutingKey,
    );
  }
}
