import 'package:test/test.dart';
import 'package:wren/wren.dart';

void main() {
  group('RetryPolicy.calculateDelay', () {
    test('exponential backoff doubles and caps', () {
      const policy = RetryPolicy(
        strategy: ExponentialBackoff(
          initialMs: 1000,
          maxMs: 5000,
          multiplier: 2.0,
        ),
        maxAttempts: 5,
      );
      expect(policy.calculateDelay(1), 1000);
      expect(policy.calculateDelay(2), 2000);
      expect(policy.calculateDelay(3), 4000);
      expect(policy.calculateDelay(4), 5000); // capped
      expect(policy.calculateDelay(5), 5000); // capped
    });

    test('attempt below 1 is treated as 1', () {
      const policy = RetryPolicy(
        strategy: ExponentialBackoff(initialMs: 100, maxMs: 9999, multiplier: 2),
        maxAttempts: 3,
      );
      expect(policy.calculateDelay(0), 100);
    });

    test('fixed interval is constant and never negative', () {
      const policy = RetryPolicy(strategy: FixedInterval(250), maxAttempts: 3);
      expect(policy.calculateDelay(1), 250);
      expect(policy.calculateDelay(9), 250);

      const negative = RetryPolicy(strategy: FixedInterval(-5), maxAttempts: 1);
      expect(negative.calculateDelay(1), 0);
    });

    test('retryIntervals yields one delay per attempt', () {
      final policy = RetryPolicy.defaults();
      expect(policy.retryIntervals().length, 5);
    });
  });

  group('RetryMetadata', () {
    test('round-trips through headers', () {
      const metadata = RetryMetadata(
        attempt: 2,
        maxAttempts: 5,
        firstDeath: '2026-01-01T00:00:00Z',
        reason: 'boom',
      );
      final headers = metadata.toHeaders();
      expect(headers[retryCountHeader], '2');
      expect(headers[maxRetriesHeader], '5');
      expect(headers[firstDeathHeader], '2026-01-01T00:00:00Z');
      expect(headers[retryReasonHeader], 'boom');

      final parsed = RetryMetadata.fromHeaders(headers, 99);
      expect(parsed.attempt, 2);
      expect(parsed.maxAttempts, 5);
      expect(parsed.reason, 'boom');
    });

    test('fromHeaders falls back to defaultMax when absent', () {
      final parsed = RetryMetadata.fromHeaders({}, 7);
      expect(parsed.attempt, 0);
      expect(parsed.maxAttempts, 7);
    });

    test('recordFailure bumps the attempt and sets the reason', () {
      final metadata = RetryMetadata.fresh(3).recordFailure('nope');
      expect(metadata.attempt, 1);
      expect(metadata.reason, 'nope');
    });

    test('isExhausted once attempts reach the maximum', () {
      expect(const RetryMetadata(attempt: 3, maxAttempts: 3).isExhausted, isTrue);
      expect(const RetryMetadata(attempt: 2, maxAttempts: 3).isExhausted, isFalse);
    });
  });
}
