import 'package:test/test.dart';
import 'package:wren/wren.dart';

void main() {
  group('RetryInfrastructure.forQueue', () {
    test('derives topology names from the main queue', () {
      final infra =
          RetryInfrastructure.forQueue('orders', RetryPolicy.defaults());
      expect(infra.mainQueue, 'orders');
      expect(infra.retryExchange, 'orders.retry');
      expect(infra.dlxExchange, 'orders.dlx');
      expect(infra.dlq, 'orders.dlq');
    });
  });

  group('routingKeyForAttempt', () {
    test('exponential backoff keys by capped attempt number', () {
      final infra = RetryInfrastructure.forQueue(
        'orders',
        const RetryPolicy(
          strategy: ExponentialBackoff(
            initialMs: 100,
            maxMs: 1000,
            multiplier: 2,
          ),
          maxAttempts: 3,
        ),
      );
      expect(infra.routingKeyForAttempt(1), 'attempt.1');
      expect(infra.routingKeyForAttempt(2), 'attempt.2');
      // Capped at maxAttempts.
      expect(infra.routingKeyForAttempt(9), 'attempt.3');
    });

    test('fixed interval always uses the single retry key', () {
      final infra = RetryInfrastructure.forQueue(
        'orders',
        const RetryPolicy(strategy: FixedInterval(500), maxAttempts: 3),
      );
      expect(infra.routingKeyForAttempt(1), 'retry');
      expect(infra.routingKeyForAttempt(2), 'retry');
    });
  });
}
