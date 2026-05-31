import 'dart:math' as math;

import 'connection.dart';
import 'retry.dart';
import 'topology.dart';

/// The routing key that lands a message in the dead-letter queue.
const String _dlqRoutingKey = 'dlq';

/// The broker topology that powers retries and dead-lettering for one main
/// queue. Build with [RetryInfrastructure.forQueue], declare it with [setup],
/// and hand it to `channel.startConsumerWithRetry` / `startRouterWithRetry`.
///
/// Mirrors bunnyhop's `RetryInfrastructure`: when a handler asks to retry, the
/// message is republished into a delay queue (a queue with a TTL and no
/// consumer) that dead-letters back to the main queue when the TTL expires.
/// Exhausted and dead-lettered messages go to the DLQ.
final class RetryInfrastructure {
  const RetryInfrastructure({
    required this.mainQueue,
    required this.retryExchange,
    required this.dlxExchange,
    required this.dlq,
    required this.policy,
  });

  /// Derive the retry topology for [mainQueue] from a [RetryPolicy]. Names are
  /// derived from the main queue: `<q>.retry`, `<q>.dlx`, `<q>.dlq`.
  factory RetryInfrastructure.forQueue(String mainQueue, RetryPolicy policy) {
    return RetryInfrastructure(
      mainQueue: mainQueue,
      retryExchange: '$mainQueue.retry',
      dlxExchange: '$mainQueue.dlx',
      dlq: '$mainQueue.dlq',
      policy: policy,
    );
  }

  final String mainQueue;
  final String retryExchange;
  final String dlxExchange;
  final String dlq;
  final RetryPolicy policy;

  /// Declare the whole retry topology, idempotently: the retry exchange, the
  /// DLX, the main queue, the DLQ (bound to the DLX), and one delay queue per
  /// retry slot (each with its TTL and a dead-letter route back to the main
  /// queue).
  Future<void> setup(WrenChannel channel) async {
    await channel.declareExchange(
      retryExchange,
      ExchangeKind.direct,
      const ExchangeOptions(),
    );
    await channel.declareExchange(
      dlxExchange,
      ExchangeKind.direct,
      const ExchangeOptions(),
    );
    await channel.declareQueue(mainQueue);
    await channel.declareQueue(dlq);
    await channel.bindQueue(dlq, dlxExchange, _dlqRoutingKey);

    for (final slot in _retrySlots()) {
      final options = QueueOptions(arguments: {
        'x-message-ttl': Arg.int(slot.ttlMs),
        'x-dead-letter-exchange': const Arg.string(''),
        'x-dead-letter-routing-key': Arg.string(mainQueue),
      });
      await channel.declareQueueWith(slot.queue, options);
      await channel.bindQueue(slot.queue, retryExchange, slot.routingKey);
    }
  }

  /// The delay queues to declare: one per attempt for exponential backoff (each
  /// with its own TTL), or a single queue for a fixed interval.
  List<_RetrySlot> _retrySlots() {
    final strategy = policy.strategy;
    switch (strategy) {
      case FixedInterval(:final intervalMs):
        return [
          _RetrySlot(
            queue: '$mainQueue.retry',
            routingKey: 'retry',
            ttlMs: math.max(intervalMs, 0),
          ),
        ];
      case ExponentialBackoff():
        return [
          for (var attempt = 1; attempt <= policy.maxAttempts; attempt++)
            _RetrySlot(
              queue: '$mainQueue.retry.$attempt',
              routingKey: 'attempt.$attempt',
              ttlMs: policy.calculateDelay(attempt),
            ),
        ];
    }
  }

  /// The routing key that lands a message in the delay queue for [attempt].
  String routingKeyForAttempt(int attempt) {
    final strategy = policy.strategy;
    switch (strategy) {
      case FixedInterval():
        return 'retry';
      case ExponentialBackoff():
        final capped = math.min(math.max(attempt, 1), policy.maxAttempts);
        return 'attempt.$capped';
    }
  }

  /// The routing key for the dead-letter queue.
  String get dlqRoutingKey => _dlqRoutingKey;
}

final class _RetrySlot {
  const _RetrySlot({
    required this.queue,
    required this.routingKey,
    required this.ttlMs,
  });

  final String queue;
  final String routingKey;
  final int ttlMs;
}
