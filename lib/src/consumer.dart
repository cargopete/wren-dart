import 'dart:typed_data';

import 'package:dart_amqp/dart_amqp.dart' as amqp;

import 'connection.dart';
import 'errors.dart';
import 'log.dart';
import 'message.dart';
import 'publish_options.dart';
import 'retry.dart';
import 'retry_infrastructure.dart';
import 'router.dart';
import 'topology.dart';

/// A running subscription. Created via the `startConsumer` / `startRouter`
/// extension methods on [WrenChannel]. Stop it with [stop].
final class WrenConsumer {
  WrenConsumer._(this._consumer);

  final amqp.Consumer _consumer;

  /// Cancel the subscription. Safe to call more than once.
  Future<void> stop() async {
    try {
      await _consumer.cancel();
    } on Object {
      // Best-effort.
    }
  }
}

/// Build a wren [Message] from a raw dart_amqp delivery.
///
/// Note: `redelivered` is always `false` — dart_amqp doesn't surface the flag.
Message messageFromDelivery(amqp.AmqpMessage delivery) {
  final properties = delivery.properties;
  final headers = <String, String>{};
  final rawHeaders = properties?.headers;
  if (rawHeaders != null) {
    rawHeaders.forEach((key, value) {
      if (value != null) headers[key] = value.toString();
    });
  }
  return Message(
    payload: delivery.payload ?? Uint8List(0),
    routingKey: delivery.routingKey ?? '',
    headers: headers,
    correlationId: properties?.corellationId,
    replyTo: properties?.replyTo,
    redelivered: false,
  );
}

/// Consumer-starting operations on a channel.
extension ConsumerOps on WrenChannel {
  /// Start a consumer on [queue]. Each delivery is passed to [handler], then
  /// settled with the broker per the returned [Confirmation].
  ///
  /// [maxConcurrent] sets the channel prefetch, so that many deliveries are in
  /// flight (and settled independently) at a time; the default of `1` processes
  /// serially. With [retry] infrastructure, handlers returning [Confirmation.retry]
  /// are redelivered through the delay queues, and [Confirmation.deadLetter]
  /// (and exhausted retries) go to the DLQ.
  Future<WrenConsumer> startConsumer(
    String queue,
    MessageHandler handler, {
    RetryInfrastructure? retry,
    int maxConcurrent = 1,
    ConsumeOptions options = const ConsumeOptions(),
  }) async {
    await qos(maxConcurrent < 1 ? 1 : maxConcurrent);

    final amqp.Queue amqpQueue;
    final amqp.Consumer consumer;
    try {
      amqpQueue = await rawChannel.queue(queue, declare: false);
      consumer = await amqpQueue.consume(
        consumerTag: options.consumerTag,
        noLocal: options.noLocal,
        noAck: options.autoAck,
        exclusive: options.exclusive,
        arguments: Arg.toAmqp(options.arguments),
      );
    } on Object catch (e) {
      throw ChannelFailed(e.toString());
    }

    consumer.listen(
      (delivery) async {
        final message = messageFromDelivery(delivery);
        try {
          final confirmation = await handler(message);
          if (!options.autoAck) {
            await _settle(this, retry, message, delivery, confirmation);
          }
        } on Object catch (e) {
          // A crashing handler shouldn't lose the message: leave it unacked and
          // let the broker redeliver, mirroring the Gleam original's restart.
          logWarning('wren: handler threw, requeueing delivery: $e');
          if (!options.autoAck) delivery.reject(true);
        }
      },
      onError: (Object e) => logWarning('wren: consumer stream error: $e'),
    );

    return WrenConsumer._(consumer);
  }

  /// Start a consumer backed by retry infrastructure. The topology is declared
  /// first (via [RetryInfrastructure.setup]), then the consumer subscribes to
  /// the infrastructure's main queue.
  Future<WrenConsumer> startConsumerWithRetry(
    MessageHandler handler,
    RetryInfrastructure infra, {
    int maxConcurrent = 1,
    ConsumeOptions options = const ConsumeOptions(),
  }) async {
    await infra.setup(this);
    return startConsumer(
      infra.mainQueue,
      handler,
      retry: infra,
      maxConcurrent: maxConcurrent,
      options: options,
    );
  }

  /// Start a consumer on [queue] that dispatches each delivery through [router].
  Future<WrenConsumer> startRouter(
    String queue,
    Router router, {
    int maxConcurrent = 1,
    ConsumeOptions options = const ConsumeOptions(),
  }) =>
      startConsumer(
        queue,
        router.dispatch,
        maxConcurrent: maxConcurrent,
        options: options,
      );

  /// Start a router-backed consumer with retry infrastructure.
  Future<WrenConsumer> startRouterWithRetry(
    Router router,
    RetryInfrastructure infra, {
    int maxConcurrent = 1,
    ConsumeOptions options = const ConsumeOptions(),
  }) =>
      startConsumerWithRetry(
        router.dispatch,
        infra,
        maxConcurrent: maxConcurrent,
        options: options,
      );
}

/// Settle a delivery according to the handler's [Confirmation]. `ack`/`reject`
/// settle directly; `retry`/`deadLetter` route through the retry infrastructure
/// when one is configured.
Future<void> _settle(
  WrenChannel channel,
  RetryInfrastructure? infra,
  Message message,
  amqp.AmqpMessage delivery,
  Confirmation confirmation,
) async {
  switch (confirmation) {
    case Confirmation.ack:
      delivery.ack();
    case Confirmation.reject:
      delivery.reject(false);
    case Confirmation.retry:
      await _settleRetry(channel, infra, message, delivery);
    case Confirmation.deadLetter:
      await _settleDeadLetter(channel, infra, message, delivery);
  }
}

Future<void> _settleRetry(
  WrenChannel channel,
  RetryInfrastructure? infra,
  Message message,
  amqp.AmqpMessage delivery,
) async {
  if (infra == null) {
    logWarning(
      'wren: Retry requested but no retry infrastructure configured; rejecting',
    );
    delivery.reject(false);
    return;
  }
  final metadata = _stamp(
    RetryMetadata.fromHeaders(message.headers, infra.policy.maxAttempts)
        .recordFailure('handler returned Retry'),
  );
  final (exchange, routingKey) = metadata.isExhausted
      ? (infra.dlxExchange, infra.dlqRoutingKey)
      : (infra.retryExchange, infra.routingKeyForAttempt(metadata.attempt));
  await _reroute(channel, message, delivery, exchange, routingKey, metadata);
}

Future<void> _settleDeadLetter(
  WrenChannel channel,
  RetryInfrastructure? infra,
  Message message,
  amqp.AmqpMessage delivery,
) async {
  if (infra == null) {
    // No DLQ to route to — reject without requeue (the broker's own DLX, if any).
    delivery.reject(false);
    return;
  }
  final metadata = _stamp(
    RetryMetadata.fromHeaders(message.headers, infra.policy.maxAttempts)
        .recordFailure('handler returned DeadLetter'),
  );
  await _reroute(
    channel,
    message,
    delivery,
    infra.dlxExchange,
    infra.dlqRoutingKey,
    metadata,
  );
}

/// Republish [message] (with refreshed retry headers) to [exchange]/[routingKey],
/// then ack the original. If the republish fails, reject so we don't lose track.
Future<void> _reroute(
  WrenChannel channel,
  Message message,
  amqp.AmqpMessage delivery,
  String exchange,
  String routingKey,
  RetryMetadata metadata,
) async {
  final options = PublishOptions()
      .toExchange(exchange)
      .route(routingKey)
      .withHeaders(_mergeRetryHeaders(message.headers, metadata));
  try {
    await channel.publishWithOptions(message.payload, options);
    delivery.ack();
  } on Object {
    logWarning("wren: failed to route message to '$exchange'");
    delivery.reject(false);
  }
}

/// Overlay refreshed retry headers onto the message's existing headers,
/// replacing any stale retry headers of the same name.
Map<String, String> _mergeRetryHeaders(
  Map<String, String> original,
  RetryMetadata metadata,
) {
  final refreshed = metadata.toHeaders();
  return {
    for (final entry in original.entries)
      if (!refreshed.containsKey(entry.key)) entry.key: entry.value,
    ...refreshed,
  };
}

/// Timestamp a failure: always set `lastRetry`; set `firstDeath` only once.
RetryMetadata _stamp(RetryMetadata metadata) {
  final now = DateTime.now().toUtc().toIso8601String();
  return metadata.copyWith(
    lastRetry: now,
    firstDeath: metadata.firstDeath ?? now,
  );
}
