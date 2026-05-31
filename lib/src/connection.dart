import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:dart_amqp/dart_amqp.dart' as amqp;

import 'codec.dart';
import 'config.dart';
import 'errors.dart';
import 'kind_routing.dart';
import 'publish_options.dart';
import 'topology.dart';

/// An open connection to a RabbitMQ broker. Created via [WrenConnection.connect].
final class WrenConnection {
  WrenConnection._(this.rawClient);

  /// The underlying dart_amqp client. Exposed for advanced use; prefer the
  /// wrapper methods.
  final amqp.Client rawClient;

  bool _open = true;

  /// Open a connection to the broker.
  static Future<WrenConnection> connect(Config config) async {
    final settings = amqp.ConnectionSettings(
      host: config.host,
      port: config.port,
      virtualHost: config.virtualHost,
      authProvider: amqp.PlainAuthenticator(config.username, config.password),
      // wren manages its own recovery, so the underlying client fails fast.
      maxConnectionAttempts: 1,
      tuningSettings: amqp.TuningSettings(
        heartbeatPeriod: Duration(seconds: config.heartbeatSeconds),
      ),
      tlsContext: config.tls.toSecurityContext(),
      onBadCertificate: config.tls.enabled && !config.tls.verify
          ? (X509Certificate cert) => true
          : null,
    );

    final client = amqp.Client(settings: settings);
    final connection = WrenConnection._(client);
    try {
      await client
          .connect()
          .timeout(Duration(milliseconds: config.connectionTimeoutMs));
    } on TimeoutException {
      await _safeClose(client);
      throw ConnectionFailed(
        'connection timed out after ${config.connectionTimeoutMs}ms',
      );
    } on Object catch (e) {
      await _safeClose(client);
      throw ConnectionFailed(e.toString());
    }
    // A socket/protocol error marks the connection as no longer open.
    client.errorListener((_) => connection._open = false);
    return connection;
  }

  /// Is the connection still believed to be alive? Flips to `false` once the
  /// connection is closed or the client reports a fatal error.
  bool get isOpen => _open;

  /// Open a channel over this connection.
  Future<WrenChannel> openChannel() async {
    try {
      final channel = await rawClient.channel();
      return WrenChannel._(channel);
    } on Object catch (e) {
      throw ChannelFailed(e.toString());
    }
  }

  /// Close the connection (and implicitly its channels).
  Future<void> close() async {
    _open = false;
    await _safeClose(rawClient);
  }

  static Future<void> _safeClose(amqp.Client client) async {
    try {
      await client.close();
    } on Object {
      // Best-effort: a teardown failure is nothing we can act on.
    }
  }
}

/// A channel multiplexed over a [WrenConnection]. Created via
/// [WrenConnection.openChannel].
final class WrenChannel {
  WrenChannel._(this.rawChannel);

  /// The underlying dart_amqp channel. Exposed for advanced use and for wren's
  /// own consumer machinery; prefer the wrapper methods.
  final amqp.Channel rawChannel;

  final Map<String, amqp.Queue> _queues = {};
  final Map<String, amqp.Exchange> _exchanges = {};
  bool _confirmsEnabled = false;

  // ===========================================================================
  // Channel configuration
  // ===========================================================================

  /// Actively probe the channel by round-tripping a throwaway private queue —
  /// confirms the channel is responsive, not merely that it exists.
  Future<void> healthCheck() async {
    try {
      final queue = await rawChannel.privateQueue();
      await queue.delete();
    } on Object catch (e) {
      throw ChannelFailed(e.toString());
    }
  }

  /// Set channel prefetch: the number of unacknowledged messages the broker will
  /// deliver before waiting for acks.
  Future<void> qos(int prefetchCount) => qosWith(prefetchCount, 0, false);

  /// Set channel prefetch with full control over `prefetchSize` (octets, `0` for
  /// no limit) and whether the setting is `global` (channel-wide vs per-consumer).
  Future<void> qosWith(int prefetchCount, int prefetchSize, bool global) async {
    try {
      await rawChannel.qos(prefetchSize, prefetchCount, global: global);
    } on Object catch (e) {
      throw ChannelFailed(e.toString());
    }
  }

  // ===========================================================================
  // Topology
  // ===========================================================================

  /// Declare a durable queue with default options (idempotent).
  Future<void> declareQueue(String name) =>
      declareQueueWith(name, const QueueOptions());

  /// Check that a queue exists without creating it (a passive declare). Throws
  /// if it doesn't exist. Note: a failed passive declare closes the channel.
  Future<void> declareQueuePassive(String name) async {
    try {
      await rawChannel.queue(name, passive: true);
    } on Object catch (e) {
      throw ChannelFailed(e.toString());
    }
  }

  /// Declare a queue with explicit options, including AMQP `x-*` arguments.
  Future<void> declareQueueWith(String name, QueueOptions options) async {
    try {
      final queue = await rawChannel.queue(
        name,
        durable: options.durable,
        exclusive: options.exclusive,
        autoDelete: options.autoDelete,
        arguments: Arg.toAmqp(options.arguments),
      );
      _queues[name] = queue;
    } on Object catch (e) {
      throw ChannelFailed(e.toString());
    }
  }

  /// Declare an exchange of the given kind (idempotent).
  ///
  /// Note: the underlying dart_amqp does not expose the `autoDelete` or
  /// `internal` declare flags, so those [ExchangeOptions] fields are ignored.
  Future<void> declareExchange(
    String name,
    ExchangeKind kind,
    ExchangeOptions options,
  ) async {
    try {
      final exchange = await rawChannel.exchange(
        name,
        _exchangeType(kind),
        durable: options.durable,
        arguments: Arg.toAmqp(options.arguments),
      );
      _exchanges[name] = exchange;
    } on Object catch (e) {
      throw ChannelFailed(e.toString());
    }
  }

  /// Bind a queue to an exchange with a routing key.
  Future<void> bindQueue(
    String queue,
    String exchange,
    String routingKey,
  ) =>
      bindQueueWith(queue, exchange, routingKey, const {});

  /// Bind a queue with binding arguments — needed for `headers` exchanges.
  Future<void> bindQueueWith(
    String queue,
    String exchange,
    String routingKey,
    Map<String, Arg> arguments,
  ) async {
    try {
      final q = await _queueHandle(queue);
      final ex = await _exchangeHandle(exchange);
      await q.bind(ex, routingKey, arguments: Arg.toAmqp(arguments));
    } on Object catch (e) {
      throw ChannelFailed(e.toString());
    }
  }

  /// Remove a binding between a queue and an exchange.
  Future<void> unbindQueue(
    String queue,
    String exchange,
    String routingKey,
  ) async {
    try {
      final q = await _queueHandle(queue);
      final ex = await _exchangeHandle(exchange);
      await q.unbind(ex, routingKey);
    } on Object catch (e) {
      throw ChannelFailed(e.toString());
    }
  }

  /// Delete a queue, optionally only `ifUnused` (no consumers) and/or `ifEmpty`
  /// (no messages). The delete fails if a guard isn't met.
  Future<void> deleteQueue(
    String name, {
    bool ifUnused = false,
    bool ifEmpty = false,
  }) async {
    try {
      final q = await _queueHandle(name);
      await q.delete(ifUnused: ifUnused, ifEmpty: ifEmpty);
      _queues.remove(name);
    } on Object catch (e) {
      throw ChannelFailed(e.toString());
    }
  }

  /// Delete an exchange, optionally only `ifUnused` (no bindings).
  Future<void> deleteExchange(String name, {bool ifUnused = false}) async {
    try {
      final ex = await _exchangeHandle(name);
      await ex.delete(ifUnused: ifUnused);
      _exchanges.remove(name);
    } on Object catch (e) {
      throw ChannelFailed(e.toString());
    }
  }

  /// Remove all ready messages from a queue.
  Future<void> purgeQueue(String name) async {
    try {
      final q = await _queueHandle(name);
      await q.purge();
    } on Object catch (e) {
      throw ChannelFailed(e.toString());
    }
  }

  // ===========================================================================
  // Publishing
  // ===========================================================================

  /// Publish raw bytes to an exchange with a routing key. Use `''` as the
  /// exchange to publish straight to a queue by name (the default exchange).
  Future<void> publish(
    String exchange,
    String routingKey,
    Uint8List payload,
  ) =>
      publishWithOptions(
        payload,
        PublishOptions().toExchange(exchange).route(routingKey),
      );

  /// Publish a UTF-8 `text` payload — the common case.
  Future<void> publishText(
    String exchange,
    String routingKey,
    String text,
  ) =>
      publish(exchange, routingKey, Uint8List.fromList(utf8.encode(text)));

  /// Publish a message with the full set of [PublishOptions].
  Future<void> publishWithOptions(
    Uint8List payload,
    PublishOptions options,
  ) async {
    final properties = _propertiesOf(options);
    try {
      if (options.exchange.isEmpty) {
        final queue = await _queueHandle(options.routingKey);
        queue.publish(
          payload,
          properties: properties,
          mandatory: options.mandatory,
        );
      } else {
        final exchange = await _exchangeHandle(options.exchange);
        exchange.publish(
          payload,
          options.routingKey,
          properties: properties,
          mandatory: options.mandatory,
        );
      }
    } on Object catch (e) {
      throw ChannelFailed(e.toString());
    }
  }

  /// Put the channel into publisher-confirm mode. Idempotent; called for you by
  /// [publishConfirmed].
  Future<void> enableConfirms() async {
    if (_confirmsEnabled) return;
    try {
      await rawChannel.confirmPublishedMessages();
      _confirmsEnabled = true;
    } on Object catch (e) {
      throw ChannelFailed(e.toString());
    }
  }

  /// Publish and wait (up to [timeoutMs]) for the broker to confirm the message.
  ///
  /// Note: the underlying confirm notifications don't carry delivery tags, so
  /// this matches the published message to the next confirmation. Use it
  /// serially (publish, await) rather than interleaving concurrent confirmed
  /// publishes on one channel.
  Future<void> publishConfirmed(
    Uint8List payload,
    PublishOptions options,
    int timeoutMs,
  ) async {
    await enableConfirms();
    final verdict = Completer<bool>();
    final subscription = rawChannel.publishNotifier((notification) {
      if (!verdict.isCompleted) verdict.complete(notification.published);
    });
    try {
      await publishWithOptions(payload, options);
      final published = await verdict.future
          .timeout(Duration(milliseconds: timeoutMs));
      if (!published) {
        throw const ChannelFailed('publish was nacked by the broker');
      }
    } on TimeoutException {
      throw const ChannelFailed('publish confirmation timed out');
    } finally {
      await subscription.cancel();
    }
  }

  /// Encode a typed [value] with [codec] and publish it with the given options.
  Future<void> publishEncoded<T>(
    T value,
    Codec<T> codec,
    PublishOptions options,
  ) async {
    final Uint8List payload;
    try {
      payload = codec.encode(value);
    } on CodecError catch (e) {
      throw EncodingFailed(e.reason);
    }
    await publishWithOptions(payload, options);
  }

  // ===========================================================================
  // Batch / multi-target publishing
  // ===========================================================================

  /// Publish each `(Target, payload)` in turn (order preserved), collecting
  /// per-message failures rather than stopping at the first.
  Future<BatchResult> publishBatch(
    List<(Target, Uint8List)> messages,
    PublishOptions options,
  ) async {
    final failures = <BatchFailure>[];
    for (final (target, payload) in messages) {
      try {
        await _publishToTarget(target, payload, options);
      } on WrenError catch (e) {
        failures.add(BatchFailure(target: target, payload: payload, error: e));
      }
    }
    return BatchResult(
      published: messages.length - failures.length,
      failures: failures,
    );
  }

  /// Publish one [payload] to several targets.
  Future<BatchResult> publishToTargets(
    Uint8List payload,
    List<Target> targets,
    PublishOptions options,
  ) =>
      publishBatch([for (final t in targets) (t, payload)], options);

  /// Like [publishBatch], but re-publishes failures up to [maxAttempts] times.
  Future<BatchResult> publishBatchWithRetry(
    List<(Target, Uint8List)> messages,
    PublishOptions options,
    int maxAttempts,
  ) async {
    var remaining = messages;
    var publishedSoFar = 0;
    var attemptsLeft = maxAttempts < 1 ? 1 : maxAttempts;
    while (true) {
      final result = await publishBatch(remaining, options);
      publishedSoFar += result.published;
      if (result.failures.isEmpty || attemptsLeft <= 1) {
        return BatchResult(published: publishedSoFar, failures: result.failures);
      }
      remaining = [for (final f in result.failures) (f.target, f.payload)];
      attemptsLeft -= 1;
    }
  }

  Future<void> _publishToTarget(
    Target target,
    Uint8List payload,
    PublishOptions options,
  ) =>
      publishWithOptions(
        payload,
        options.toExchange(target.exchange).route(target.routingKey),
      );

  // ===========================================================================
  // Kind-based producer
  // ===========================================================================

  /// Publish [payload] for [kind], applying the [routing] table and stamping the
  /// `kind` header.
  Future<void> publishForKind(
    KindRouting routing,
    String kind,
    Uint8List payload,
    PublishOptions options,
  ) =>
      publishWithOptions(payload, routing.apply(kind, options).withKind(kind));

  /// Like [publishForKind], but encodes a typed value with [codec] first.
  Future<void> publishEncodedForKind<T>(
    KindRouting routing,
    String kind,
    T value,
    Codec<T> codec,
    PublishOptions options,
  ) async {
    final Uint8List payload;
    try {
      payload = codec.encode(value);
    } on CodecError catch (e) {
      throw EncodingFailed(e.reason);
    }
    await publishForKind(routing, kind, payload, options);
  }

  // ===========================================================================
  // One-off fetch
  // ===========================================================================

  /// Fetch a single message's raw bytes from a queue, waiting up to
  /// [timeoutMs]. A primitive for one-off fetches; prefer a consumer for
  /// ongoing work.
  ///
  /// Emulated with a short-lived, single-prefetch consumer, since the
  /// underlying dart_amqp has no `basic.get`.
  Future<Uint8List> get(String queue, {int timeoutMs = 1000}) async {
    final result = Completer<Uint8List>();
    amqp.Consumer? consumer;
    try {
      await qos(1);
      final q = await _queueHandle(queue);
      consumer = await q.consume(noAck: false);
      consumer.listen((message) {
        if (!result.isCompleted) {
          result.complete(message.payload ?? Uint8List(0));
          message.ack();
        }
      });
      return await result.future.timeout(Duration(milliseconds: timeoutMs));
    } on TimeoutException {
      throw const ChannelFailed('no message available');
    } on Object catch (e) {
      throw ChannelFailed(e.toString());
    } finally {
      await consumer?.cancel();
    }
  }

  /// Close the channel. Safe to call even if already closed.
  Future<void> close() async {
    try {
      await rawChannel.close();
    } on Object {
      // Best-effort.
    }
  }

  // ===========================================================================
  // Internal helpers
  // ===========================================================================

  Future<amqp.Queue> _queueHandle(String name) async {
    final cached = _queues[name];
    if (cached != null) return cached;
    final queue = await rawChannel.queue(name, declare: false);
    return _queues[name] = queue;
  }

  Future<amqp.Exchange> _exchangeHandle(String name) async {
    final cached = _exchanges[name];
    if (cached != null) return cached;
    // We don't know the real type, but a passive declare ignores it.
    final exchange =
        await rawChannel.exchange(name, amqp.ExchangeType.DIRECT, passive: true);
    return _exchanges[name] = exchange;
  }

  static amqp.ExchangeType _exchangeType(ExchangeKind kind) {
    switch (kind) {
      case ExchangeKind.direct:
        return amqp.ExchangeType.DIRECT;
      case ExchangeKind.fanout:
        return amqp.ExchangeType.FANOUT;
      case ExchangeKind.topic:
        return amqp.ExchangeType.TOPIC;
      case ExchangeKind.headers:
        return amqp.ExchangeType.HEADERS;
    }
  }

  static amqp.MessageProperties _propertiesOf(PublishOptions options) {
    final properties = amqp.MessageProperties();
    if (options.headers.isNotEmpty) {
      properties.headers = Map<String, Object?>.from(options.headers);
    }
    if (options.priority != null) properties.priority = options.priority;
    if (options.expiration != null) {
      properties.expiration = options.expiration.toString();
    }
    if (options.contentType != null) {
      properties.contentType = options.contentType;
    }
    if (options.persistent) properties.persistent = true;
    for (final property in options.properties) {
      switch (property) {
        case CorrelationId(:final value):
          properties.corellationId = value;
        case ReplyTo(:final value):
          properties.replyTo = value;
        case MessageId(:final value):
          properties.messageId = value;
        case MessageType(:final value):
          properties.type = value;
        case UserId(:final value):
          properties.userId = value;
        case AppId(:final value):
          properties.appId = value;
        case ContentEncoding(:final value):
          properties.contentEncoding = value;
        case Timestamp(:final seconds):
          properties.timestamp =
              DateTime.fromMillisecondsSinceEpoch(seconds * 1000, isUtc: true);
      }
    }
    return properties;
  }
}
