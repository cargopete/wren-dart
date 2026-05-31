import 'dart:async';
import 'dart:math' as math;

import 'config.dart';
import 'connection.dart';
import 'consumer.dart';
import 'log.dart';
import 'retry_infrastructure.dart';
import 'router.dart';
import 'topology.dart';

/// Tuning for a recoverable consumer.
final class RecoverableOptions {
  const RecoverableOptions({
    this.prefetch,
    this.retry,
    this.onConnect,
    this.initialBackoffMs = 500,
    this.maxBackoffMs = 30000,
    this.maxConcurrent = 1,
    this.consume = const ConsumeOptions(),
  });

  /// Apply a channel prefetch each time the consumer (re)connects.
  final int? prefetch;

  /// Retry infrastructure to (re)declare on each connect and back the consumer.
  final RetryInfrastructure? retry;

  /// A hook run every time the consumer (re)establishes its connection — handy
  /// for re-declaring topology, emitting metrics, or logging.
  final FutureOr<void> Function(WrenConnection connection)? onConnect;

  /// The starting reconnection backoff, in milliseconds.
  final int initialBackoffMs;

  /// The ceiling on the reconnection backoff, in milliseconds.
  final int maxBackoffMs;

  /// Process up to this many deliveries at once (each bounded by prefetch).
  final int maxConcurrent;

  /// Subscription options (auto-ack, exclusive, consumer tag, …).
  final ConsumeOptions consume;

  /// Return a copy with the given fields overridden.
  RecoverableOptions copyWith({
    int? prefetch,
    RetryInfrastructure? retry,
    FutureOr<void> Function(WrenConnection)? onConnect,
    int? initialBackoffMs,
    int? maxBackoffMs,
    int? maxConcurrent,
    ConsumeOptions? consume,
  }) {
    return RecoverableOptions(
      prefetch: prefetch ?? this.prefetch,
      retry: retry ?? this.retry,
      onConnect: onConnect ?? this.onConnect,
      initialBackoffMs: initialBackoffMs ?? this.initialBackoffMs,
      maxBackoffMs: maxBackoffMs ?? this.maxBackoffMs,
      maxConcurrent: maxConcurrent ?? this.maxConcurrent,
      consume: consume ?? this.consume,
    );
  }
}

/// A self-healing consumer that owns its own connection. If the connection
/// drops, it reconnects with capped exponential backoff, re-opening the channel
/// and re-subscribing — an explicit backoff loop standing in for the Gleam
/// original's OTP supervision.
///
/// When [RecoverableOptions.retry] is set, the consumer subscribes to the
/// infrastructure's main queue (and the `queue` argument is ignored).
final class RecoverableConsumer {
  RecoverableConsumer._(this._config, this._queue, this._handler, this._options);

  final Config _config;
  final String _queue;
  final MessageHandler _handler;
  final RecoverableOptions _options;

  WrenConnection? _connection;
  WrenConsumer? _consumer;
  bool _running = true;
  bool _reconnecting = false;

  /// Start a recoverable consumer dispatching deliveries to [handler].
  static Future<RecoverableConsumer> start(
    Config config,
    String queue,
    MessageHandler handler,
    RecoverableOptions options,
  ) async {
    final consumer = RecoverableConsumer._(config, queue, handler, options);
    await consumer._establish();
    return consumer;
  }

  /// Start a recoverable consumer that dispatches deliveries through [router].
  static Future<RecoverableConsumer> startRouter(
    Config config,
    String queue,
    Router router,
    RecoverableOptions options,
  ) =>
      start(config, queue, router.dispatch, options);

  Future<void> _establish() async {
    final connection = await WrenConnection.connect(_config);
    final channel = await connection.openChannel();

    final prefetch = _options.prefetch ??
        (_options.maxConcurrent > 1 ? _options.maxConcurrent : null);
    if (prefetch != null) await channel.qos(prefetch);

    final retry = _options.retry;
    if (retry != null) await retry.setup(channel);
    final subscribeQueue = retry?.mainQueue ?? _queue;

    final consumer = await channel.startConsumer(
      subscribeQueue,
      _handler,
      retry: retry,
      maxConcurrent: _options.maxConcurrent,
      options: _options.consume,
    );

    _connection = connection;
    _consumer = consumer;

    // A fatal connection error wakes the reconnection loop.
    connection.rawClient.errorListener((_) => _onConnectionDown());
    await _options.onConnect?.call(connection);
  }

  void _onConnectionDown() {
    if (!_running || _reconnecting) return;
    _reconnecting = true;
    logWarning('wren: connection lost; reconnecting…');
    unawaited(_reconnectLoop(_options.initialBackoffMs));
  }

  Future<void> _reconnectLoop(int backoffMs) async {
    await _connection?.close();
    var delay = backoffMs;
    while (_running) {
      await Future<void>.delayed(Duration(milliseconds: delay));
      try {
        await _establish();
        _reconnecting = false;
        return;
      } on Object {
        delay = math.min(delay * 2, _options.maxBackoffMs);
        logWarning('wren: reconnect failed; retrying in ${delay}ms');
      }
    }
  }

  /// Stop the consumer and close its connection.
  Future<void> stop() async {
    _running = false;
    await _consumer?.stop();
    await _connection?.close();
  }
}
