/// The routing behaviour of an exchange.
enum ExchangeKind {
  /// Exact routing-key match.
  direct('direct'),

  /// Broadcast to every bound queue.
  fanout('fanout'),

  /// Wildcard routing-key match (`*` / `#`).
  topic('topic'),

  /// Match on message headers rather than routing key.
  headers('headers');

  const ExchangeKind(this.wireName);

  /// The on-the-wire exchange-type string.
  final String wireName;
}

/// A typed value for an AMQP argument (the `x-*` settings on queues, exchanges
/// and bindings — e.g. `x-message-ttl`, `x-dead-letter-exchange`).
sealed class Arg {
  const Arg();

  /// An integer argument.
  const factory Arg.int(int value) = IntArg;

  /// A string argument.
  const factory Arg.string(String value) = StringArg;

  /// A boolean argument.
  const factory Arg.bool(bool value) = BoolArg;

  /// The underlying value, as the dynamic `Object` dart_amqp expects.
  Object get value;

  /// Convert a typed argument map into the `Map<String, Object>` dart_amqp
  /// takes for queue/exchange/binding arguments.
  static Map<String, Object> toAmqp(Map<String, Arg> args) =>
      args.map((key, arg) => MapEntry(key, arg.value));
}

/// An integer argument.
final class IntArg extends Arg {
  const IntArg(this.value);
  @override
  final int value;
}

/// A string argument.
final class StringArg extends Arg {
  const StringArg(this.value);
  @override
  final String value;
}

/// A boolean argument.
final class BoolArg extends Arg {
  const BoolArg(this.value);
  @override
  final bool value;
}

/// Settings for declaring a queue. Durable, non-exclusive, non-auto-delete with
/// no extra arguments is the sensible default.
final class QueueOptions {
  const QueueOptions({
    this.durable = true,
    this.exclusive = false,
    this.autoDelete = false,
    this.arguments = const {},
  });

  /// Survive a broker restart.
  final bool durable;

  /// Private to the declaring connection.
  final bool exclusive;

  /// Delete the queue once its last consumer goes away.
  final bool autoDelete;

  /// Extra `x-*` arguments.
  final Map<String, Arg> arguments;

  /// Return a copy with the given fields overridden.
  QueueOptions copyWith({
    bool? durable,
    bool? exclusive,
    bool? autoDelete,
    Map<String, Arg>? arguments,
  }) {
    return QueueOptions(
      durable: durable ?? this.durable,
      exclusive: exclusive ?? this.exclusive,
      autoDelete: autoDelete ?? this.autoDelete,
      arguments: arguments ?? this.arguments,
    );
  }
}

/// Settings for declaring an exchange. Durable, non-auto-delete, non-internal
/// with no extra arguments is the sensible default.
final class ExchangeOptions {
  const ExchangeOptions({
    this.durable = true,
    this.autoDelete = false,
    this.internal = false,
    this.arguments = const {},
  });

  /// Survive a broker restart.
  final bool durable;

  /// Delete the exchange once its last binding goes away.
  final bool autoDelete;

  /// Internal exchanges can't be published to directly by clients.
  final bool internal;

  /// Extra `x-*` arguments.
  final Map<String, Arg> arguments;
}

/// How a consumer subscribes (the `basic.consume` knobs).
final class ConsumeOptions {
  const ConsumeOptions({
    this.autoAck = false,
    this.exclusive = false,
    this.noLocal = false,
    this.consumerTag,
    this.arguments = const {},
  });

  /// Let the broker consider messages acknowledged on delivery. With this on,
  /// the handler's [Confirmation] can't be honoured — the message is already
  /// gone — so settlement is skipped.
  final bool autoAck;

  /// Request to be the only consumer on the queue.
  final bool exclusive;

  /// Don't deliver messages published on this connection back to it.
  final bool noLocal;

  /// A specific consumer tag (the broker generates one if `null`).
  final String? consumerTag;

  /// Extra `x-*` arguments for the subscription (e.g. consumer priority).
  final Map<String, Arg> arguments;

  /// Return a copy with the given fields overridden.
  ConsumeOptions copyWith({
    bool? autoAck,
    bool? exclusive,
    bool? noLocal,
    String? consumerTag,
    Map<String, Arg>? arguments,
  }) {
    return ConsumeOptions(
      autoAck: autoAck ?? this.autoAck,
      exclusive: exclusive ?? this.exclusive,
      noLocal: noLocal ?? this.noLocal,
      consumerTag: consumerTag ?? this.consumerTag,
      arguments: arguments ?? this.arguments,
    );
  }
}
