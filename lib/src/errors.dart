/// Anything that can go wrong talking to the broker.
///
/// wren surfaces failures as exceptions rather than a `Result` type — the
/// idiomatic Dart choice. Every async operation that can fail throws a
/// [WrenError] subtype, so a single `on WrenError catch` handles them all.
sealed class WrenError implements Exception {
  const WrenError(this.reason);

  /// A human-readable description of what went wrong.
  final String reason;

  @override
  String toString() => '$runtimeType: $reason';
}

/// Failed to establish the underlying AMQP connection.
final class ConnectionFailed extends WrenError {
  const ConnectionFailed(super.reason);
}

/// A channel-level operation (declare, publish, consume, …) failed.
final class ChannelFailed extends WrenError {
  const ChannelFailed(super.reason);
}

/// A value could not be serialised before publishing.
final class EncodingFailed extends WrenError {
  const EncodingFailed(super.reason);
}

/// A payload could not be deserialised into the expected type.
final class DecodingFailed extends WrenError {
  const DecodingFailed(super.reason);
}
