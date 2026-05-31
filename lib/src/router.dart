import 'dart:async';

import 'codec.dart';
import 'log.dart';
import 'message.dart';

/// A handler for an already-decoded value of type [T].
typedef TypedHandler<T> = FutureOr<Confirmation> Function(T value);

/// A handler that also receives the raw [Message] as context.
typedef TypedHandlerWith<T> = FutureOr<Confirmation> Function(
  T value,
  Message message,
);

/// A handler for a raw [Message].
typedef MessageHandler = FutureOr<Confirmation> Function(Message message);

/// Routes deliveries to handlers by their `kind` header. Build with [Router.new],
/// register typed handlers with [handle] / [handleWith], set a catch-all with
/// [fallback], then run it with `channel.startRouter`.
///
/// This is wren's idiomatic take on bunnyhop's `Router`: each typed handler is
/// erased to a [MessageHandler] by closing over its codec, so handlers for
/// different message types live in one table.
final class Router {
  Router._(this._handlers, this._fallback);

  /// A new router whose default fallback rejects unrouted messages with a
  /// warning.
  Router() : _handlers = const {}, _fallback = _warnAndReject;

  final Map<String, MessageHandler> _handlers;
  final MessageHandler _fallback;

  /// Register a handler for messages of [kind]. The payload is decoded with
  /// [codec]; on a decode failure the message is rejected (and a warning
  /// logged), so the handler only ever sees well-formed values.
  Router handle<T>(String kind, Codec<T> codec, TypedHandler<T> handler) =>
      handleWith<T>(kind, codec, (value, _) => handler(value));

  /// Like [handle], but the handler also receives the raw [Message] — its
  /// headers, routing key, and undecoded payload — as context.
  Router handleWith<T>(
    String kind,
    Codec<T> codec,
    TypedHandlerWith<T> handler,
  ) {
    FutureOr<Confirmation> erased(Message message) {
      final T value;
      try {
        value = codec.decode(message.payload);
      } on CodecError catch (e) {
        logWarning(
          "wren: dropping '$kind' — payload failed to decode: ${e.reason}",
        );
        return Confirmation.reject;
      }
      return handler(value, message);
    }

    return Router._({..._handlers, kind: erased}, _fallback);
  }

  /// Set the fallback handler invoked for messages whose `kind` has no
  /// registered handler (or that carry no `kind` header at all).
  Router fallback(MessageHandler handler) => Router._(_handlers, handler);

  /// Dispatch [message] to the handler registered for its kind, or the fallback.
  FutureOr<Confirmation> dispatch(Message message) {
    final kind = message.kind;
    final handler = (kind == null ? null : _handlers[kind]) ?? _fallback;
    return handler(message);
  }
}

Confirmation _warnAndReject(Message message) {
  logWarning("wren: no handler for kind '${message.kind ?? '<none>'}', rejecting");
  return Confirmation.reject;
}
