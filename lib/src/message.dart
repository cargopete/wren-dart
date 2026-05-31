import 'dart:convert';
import 'dart:typed_data';

/// The header carrying a message's kind — the discriminator a router dispatches
/// on. Matches the convention used by the Rust `bunnyhop` crate and Gleam wren.
const String kindHeader = 'kind';

/// A delivered message handed to a consumer's handler. The [payload] is raw
/// bytes; use [text] for the common UTF-8 case, or a codec to decode it.
final class Message {
  const Message({
    required this.payload,
    this.routingKey = '',
    this.headers = const {},
    this.correlationId,
    this.replyTo,
    this.redelivered = false,
  });

  /// The raw message body.
  final Uint8List payload;

  /// The routing key the message was delivered with.
  final String routingKey;

  /// String headers carried on the message (the AMQP field table).
  final Map<String, String> headers;

  /// The AMQP `correlation_id` property, if set (used to pair RPC replies).
  final String? correlationId;

  /// The AMQP `reply_to` property, if set (where to send an RPC reply).
  final String? replyTo;

  /// True if the broker has delivered this message before (e.g. after a requeue).
  final bool redelivered;

  /// The `kind` header off the message, or `null` if absent.
  String? get kind => headers[kindHeader];

  /// The payload decoded as UTF-8 text, or `null` if it isn't valid UTF-8.
  String? get text {
    try {
      return utf8.decode(payload);
    } on FormatException {
      return null;
    }
  }
}

/// How a consumer wishes a delivered message to be settled with the broker.
enum Confirmation {
  /// Processed successfully — remove from the queue.
  ack,

  /// Permanent failure — discard without redelivery or dead-lettering.
  reject,

  /// Transient failure — redeliver for another attempt.
  retry,

  /// Unprocessable — route to the dead-letter exchange, if configured.
  deadLetter,
}

/// The broker's verdict when waiting on a publisher confirm.
enum Confirm {
  /// All messages since the last wait were acknowledged.
  confirmed,

  /// At least one message was negatively acknowledged.
  nacked,

  /// The wait expired before a verdict arrived.
  timedOut,
}
