import 'dart:typed_data';

import 'errors.dart';
import 'message.dart';

/// A standard AMQP message property. Set these on [PublishOptions] via the
/// `with*` helpers; `correlationId` + `replyTo` are the basis of request/reply.
sealed class Property {
  const Property();
}

/// Pairs a reply with its request — the heart of RPC.
final class CorrelationId extends Property {
  const CorrelationId(this.value);
  final String value;
}

/// Where a responder should send its answer.
final class ReplyTo extends Property {
  const ReplyTo(this.value);
  final String value;
}

/// A unique message identifier.
final class MessageId extends Property {
  const MessageId(this.value);
  final String value;
}

/// The application message type.
final class MessageType extends Property {
  const MessageType(this.value);
  final String value;
}

/// The identity of the publishing user.
final class UserId extends Property {
  const UserId(this.value);
  final String value;
}

/// The identity of the publishing application.
final class AppId extends Property {
  const AppId(this.value);
  final String value;
}

/// The payload's content encoding (e.g. `gzip`).
final class ContentEncoding extends Property {
  const ContentEncoding(this.value);
  final String value;
}

/// A POSIX timestamp (seconds).
final class Timestamp extends Property {
  const Timestamp(this.seconds);
  final int seconds;
}

/// Options controlling how a message is published. Build with [PublishOptions.new]
/// and refine with the fluent `to*` / `with*` helpers, e.g.
/// `PublishOptions().route('orders').withPriority(5)`.
///
/// Mirrors the producer surface of the Rust `bunnyhop` crate: routing, headers,
/// priority, per-message expiration, the `mandatory` flag, and the standard
/// message properties.
final class PublishOptions {
  const PublishOptions({
    this.exchange = '',
    this.routingKey = '',
    this.headers = const {},
    this.priority,
    this.expiration,
    this.mandatory = false,
    this.contentType,
    this.persistent = false,
    this.properties = const [],
  });

  /// Exchange to publish to. `''` is the default exchange (route by queue name).
  final String exchange;

  /// Routing key (or queue name when using the default exchange).
  final String routingKey;

  /// String headers, carried as an AMQP field table.
  final Map<String, String> headers;

  /// Message priority (0–255 on a priority queue).
  final int? priority;

  /// Per-message TTL in milliseconds before the broker discards it.
  final int? expiration;

  /// Ask the broker to return the message if it can't be routed to a queue.
  final bool mandatory;

  /// MIME content type, e.g. `application/json`.
  final String? contentType;

  /// Persist the message (delivery mode 2) so it survives a broker restart on a
  /// durable queue.
  final bool persistent;

  /// Standard AMQP message properties (correlation id, reply-to, …).
  final List<Property> properties;

  PublishOptions _copy({
    String? exchange,
    String? routingKey,
    Map<String, String>? headers,
    int? priority,
    int? expiration,
    bool? mandatory,
    String? contentType,
    bool? persistent,
    List<Property>? properties,
  }) {
    return PublishOptions(
      exchange: exchange ?? this.exchange,
      routingKey: routingKey ?? this.routingKey,
      headers: headers ?? this.headers,
      priority: priority ?? this.priority,
      expiration: expiration ?? this.expiration,
      mandatory: mandatory ?? this.mandatory,
      contentType: contentType ?? this.contentType,
      persistent: persistent ?? this.persistent,
      properties: properties ?? this.properties,
    );
  }

  /// Set the target exchange.
  PublishOptions toExchange(String exchange) => _copy(exchange: exchange);

  /// Set the routing key (or queue name, on the default exchange).
  PublishOptions route(String routingKey) => _copy(routingKey: routingKey);

  /// Append a single header.
  PublishOptions withHeader(String key, String value) =>
      _copy(headers: {...headers, key: value});

  /// Replace all headers at once.
  PublishOptions withHeaders(Map<String, String> headers) =>
      _copy(headers: headers);

  /// Set the message priority.
  PublishOptions withPriority(int priority) => _copy(priority: priority);

  /// Set a per-message expiration (TTL) in milliseconds.
  PublishOptions withExpiration(int millis) => _copy(expiration: millis);

  /// Mark the publish as mandatory (broker returns unroutable messages).
  PublishOptions asMandatory() => _copy(mandatory: true);

  /// Set the MIME content type.
  PublishOptions withContentType(String contentType) =>
      _copy(contentType: contentType);

  /// Set the message `kind` header — the discriminator a router uses to pick a
  /// handler. Sugar over `withHeader(kindHeader, kind)`.
  PublishOptions withKind(String kind) => withHeader(kindHeader, kind);

  /// Mark the message persistent (delivery mode 2).
  PublishOptions asPersistent() => _copy(persistent: true);

  /// Add a raw AMQP message [Property].
  PublishOptions withProperty(Property property) =>
      _copy(properties: [property, ...properties]);

  /// Set the correlation id — pair a reply with its request.
  PublishOptions withCorrelationId(String id) =>
      withProperty(CorrelationId(id));

  /// Set the reply-to queue — where a responder should send its answer.
  PublishOptions withReplyTo(String queue) => withProperty(ReplyTo(queue));

  /// Set the message id.
  PublishOptions withMessageId(String id) => withProperty(MessageId(id));

  /// Set the application message type.
  PublishOptions withMessageType(String type) =>
      withProperty(MessageType(type));

  /// Set the timestamp (POSIX seconds).
  PublishOptions withTimestamp(int seconds) => withProperty(Timestamp(seconds));
}

/// A publish destination: an exchange and routing key.
final class Target {
  const Target({required this.exchange, required this.routingKey});

  /// A target on the default exchange, routing by queue name.
  const Target.queue(String queue)
      : exchange = '',
        routingKey = queue;

  final String exchange;
  final String routingKey;
}

/// One message that couldn't be published, with the reason.
final class BatchFailure {
  const BatchFailure({
    required this.target,
    required this.payload,
    required this.error,
  });

  final Target target;
  final Uint8List payload;
  final WrenError error;
}

/// The outcome of a batch publish: how many succeeded, and which failed.
final class BatchResult {
  const BatchResult({required this.published, required this.failures});

  /// How many messages were published successfully.
  final int published;

  /// The messages that couldn't be published, with their reasons.
  final List<BatchFailure> failures;
}
