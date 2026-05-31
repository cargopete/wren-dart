import 'package:test/test.dart';
import 'package:wren/wren.dart';

void main() {
  group('PublishOptions builder', () {
    test('starts on the default exchange with no routing', () {
      final options = PublishOptions();
      expect(options.exchange, '');
      expect(options.routingKey, '');
      expect(options.headers, isEmpty);
      expect(options.persistent, isFalse);
    });

    test('chains routing, headers, and flags immutably', () {
      final base = PublishOptions();
      final options = base
          .toExchange('events')
          .route('orders')
          .withHeader('x', '1')
          .withPriority(5)
          .withExpiration(1000)
          .asMandatory()
          .asPersistent()
          .withContentType('application/json');

      expect(options.exchange, 'events');
      expect(options.routingKey, 'orders');
      expect(options.headers['x'], '1');
      expect(options.priority, 5);
      expect(options.expiration, 1000);
      expect(options.mandatory, isTrue);
      expect(options.persistent, isTrue);
      expect(options.contentType, 'application/json');

      // The original is untouched.
      expect(base.exchange, '');
      expect(base.headers, isEmpty);
    });

    test('withKind sets the kind header', () {
      final options = PublishOptions().withKind('order.created');
      expect(options.headers[kindHeader], 'order.created');
    });

    test('property helpers append AMQP properties', () {
      final options = PublishOptions()
          .withCorrelationId('abc')
          .withReplyTo('replies')
          .withMessageId('m-1');
      expect(options.properties, contains(isA<CorrelationId>()));
      expect(options.properties, contains(isA<ReplyTo>()));
      expect(options.properties, contains(isA<MessageId>()));
    });
  });
}
