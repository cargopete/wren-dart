import 'package:test/test.dart';
import 'package:wren/wren.dart';

void main() {
  group('KindRouting.apply', () {
    test('uses the table when no exchange is set on the options', () {
      final routing = KindRouting().routeKind('order.created', 'orders');
      final result = routing.apply('order.created', PublishOptions());
      expect(result.exchange, 'orders');
      expect(result.routingKey, 'order.created');
    });

    test('an explicit exchange on the options wins over the table', () {
      final routing = KindRouting().routeKind('order.created', 'orders');
      final result = routing.apply(
        'order.created',
        PublishOptions().toExchange('override'),
      );
      expect(result.exchange, 'override');
    });

    test('an unset routing key defaults to the kind', () {
      final result = KindRouting().apply('order.created', PublishOptions());
      expect(result.exchange, '');
      expect(result.routingKey, 'order.created');
    });

    test('routeKindWithKey uses the explicit key', () {
      final routing =
          KindRouting().routeKindWithKey('order.created', 'orders', 'new');
      final result = routing.apply('order.created', PublishOptions());
      expect(result.routingKey, 'new');
    });
  });
}
