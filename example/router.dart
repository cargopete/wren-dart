// Dispatch typed messages by kind. Bring up a broker first (see the repo's
// docker-compose.yml), then run with: dart run example/router.dart
import 'package:wren/wren.dart';

class Order {
  Order(this.id);
  final String id;
}

final orderCodec = Codec.json<Order>(
  toJson: (order) => {'id': order.id},
  fromJson: (json) => Order((json as Map)['id'] as String),
);

Future<void> main() async {
  final connection = await WrenConnection.connect(const Config());
  final channel = await connection.openChannel();
  await channel.declareQueue('orders');

  final router = Router()
      .handle<Order>('order.created', orderCodec, (order) {
        print('handling order ${order.id}');
        return Confirmation.ack;
      })
      .fallback((message) {
        print('no handler for kind ${message.kind}');
        return Confirmation.reject;
      });

  final consumer = await channel.startRouter('orders', router);

  await channel.publishEncoded(
    Order('A-1'),
    orderCodec,
    PublishOptions().route('orders').withKind('order.created'),
  );

  await Future<void>.delayed(const Duration(seconds: 1));

  await consumer.stop();
  await channel.close();
  await connection.close();
}
