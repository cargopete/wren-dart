// A self-healing consumer. It owns its connection and reconnects on its own —
// try restarting the broker while it runs. Run with:
//   dart run example/recovery.dart
import 'package:wren/wren.dart';

Future<void> main() async {
  final router = Router().handleWith<String>(
    'order.created',
    Codec.string(),
    (body, message) {
      print('processed "$body"');
      return Confirmation.ack;
    },
  ).fallback((_) => Confirmation.reject);

  final consumer = await RecoverableConsumer.startRouter(
    const Config(),
    'orders',
    router,
    RecoverableOptions(
      maxConcurrent: 4,
      onConnect: (connection) async {
        final channel = await connection.openChannel();
        await channel.declareQueue('orders');
        await channel.close();
        print('connected; topology declared');
      },
    ),
  );

  print('consuming — press Ctrl-C to stop');
  await Future<void>.delayed(const Duration(seconds: 30));
  await consumer.stop();
}
