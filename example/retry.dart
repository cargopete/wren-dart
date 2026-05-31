// Fail once, then succeed via a delay queue. Bring up a broker first, then run
// with: dart run example/retry.dart
import 'package:wren/wren.dart';

Future<void> main() async {
  final connection = await WrenConnection.connect(const Config());
  final channel = await connection.openChannel();

  final infra = RetryInfrastructure.forQueue(
    'orders',
    const RetryPolicy(
      strategy: ExponentialBackoff(
        initialMs: 1000,
        maxMs: 60000,
        multiplier: 2.0,
      ),
      maxAttempts: 5,
    ),
  );

  var attempts = 0;
  final router = Router().handleWith<String>(
    'order.created',
    Codec.string(),
    (body, message) {
      attempts++;
      if (attempts == 1) {
        print('first attempt for "$body" — asking for a retry');
        return Confirmation.retry;
      }
      print('second attempt for "$body" — success');
      return Confirmation.ack;
    },
  );

  final consumer = await channel.startRouterWithRetry(router, infra);

  await channel.publishEncoded(
    'order-payload',
    Codec.string(),
    PublishOptions().route('orders').withKind('order.created'),
  );

  await Future<void>.delayed(const Duration(seconds: 3));

  await consumer.stop();
  await channel.close();
  await connection.close();
}
