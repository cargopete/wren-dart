// End-to-end tests against a real broker. Disabled by default; enable with:
//   docker compose up -d
//   WREN_INTEGRATION=1 dart test test/integration_test.dart
import 'dart:async';
import 'dart:io';

import 'package:test/test.dart';
import 'package:wren/wren.dart';

void main() {
  final enabled = Platform.environment['WREN_INTEGRATION'] == '1';
  final skip = enabled ? false : 'set WREN_INTEGRATION=1 (and run a broker)';

  group('against a live broker', () {
    late WrenConnection connection;
    late WrenChannel channel;
    final stringCodec = Codec.string();

    setUp(() async {
      connection = await WrenConnection.connect(const Config());
      channel = await connection.openChannel();
    });

    tearDown(() async {
      await channel.close();
      await connection.close();
    });

    test('publishes and consumes a round trip', () async {
      final queue = 'wren.it.roundtrip.${DateTime.now().microsecondsSinceEpoch}';
      await channel.declareQueue(queue);
      addTearDown(() => channel.deleteQueue(queue));

      final received = Completer<String>();
      final router = Router().handle<String>('greeting', stringCodec, (body) {
        if (!received.isCompleted) received.complete(body);
        return Confirmation.ack;
      });
      final consumer = await channel.startRouter(queue, router);
      addTearDown(consumer.stop);

      await channel.publishEncoded(
        'hello world',
        stringCodec,
        PublishOptions().route(queue).withKind('greeting'),
      );

      final body = await received.future.timeout(const Duration(seconds: 5));
      expect(body, 'hello world');
    });

    test('retries a failed delivery via the delay queue', () async {
      final queue = 'wren.it.retry.${DateTime.now().microsecondsSinceEpoch}';
      final infra = RetryInfrastructure.forQueue(
        queue,
        const RetryPolicy(strategy: FixedInterval(1000), maxAttempts: 3),
      );

      var attempts = 0;
      final succeeded = Completer<void>();
      final router = Router().handle<String>('job', stringCodec, (_) {
        attempts++;
        if (attempts == 1) return Confirmation.retry;
        if (!succeeded.isCompleted) succeeded.complete();
        return Confirmation.ack;
      });

      final consumer = await channel.startRouterWithRetry(router, infra);
      addTearDown(consumer.stop);
      addTearDown(() => channel.deleteQueue(queue));
      addTearDown(() => channel.deleteQueue('$queue.retry'));
      addTearDown(() => channel.deleteQueue('$queue.dlq'));
      addTearDown(() => channel.deleteExchange('$queue.retry'));
      addTearDown(() => channel.deleteExchange('$queue.dlx'));

      await channel.publishEncoded(
        'work',
        stringCodec,
        PublishOptions().route(queue).withKind('job'),
      );

      await succeeded.future.timeout(const Duration(seconds: 8));
      expect(attempts, 2);
    });

    test('get fetches a single message', () async {
      final queue = 'wren.it.get.${DateTime.now().microsecondsSinceEpoch}';
      await channel.declareQueue(queue);
      addTearDown(() => channel.deleteQueue(queue));

      await channel.publishText('', queue, 'one-off');
      final payload = await channel.get(queue, timeoutMs: 3000);
      expect(String.fromCharCodes(payload), 'one-off');
    });
  }, skip: skip);
}
