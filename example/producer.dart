// Publishing: options, confirms, and batches. Run with:
//   dart run example/producer.dart
import 'dart:convert';
import 'dart:typed_data';

import 'package:wren/wren.dart';

Uint8List bytes(String text) => Uint8List.fromList(utf8.encode(text));

Future<void> main() async {
  final client = await Client.start(const Config());
  final channel = client.channel;

  await channel.declareQueue('orders');

  // A confirmed publish: waits for the broker to acknowledge.
  await channel.publishConfirmed(
    bytes('hello'),
    PublishOptions().route('orders').asPersistent(),
    5000,
  );

  // A batch to several targets, collecting per-message failures.
  final result = await channel.publishToTargets(
    bytes('broadcast'),
    [const Target.queue('orders'), const Target.queue('audit')],
    PublishOptions(),
  );
  print('published ${result.published}, failures ${result.failures.length}');

  await client.close();
}
