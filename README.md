# wren

An ergonomic **RabbitMQ / AMQP** messaging library for [Dart](https://dart.dev),
built as a friendly, type-safe layer over [`dart_amqp`](https://pub.dev/packages/dart_amqp).

> Small, quick, and busy — a wren is a tiny bird that does a great deal. This
> library aims to do the same for your messages: typed publishing, a
> router-style consumer, retries and dead-lettering, and self-healing
> reconnection, without the boilerplate.

This is the Dart sibling of the [Gleam `wren`](https://github.com/cargopete/wren)
library, ported capability-for-capability where the platform allows. Where
Gleam leans on the BEAM and OTP supervision, the Dart port uses idiomatic
`async`/`Future`/`Stream` machinery and an explicit backoff-driven recovery
loop instead.

## Status

**Stable — `1.0`.** Feature-complete against the Gleam original (modulo the
platform deviations noted below), exercised by a unit suite for the pure logic
**and** an integration suite against a real RabbitMQ broker (round-trip publish/
consume, the full retry → delay-queue → dead-letter path, and one-off `get`).

- ✅ Typed connections & channels over `dart_amqp`
- ✅ Topology — queues (incl. passive), exchanges, bindings, deletes with
  guards, purge, and typed `x-*` arguments
- ✅ Producer surface — options, headers, priority, expiration, persistence,
  full AMQP message properties (`correlationId` / `replyTo` for RPC), batch /
  multi-target publishing, and kind-based routing
- ✅ Publisher confirms (`publishConfirmed`)
- ✅ A `Codec` abstraction — JSON / string / bytes, plus `publishEncoded`
- ✅ A consumer with prefetch-bounded concurrency and per-delivery settlement
- ✅ Router-style consumer — dispatch by message `kind` to typed handlers
- ✅ Retry & dead-letter infrastructure — TTL delay queues, a DLX, and a DLQ
- ✅ A self-healing recoverable consumer — capped exponential-backoff reconnection
- ✅ A round-robin connection pool and a `Client` front door
- ✅ Config — `Config.fromEnv`, TLS, vhost / heartbeat / timeout, validation

## Install

```yaml
dependencies:
  wren: ^1.0.0
```

## A quick taste

```dart
import 'dart:typed_data';
import 'package:wren/wren.dart';

Future<void> main() async {
  final connection = await WrenConnection.connect(const Config(host: 'localhost'));
  final channel = await connection.openChannel();

  await channel.declareQueue('orders');

  // A codec is just an encode/decode pair; `Codec.json` builds one from
  // `toJson` / `fromJson` callbacks.
  final orderCodec = Codec.json<Order>(
    toJson: (o) => {'id': o.id},
    fromJson: (j) => Order((j as Map)['id'] as String),
  );

  // Publish, tagging the message with its kind for routing.
  await channel.publishEncoded(
    Order('A-1'),
    orderCodec,
    PublishOptions().route('orders').withKind('order.created'),
  );

  // Consume, routing by kind to typed handlers.
  final router = Router()
      .handle<Order>('order.created', orderCodec, (order) {
        print('got order ${order.id}');
        return Confirmation.ack;
      })
      .fallback((_) => Confirmation.reject);

  final consumer = await channel.startRouter('orders', router);

  // … later …
  await consumer.stop();
  await channel.close();
  await connection.close();
}

class Order {
  Order(this.id);
  final String id;
}
```

Handlers may be sync or `async` and return a [`Confirmation`]: `ack` removes the
message, `reject` discards it, and — with retry infrastructure — `retry`
redelivers after a backoff while `deadLetter` routes to the dead-letter queue.

### Retries & dead-lettering

Give the consumer retry infrastructure and wren builds the delay-queue +
dead-letter topology for you:

```dart
final infra = RetryInfrastructure.forQueue('orders', RetryPolicy.defaults());

// Declares the topology and starts consuming, routing by kind.
final consumer = await channel.startRouterWithRetry(router, infra);
```

### Self-healing consumer

`RecoverableConsumer` owns its own connection and reconnects with capped
exponential backoff, re-subscribing on its own:

```dart
final consumer = await RecoverableConsumer.startRouter(
  const Config(host: 'localhost'),
  'orders',
  router,
  const RecoverableOptions(maxConcurrent: 8),
);
```

## Design

- A typed, builder-style producer API; routing is explicit.
- Codecs are plain values (`Codec.json` / `Codec.string` / `Codec.bytes`) rather
  than typeclass machinery.
- Failures surface as exceptions (`WrenError` subtypes: `ConnectionFailed`,
  `ChannelFailed`, `EncodingFailed`, `DecodingFailed`) — the idiomatic Dart
  choice — so a single `on WrenError catch` covers them all.
- Connection recovery (M2) leans on an explicit backoff loop rather than
  hand-rolled reconnection scattered through call sites.

## Examples

Runnable programs live under [`example/`](./example):

```sh
dart run example/producer.dart   # options, confirms, batches
dart run example/router.dart     # dispatch typed messages by kind
dart run example/retry.dart      # fail once, then succeed via a delay queue
dart run example/recovery.dart   # a self-healing consumer
```

## Development

The integration suite talks to a real broker. Bring one up first:

```sh
docker compose up -d                     # start a local RabbitMQ (guest/guest)
dart test                                # unit suite (no broker needed)
WREN_INTEGRATION=1 dart test             # unit + integration suites
docker compose down                      # stop the broker
```

## Notes & deviations from the Gleam original

These follow from wrapping `dart_amqp` rather than the Erlang `amqp_client`:

- **`Message.redelivered`** is always `false` — `dart_amqp` doesn't surface the
  redelivery flag to consumers.
- **Exchange `autoDelete` / `internal` declare flags** aren't supported by
  `dart_amqp`'s `exchange()` and are ignored.
- **`get`** is emulated with a short-lived, single-prefetch consumer, since
  `dart_amqp` exposes no `basic.get`.
- **Publisher confirms** are matched to the next confirmation rather than by
  delivery tag (the notification carries no tag), so use `publishConfirmed`
  serially.

## Licence

Released under the [MIT](./LICENSE) licence.
