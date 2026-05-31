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

**Building toward `1.0` — milestone-by-milestone.**

- ✅ **M1 — Foundation:** connections & channels, topology (queues, exchanges,
  bindings, deletes, `x-*` arguments), the full producer surface (options,
  headers, priority, expiration, persistence, message properties, batch /
  multi-target, kind-routing), publisher confirms, a `Codec` abstraction
  (JSON / string / bytes), the retry policy & metadata types, config (incl.
  `Config.fromEnv`, TLS, validation), and a one-off `get`.
- ✅ **M2 — Consumers:** a consumer with concurrency control and per-delivery
  settlement, the router-by-kind, retry / dead-letter infrastructure, the
  self-healing recoverable consumer (explicit backoff reconnection), the
  connection pool, and the `Client` front door.
- ⏳ **M3 — Tests & examples:** a unit suite plus runnable example programs.

## Install

```yaml
dependencies:
  wren: ^0.2.0
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
