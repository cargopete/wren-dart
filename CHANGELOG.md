# Changelog

## 0.2.0 — Consumers

- `WrenChannel.startConsumer` / `startRouter` (plus `*WithRetry` variants) with
  prefetch-bounded concurrency and per-delivery settlement.
- `Router` — dispatch by message `kind` to typed handlers, with a fallback.
- `RetryInfrastructure` — delay-queue + dead-letter topology, and retry /
  dead-letter rerouting of settled messages.
- `RecoverableConsumer` — a self-healing consumer that reconnects with capped
  exponential backoff and re-subscribes.
- `Pool` — round-robin connection pool, and `Client` — a connection+channel
  front door.

## 0.1.0 — Foundation

The producer-side foundation of the Dart port, wrapping `dart_amqp`.

- Connections & channels (`WrenConnection`, `WrenChannel`) with TLS, heartbeat,
  and connection-timeout support.
- Topology: declare queues (incl. passive) and exchanges, bind/unbind, delete
  with guards, purge, and typed `x-*` arguments (`Arg`).
- Producer surface: `publish` / `publishText` / `publishWithOptions`, a fluent
  `PublishOptions` builder (headers, priority, expiration, mandatory,
  persistence, content type), full AMQP message properties (`correlationId`,
  `replyTo`, …), batch / multi-target publishing, and kind-based routing.
- Publisher confirms (`enableConfirms` / `publishConfirmed`).
- Codecs: `Codec.json` / `Codec.string` / `Codec.bytes`, plus `publishEncoded`.
- Retry policy & metadata types (`RetryPolicy`, `RetryMetadata`).
- Config: `Config`, `Config.fromEnv`, `Config.fromLookup`, validation, and TLS.
- One-off `get` (emulated via a short-lived consumer).
