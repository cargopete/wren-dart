# Changelog

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
