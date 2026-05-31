import 'dart:io';

import 'errors.dart';

/// TLS settings for a connection. [Tls.none] is plaintext; [Tls.secure]
/// enables it.
final class Tls {
  const Tls._({
    required this.enabled,
    this.verify = true,
    this.caCertFile,
    this.certFile,
    this.keyFile,
  });

  /// Plaintext connection (the default).
  static const Tls none = Tls._(enabled: false);

  /// A TLS connection. [verify] toggles peer-certificate verification; the
  /// optional PEM paths are a CA bundle, a client certificate, and its key.
  const Tls.secure({
    bool verify = true,
    String? caCertFile,
    String? certFile,
    String? keyFile,
  }) : this._(
          enabled: true,
          verify: verify,
          caCertFile: caCertFile,
          certFile: certFile,
          keyFile: keyFile,
        );

  /// Whether TLS is enabled.
  final bool enabled;

  /// Whether to verify the broker's certificate.
  final bool verify;

  /// Path to a CA-bundle PEM file, if any.
  final String? caCertFile;

  /// Path to a client-certificate PEM file, if any.
  final String? certFile;

  /// Path to the client private-key PEM file, if any.
  final String? keyFile;

  /// Build a dart:io [SecurityContext] from the configured PEM paths, or `null`
  /// if TLS is disabled.
  SecurityContext? toSecurityContext() {
    if (!enabled) return null;
    final context = SecurityContext(withTrustedRoots: true);
    if (caCertFile != null) context.setTrustedCertificates(caCertFile!);
    if (certFile != null) context.useCertificateChain(certFile!);
    if (keyFile != null) context.usePrivateKey(keyFile!);
    return context;
  }
}

/// Connection settings. Build via [Config.new] (which carries sensible
/// localhost defaults) and override as needed, or [Config.fromEnv].
final class Config {
  /// Sensible localhost defaults (the classic `guest`/`guest`, vhost `/`,
  /// plaintext).
  const Config({
    this.host = 'localhost',
    this.port = 5672,
    this.username = 'guest',
    this.password = 'guest',
    this.virtualHost = '/',
    this.heartbeatSeconds = 60,
    this.connectionTimeoutMs = 10000,
    this.tls = Tls.none,
  });

  /// The broker host.
  final String host;

  /// The broker port.
  final int port;

  /// The login username.
  final String username;

  /// The login password.
  final String password;

  /// The AMQP virtual host (`"/"` is the broker default).
  final String virtualHost;

  /// Heartbeat interval in seconds (`0` disables heartbeats).
  final int heartbeatSeconds;

  /// How long to wait for the connection to establish, in milliseconds.
  final int connectionTimeoutMs;

  /// TLS settings ([Tls.none] for plaintext).
  final Tls tls;

  /// Return a copy with the given fields overridden.
  Config copyWith({
    String? host,
    int? port,
    String? username,
    String? password,
    String? virtualHost,
    int? heartbeatSeconds,
    int? connectionTimeoutMs,
    Tls? tls,
  }) {
    return Config(
      host: host ?? this.host,
      port: port ?? this.port,
      username: username ?? this.username,
      password: password ?? this.password,
      virtualHost: virtualHost ?? this.virtualHost,
      heartbeatSeconds: heartbeatSeconds ?? this.heartbeatSeconds,
      connectionTimeoutMs: connectionTimeoutMs ?? this.connectionTimeoutMs,
      tls: tls ?? this.tls,
    );
  }

  /// Build a [Config] from the environment, reading the `RABBITMQ_*` variables.
  /// Anything unset (or an unparseable number) falls back to the defaults.
  ///
  /// Recognised: `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USERNAME` (or
  /// `RABBITMQ_USER`), `RABBITMQ_PASSWORD` (or `RABBITMQ_PASS`),
  /// `RABBITMQ_VHOST`, `RABBITMQ_HEARTBEAT`, `RABBITMQ_CONNECTION_TIMEOUT`.
  factory Config.fromEnv() =>
      Config.fromLookup((key) => Platform.environment[key]);

  /// Build a [Config] from an arbitrary lookup function (env, a map, a config
  /// file…). Keys are the same `RABBITMQ_*` names as [Config.fromEnv]; missing
  /// or invalid values fall back to the defaults.
  factory Config.fromLookup(String? Function(String key) lookup) {
    const defaults = Config();
    String firstOf(List<String> keys, String fallback) {
      for (final key in keys) {
        final value = lookup(key);
        if (value != null) return value;
      }
      return fallback;
    }

    int intOr(String key, int fallback) {
      final raw = lookup(key);
      if (raw == null) return fallback;
      return int.tryParse(raw) ?? fallback;
    }

    return Config(
      host: firstOf(['RABBITMQ_HOST'], defaults.host),
      port: intOr('RABBITMQ_PORT', defaults.port),
      username: firstOf(['RABBITMQ_USERNAME', 'RABBITMQ_USER'], defaults.username),
      password: firstOf(['RABBITMQ_PASSWORD', 'RABBITMQ_PASS'], defaults.password),
      virtualHost: firstOf(['RABBITMQ_VHOST'], defaults.virtualHost),
      heartbeatSeconds: intOr('RABBITMQ_HEARTBEAT', defaults.heartbeatSeconds),
      connectionTimeoutMs:
          intOr('RABBITMQ_CONNECTION_TIMEOUT', defaults.connectionTimeoutMs),
    );
  }

  /// Check this config is well-formed (non-empty host, valid port). Throws
  /// [ConnectionFailed] if not. Useful after [Config.fromEnv], which is lenient.
  void validate() {
    if (host.isEmpty) {
      throw const ConnectionFailed('host must not be empty');
    }
    if (port < 1 || port > 65535) {
      throw const ConnectionFailed('port must be between 1 and 65535');
    }
  }
}
