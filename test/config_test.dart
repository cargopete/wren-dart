import 'package:test/test.dart';
import 'package:wren/wren.dart';

void main() {
  group('Config.fromLookup', () {
    test('reads RABBITMQ_* values', () {
      final env = {
        'RABBITMQ_HOST': 'broker.internal',
        'RABBITMQ_PORT': '5673',
        'RABBITMQ_USER': 'app',
        'RABBITMQ_PASSWORD': 's3cret',
        'RABBITMQ_VHOST': '/prod',
        'RABBITMQ_HEARTBEAT': '30',
      };
      final config = Config.fromLookup((key) => env[key]);
      expect(config.host, 'broker.internal');
      expect(config.port, 5673);
      expect(config.username, 'app');
      expect(config.password, 's3cret');
      expect(config.virtualHost, '/prod');
      expect(config.heartbeatSeconds, 30);
    });

    test('falls back to defaults for missing or invalid values', () {
      final config = Config.fromLookup((key) =>
          key == 'RABBITMQ_PORT' ? 'not-a-number' : null);
      const defaults = Config();
      expect(config.host, defaults.host);
      expect(config.port, defaults.port);
      expect(config.username, defaults.username);
    });
  });

  group('Config.validate', () {
    test('accepts a well-formed config', () {
      expect(const Config().validate, returnsNormally);
    });

    test('rejects an empty host', () {
      expect(
        const Config(host: '').validate,
        throwsA(isA<ConnectionFailed>()),
      );
    });

    test('rejects an out-of-range port', () {
      expect(
        const Config(port: 70000).validate,
        throwsA(isA<ConnectionFailed>()),
      );
    });
  });
}
