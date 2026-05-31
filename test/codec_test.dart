import 'dart:convert';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:wren/wren.dart';

void main() {
  group('Codec.json', () {
    final codec = Codec.json<Map<String, Object?>>(
      toJson: (value) => value,
      fromJson: (json) => (json as Map).cast<String, Object?>(),
    );

    test('round-trips a value', () {
      final payload = codec.encode({'id': 'A-1', 'qty': 3});
      final decoded = codec.decode(payload);
      expect(decoded, {'id': 'A-1', 'qty': 3});
    });

    test('throws DecodeError on invalid JSON', () {
      final payload = Uint8List.fromList(utf8.encode('not json'));
      expect(() => codec.decode(payload), throwsA(isA<DecodeError>()));
    });

    test('throws DecodeError on invalid UTF-8', () {
      final payload = Uint8List.fromList([0xff, 0xfe]);
      expect(() => codec.decode(payload), throwsA(isA<DecodeError>()));
    });
  });

  group('Codec.string', () {
    final codec = Codec.string();

    test('round-trips text', () {
      expect(codec.decode(codec.encode('héllo')), 'héllo');
    });

    test('throws DecodeError on invalid UTF-8', () {
      expect(
        () => codec.decode(Uint8List.fromList([0xff])),
        throwsA(isA<DecodeError>()),
      );
    });
  });

  group('Codec.bytes', () {
    test('passes payloads through unchanged', () {
      final codec = Codec.bytes();
      final bytes = Uint8List.fromList([1, 2, 3]);
      expect(codec.decode(codec.encode(bytes)), bytes);
    });
  });
}
