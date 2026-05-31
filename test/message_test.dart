import 'dart:convert';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:wren/wren.dart';

Uint8List _bytes(String text) => Uint8List.fromList(utf8.encode(text));

void main() {
  group('Message', () {
    test('exposes the kind header and text', () {
      final message = Message(
        payload: _bytes('hi'),
        headers: const {kindHeader: 'greeting'},
      );
      expect(message.kind, 'greeting');
      expect(message.text, 'hi');
    });

    test('decode returns a typed value', () {
      final message = Message(payload: _bytes('{"id":"A-1"}'));
      final codec = Codec.json<Map<String, Object?>>(
        toJson: (v) => v,
        fromJson: (j) => (j as Map).cast<String, Object?>(),
      );
      expect(message.decode(codec), {'id': 'A-1'});
    });

    test('decode wraps codec failures as DecodingFailed', () {
      final message = Message(payload: _bytes('not-json'));
      final codec = Codec.json<int>(
        toJson: (v) => v,
        fromJson: (j) => j as int,
      );
      expect(() => message.decode(codec), throwsA(isA<DecodingFailed>()));
    });
  });
}
