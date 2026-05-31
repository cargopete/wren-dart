import 'dart:convert';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:wren/wren.dart';

Message _message(String? kind, Uint8List payload) => Message(
      payload: payload,
      headers: kind == null ? {} : {kindHeader: kind},
    );

Uint8List _json(Object? value) => Uint8List.fromList(utf8.encode(jsonEncode(value)));

void main() {
  final intCodec = Codec.json<int>(
    toJson: (value) => value,
    fromJson: (json) => json as int,
  );

  test('dispatches to the handler registered for the kind', () async {
    var seen = 0;
    final router = Router().handle<int>('count', intCodec, (value) {
      seen = value;
      return Confirmation.ack;
    });

    final result = await router.dispatch(_message('count', _json(41)));
    expect(result, Confirmation.ack);
    expect(seen, 41);
  });

  test('falls back when no handler matches the kind', () async {
    final router = Router()
        .handle<int>('count', intCodec, (_) => Confirmation.ack)
        .fallback((_) => Confirmation.deadLetter);

    expect(
      await router.dispatch(_message('other', _json(1))),
      Confirmation.deadLetter,
    );
  });

  test('falls back when there is no kind header', () async {
    final router = Router().fallback((_) => Confirmation.reject);
    expect(await router.dispatch(_message(null, _json(1))), Confirmation.reject);
  });

  test('rejects a message whose payload fails to decode', () async {
    var handlerRan = false;
    final router = Router().handle<int>('count', intCodec, (_) {
      handlerRan = true;
      return Confirmation.ack;
    });

    final result = await router.dispatch(
      _message('count', Uint8List.fromList(utf8.encode('not-an-int'))),
    );
    expect(result, Confirmation.reject);
    expect(handlerRan, isFalse);
  });

  test('handleWith passes the raw message to the handler', () async {
    String? seenRoutingKey;
    final router = Router().handleWith<int>('count', intCodec, (value, message) {
      seenRoutingKey = message.routingKey;
      return Confirmation.ack;
    });

    await router.dispatch(Message(
      payload: _json(1),
      routingKey: 'rk',
      headers: {kindHeader: 'count'},
    ));
    expect(seenRoutingKey, 'rk');
  });
}
