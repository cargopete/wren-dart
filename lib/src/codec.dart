import 'dart:convert';
import 'dart:typed_data';

/// Something went wrong encoding to, or decoding from, the wire.
sealed class CodecError implements Exception {
  const CodecError(this.reason);

  final String reason;

  @override
  String toString() => '$runtimeType: $reason';
}

/// A value could not be serialised.
final class EncodeError extends CodecError {
  const EncodeError(super.reason);
}

/// A payload could not be deserialised into the expected type.
final class DecodeError extends CodecError {
  const DecodeError(super.reason);
}

/// A reversible mapping between a typed value [T] and its byte payload.
///
/// A codec is just a pair of functions, so you can supply any serialisation you
/// like. [Codec.json] builds one from `toJson` / `fromJson` callbacks;
/// [Codec.string] is the UTF-8 text codec, and [Codec.bytes] is the identity
/// codec for raw binary.
///
/// This is wren's idiomatic stand-in for bunnyhop's `Codec` trait — explicit
/// values rather than typeclass machinery.
final class Codec<T> {
  const Codec({required this.encode, required this.decode});

  /// Serialise a value to bytes. Throws [EncodeError] on failure.
  final Uint8List Function(T value) encode;

  /// Deserialise bytes into a value. Throws [DecodeError] on failure.
  final T Function(Uint8List payload) decode;

  /// A JSON codec built from `toJson` / `fromJson` callbacks.
  ///
  /// ```dart
  /// final orderCodec = Codec.json<Order>(
  ///   toJson: (o) => {'id': o.id},
  ///   fromJson: (j) => Order(id: (j as Map)['id'] as String),
  /// );
  /// ```
  static Codec<T> json<T>({
    required Object? Function(T value) toJson,
    required T Function(Object? json) fromJson,
  }) {
    return Codec<T>(
      encode: (value) {
        try {
          return Uint8List.fromList(utf8.encode(jsonEncode(toJson(value))));
        } catch (e) {
          throw EncodeError(e.toString());
        }
      },
      decode: (payload) {
        final String text;
        try {
          text = utf8.decode(payload);
        } on FormatException {
          throw const DecodeError('payload is not valid UTF-8');
        }
        try {
          return fromJson(jsonDecode(text));
        } catch (e) {
          throw DecodeError(e.toString());
        }
      },
    );
  }

  /// A UTF-8 text codec: values are [String]s, decoding fails on invalid UTF-8.
  static Codec<String> string() {
    return Codec<String>(
      encode: (value) => Uint8List.fromList(utf8.encode(value)),
      decode: (payload) {
        try {
          return utf8.decode(payload);
        } on FormatException {
          throw const DecodeError('payload is not valid UTF-8');
        }
      },
    );
  }

  /// The identity codec: payloads are passed through as raw bytes.
  static Codec<Uint8List> bytes() {
    return Codec<Uint8List>(
      encode: (value) => value,
      decode: (payload) => payload,
    );
  }
}
