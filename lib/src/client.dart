import 'config.dart';
import 'connection.dart';

/// A ready-to-use connection paired with an open channel. Saves the
/// connect-then-open-channel dance for the common case; reach for [connection]
/// to use the wider API (a second channel, liveness checks, …).
final class Client {
  Client._(this.connection, this.channel, this.config);

  /// The underlying connection.
  final WrenConnection connection;

  /// The open channel — pass it to `publish`, `declareQueue`, etc.
  final WrenChannel channel;

  /// The config the client was opened with.
  final Config config;

  /// Connect and open a channel in one step.
  static Future<Client> start(Config config) async {
    final connection = await WrenConnection.connect(config);
    final channel = await connection.openChannel();
    return Client._(connection, channel, config);
  }

  /// Close the client's channel and connection.
  Future<void> close() async {
    await channel.close();
    await connection.close();
  }
}
