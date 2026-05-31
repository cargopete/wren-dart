import 'dart:math' as math;

import 'config.dart';
import 'connection.dart';

/// A snapshot of a pool's activity.
final class PoolStats {
  const PoolStats({required this.connections, required this.channelsHandedOut});

  /// How many connections the pool holds.
  final int connections;

  /// How many channels the pool has handed out over its lifetime.
  final int channelsHandedOut;
}

/// A pool of open connections. [channel] hands out channels round-robin across
/// them, so heavy channel use is spread over several connections rather than
/// crammed onto one. Build with [Pool.start], tear down with [close].
final class Pool {
  Pool._(this._connections);

  final List<WrenConnection> _connections;
  int _next = 0;

  /// Open a pool of [size] connections (at least one).
  static Future<Pool> start(Config config, int size) async {
    final count = math.max(size, 1);
    final connections = <WrenConnection>[];
    try {
      for (var i = 0; i < count; i++) {
        connections.add(await WrenConnection.connect(config));
      }
    } on Object {
      // Roll back any connections opened so far.
      for (final connection in connections) {
        await connection.close();
      }
      rethrow;
    }
    return Pool._(connections);
  }

  /// How many connections the pool holds.
  int get size => _connections.length;

  /// A snapshot of the pool: connection count and lifetime channels handed out.
  PoolStats get stats => PoolStats(
        connections: _connections.length,
        channelsHandedOut: _next,
      );

  /// Open a fresh channel on the pool's next connection (round-robin). Close it
  /// with [WrenChannel.close] when you're done.
  Future<WrenChannel> channel() {
    final connection = _connections[_next % _connections.length];
    _next++;
    return connection.openChannel();
  }

  /// Close every connection in the pool.
  Future<void> close() async {
    for (final connection in _connections) {
      await connection.close();
    }
  }
}
