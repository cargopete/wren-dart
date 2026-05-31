import 'publish_options.dart';

/// A map from message `kind` to its destination `(exchange, routingKey)`.
/// `publishForKind` consults it so producers don't repeat routing at every call
/// site. Build it up with [routeKind] / [routeKindWithKey].
final class KindRouting {
  KindRouting._(this._routes);

  /// An empty kind-routing table.
  KindRouting() : _routes = {};

  final Map<String, Target> _routes;

  /// Route [kind] to [exchange], using the kind itself as the routing key.
  KindRouting routeKind(String kind, String exchange) =>
      routeKindWithKey(kind, exchange, kind);

  /// Route [kind] to [exchange] with an explicit [routingKey].
  KindRouting routeKindWithKey(String kind, String exchange, String routingKey) {
    return KindRouting._({
      ..._routes,
      kind: Target(exchange: exchange, routingKey: routingKey),
    });
  }

  /// The destination registered for [kind], or `null` if none.
  Target? destinationFor(String kind) => _routes[kind];

  /// Apply this routing table to [options] for [kind]. An exchange set
  /// explicitly on [options] wins; otherwise the table's mapping (if any) is
  /// used. A routing key still unset then defaults to the kind itself (matching
  /// bunnyhop).
  PublishOptions apply(String kind, PublishOptions options) {
    var result = options;
    if (result.exchange.isEmpty) {
      final destination = _routes[kind];
      if (destination != null) {
        result = result
            .toExchange(destination.exchange)
            .route(destination.routingKey);
      }
    }
    if (result.routingKey.isEmpty) {
      result = result.route(kind);
    }
    return result;
  }
}
