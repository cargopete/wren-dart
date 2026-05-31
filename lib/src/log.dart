import 'dart:io';

/// Emit a warning. wren is quiet by default; warnings go to stderr with a
/// `wren:` prefix so they're easy to spot and filter.
void logWarning(String message) {
  stderr.writeln(message);
}
