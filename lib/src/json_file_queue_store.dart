import 'dart:convert';
import 'dart:io';

import 'queue_store.dart';
import 'queued_operation.dart';

/// Persists the queue as a JSON file, rewritten atomically on every change
/// (write to a temp file, then rename).
///
/// Suitable for small queues on `dart:io` platforms. For large queues or
/// transactional guarantees with your app data, implement [QueueStore] on
/// your database instead.
class JsonFileQueueStore implements QueueStore {
  /// Creates a store backed by [file].
  JsonFileQueueStore(this.file);

  /// The file holding the queue.
  final File file;

  Map<String, QueuedOperation>? _cache;
  Future<void> _pending = Future.value();

  Future<Map<String, QueuedOperation>> _read() async {
    if (_cache != null) return _cache!;
    if (!await file.exists()) return _cache = {};
    final list = jsonDecode(await file.readAsString()) as List<Object?>;
    return _cache = {
      for (final json in list.cast<Map<String, Object?>>())
        json['id']! as String: QueuedOperation.fromJson(json),
    };
  }

  // Serializes writes so concurrent saves cannot interleave.
  Future<void> _mutate(void Function(Map<String, QueuedOperation>) change) {
    return _pending = _pending.then((_) async {
      final ops = await _read();
      change(ops);
      final tmp = File('${file.path}.tmp');
      await tmp.parent.create(recursive: true);
      await tmp.writeAsString(
          jsonEncode([for (final op in ops.values) op.toJson()]),
          flush: true);
      await tmp.rename(file.path);
    });
  }

  @override
  Future<List<QueuedOperation>> loadAll() async {
    await _pending;
    return (await _read()).values.toList();
  }

  @override
  Future<void> save(QueuedOperation operation) =>
      _mutate((ops) => ops[operation.id] = operation);

  @override
  Future<void> delete(String id) => _mutate((ops) => ops.remove(id));
}
