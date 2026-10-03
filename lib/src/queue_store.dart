import 'queued_operation.dart';

/// Persistence for the queue. Implement it on top of your database (Drift,
/// Isar, Hive, SharedPreferences…) — ideally writing the operation in the
/// same transaction as the local change it mirrors.
abstract interface class QueueStore {
  /// All operations, in any order.
  Future<List<QueuedOperation>> loadAll();

  /// Inserts or replaces [operation] by id.
  Future<void> save(QueuedOperation operation);

  /// Removes the operation with [id], if present.
  Future<void> delete(String id);
}

/// Keeps operations in memory. Useful for tests and prototypes; nothing
/// survives a restart.
class InMemoryQueueStore implements QueueStore {
  final _operations = <String, QueuedOperation>{};

  @override
  Future<List<QueuedOperation>> loadAll() async => _operations.values.toList();

  @override
  Future<void> save(QueuedOperation operation) async =>
      _operations[operation.id] = operation;

  @override
  Future<void> delete(String id) async => _operations.remove(id);
}
