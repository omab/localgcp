"""Cloud Tasks in-memory store.

Namespaces:
  queues  → queue_name  → QueueModel dict
  tasks   → task_name   → TaskModel dict
"""

from cloudbox.config import settings
from cloudbox.core.store import NamespacedStore

_store = NamespacedStore("tasks", settings.data_dir)


def get_store() -> NamespacedStore:
    return _store
