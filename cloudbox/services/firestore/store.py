"""Firestore in-memory store.

Documents are keyed by their full resource name, e.g.:
  projects/local-project/databases/(default)/documents/users/alice

The store namespace "documents" maps full doc path → Document dict.
"""

from cloudbox.config import settings
from cloudbox.core.store import NamespacedStore

_store = NamespacedStore("firestore", settings.data_dir)


def get_store() -> NamespacedStore:
    return _store
