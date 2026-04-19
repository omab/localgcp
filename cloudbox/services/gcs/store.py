"""GCS in-memory store.

Stores:
  buckets  → bucket_name → BucketModel dict
  objects  → "{bucket}/{object}" → ObjectModel dict
  bodies   → "{bucket}/{object}" → bytes
"""

from cloudbox.config import settings
from cloudbox.core.store import NamespacedStore

_store = NamespacedStore("gcs", settings.data_dir)


def get_store() -> NamespacedStore:
    return _store
