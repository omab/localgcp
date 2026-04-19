"""Thread-safe in-memory key-value store with optional JSON persistence.

Each service creates its own NamespacedStore instance. The store is a
dict-of-dicts: namespace → key → value, where values are plain dicts
that can be serialized to JSON.
"""

import json
import threading
from pathlib import Path
from typing import Any


class NamespacedStore:
    """A thread-safe namespaced key-value store.

    Usage:
        store = NamespacedStore("gcs", data_dir="/tmp/cloudbox")
        store.set("buckets", "my-bucket", {"name": "my-bucket", ...})
        bucket = store.get("buckets", "my-bucket")
        store.delete("buckets", "my-bucket")
        all_buckets = store.list("buckets")
    """

    def __init__(self, name: str, data_dir: str | None = None):
        self._name = name
        self._data_dir = Path(data_dir) / name if data_dir else None
        self._lock = threading.RLock()
        self._data: dict[str, dict[str, Any]] = {}

        if self._data_dir:
            self._data_dir.mkdir(parents=True, exist_ok=True)
            self._load()

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def get(self, namespace: str, key: str) -> Any | None:
        with self._lock:
            return self._data.get(namespace, {}).get(key)

    def set(self, namespace: str, key: str, value: Any) -> None:
        with self._lock:
            self._data.setdefault(namespace, {})[key] = value
            self._persist()

    def delete(self, namespace: str, key: str) -> bool:
        with self._lock:
            ns = self._data.get(namespace, {})
            if key not in ns:
                return False
            del ns[key]
            self._persist()
            return True

    def exists(self, namespace: str, key: str) -> bool:
        with self._lock:
            return key in self._data.get(namespace, {})

    def list(self, namespace: str) -> list[Any]:
        with self._lock:
            return list(self._data.get(namespace, {}).values())

    def keys(self, namespace: str) -> list[str]:
        with self._lock:
            return list(self._data.get(namespace, {}).keys())

    def clear_namespace(self, namespace: str) -> None:
        with self._lock:
            self._data.pop(namespace, None)
            self._persist()

    def reset(self) -> None:
        """Wipe all data (used by admin UI and tests)."""
        with self._lock:
            self._data.clear()
            self._persist()

    def stats(self) -> dict[str, int]:
        """Return count per namespace."""
        with self._lock:
            return {ns: len(keys) for ns, keys in self._data.items()}

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def _persist(self) -> None:
        if not self._data_dir:
            return
        path = self._data_dir / "data.json"
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._data, default=str), encoding="utf-8")
        tmp.replace(path)

    def _load(self) -> None:
        path = self._data_dir / "data.json"
        if path.exists():
            self._data = json.loads(path.read_text(encoding="utf-8"))
