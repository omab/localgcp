from cloudbox.config import settings
from cloudbox.core.store import NamespacedStore

_store = NamespacedStore("logging", data_dir=settings.data_dir)


def get_store() -> NamespacedStore:
    return _store
