from .versioned_value import VersionedValue


class ReplicaNode:
    def __init__(
        self,
        node_id: str,
        initial_data: dict[str, VersionedValue] | None = None,
        available: bool = True,
    ) -> None:
        self.node_id = node_id
        self.available = available
        self.store = dict(initial_data or {})

    def get(self, key: str) -> VersionedValue | None:
        return self.store.get(key)

    def write(self, key: str, value: str, version: int) -> bool:
        current = self.store.get(key)
        if current is not None and version < current.version:
            return False

        self.store[key] = VersionedValue(value, version)
        return True

    def cas(
        self,
        key: str,
        expected_version: int,
        value: str,
        new_version: int,
    ) -> tuple[bool, int | None]:
        current = self.store.get(key)
        if current is None:
            return False, None
        if current.version != expected_version:
            return False, current.version

        return self.write(key, value, new_version), new_version

    def force_set(self, key: str, value: str, version: int) -> None:
        self.store[key] = VersionedValue(value, version)
