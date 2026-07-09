from .errors import (
    BAD_REQUEST,
    NOT_FOUND,
    err_cas_failed,
    err_min_version_unavailable,
    err_read_quorum_not_reached,
    err_write_quorum_not_reached,
)
from .replica import ReplicaNode
from .versioned_value import VersionedValue


class CoordinatorStateless:
    def __init__(
        self,
        replicas: list[ReplicaNode],
        read_quorum: int = 1,
        write_quorum: int | None = None,
    ) -> None:
        if not replicas:
            raise ValueError("at least one replica is required")
        if read_quorum < 1:
            raise ValueError("read_quorum must be at least 1")

        self.replicas = list(replicas)
        self.read_quorum = read_quorum
        self.write_quorum = write_quorum or (len(self.replicas) // 2 + 1)
        self._versions = self._initial_versions()

    def get(self, key: str) -> str:
        values = self._reachable_values(key)
        if not values:
            return NOT_FOUND

        best = self._best(values)
        return f"OK {key} {best.value}"

    def getv(self, key: str, min_version: int = 0) -> str:
        if min_version < 0:
            return BAD_REQUEST

        reachable = self._reachable_replicas()
        if len(reachable) < self.read_quorum:
            return err_read_quorum_not_reached(len(reachable))

        values = [replica.get(key) for replica in reachable]
        existing_values = [value for value in values if value is not None]
        if not existing_values:
            return NOT_FOUND

        best = self._best(existing_values)
        if best.version < min_version:
            return err_min_version_unavailable(min_version, best.version)

        return f"OK {key} {best.value} VERSION {best.version}"

    def set(self, key: str, value: str) -> str:
        version = self._next_version(key)
        acks = self._write_to_reachable(key, value, version)
        if acks < self.write_quorum:
            return err_write_quorum_not_reached(acks)

        return f"OK {key} {value} VERSION {version}"

    def cas(self, key: str, expected_version: int, value: str) -> str:
        if expected_version < 0:
            return BAD_REQUEST

        values = self._reachable_values(key)
        if not values:
            return NOT_FOUND

        current = self._best(values)
        if current.version != expected_version:
            return err_cas_failed(current.version)

        version = self._next_version(key)
        acks = self._write_to_reachable(key, value, version)
        if acks < self.write_quorum:
            return err_write_quorum_not_reached(acks)

        return f"OK {key} {value} VERSION {version}"

    def _reachable_replicas(self) -> list[ReplicaNode]:
        return [replica for replica in self.replicas if replica.available]

    def _reachable_values(self, key: str) -> list[VersionedValue]:
        return [
            value
            for replica in self._reachable_replicas()
            if (value := replica.get(key)) is not None
        ]

    def _write_to_reachable(self, key: str, value: str, version: int) -> int:
        acks = 0
        for replica in self._reachable_replicas():
            if replica.write(key, value, version):
                acks += 1
        return acks

    def _next_version(self, key: str) -> int:
        version = self._versions.get(key, 0) + 1
        self._versions[key] = version
        return version

    def _initial_versions(self) -> dict[str, int]:
        versions: dict[str, int] = {}
        for replica in self.replicas:
            for key, versioned_value in replica.store.items():
                versions[key] = max(versions.get(key, 0), versioned_value.version)
        return versions

    @staticmethod
    def _best(values: list[VersionedValue]) -> VersionedValue:
        return max(values, key=lambda value: value.version)
