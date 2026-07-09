import unittest

from src.coordinator import CoordinatorStateless
from src.errors import MIN_VERSION_UNAVAILABLE
from src.replica import ReplicaNode
from src.versioned_value import VersionedValue


class MinVersionContractTest(unittest.TestCase):
    def test_getv_returns_value_at_least_equal_to_requested_min_version(self):
        fresh = ReplicaNode("r1", {"x": VersionedValue("new", 5)})
        stale = ReplicaNode("r2", {"x": VersionedValue("old", 3)})
        coordinator = CoordinatorStateless([stale, fresh])

        response = coordinator.getv("x", min_version=5)

        self.assertEqual(response, "OK x new VERSION 5")

    def test_getv_reports_unavailable_when_no_reachable_replica_has_min_version(self):
        stale = ReplicaNode("r1", {"x": VersionedValue("old", 3)})
        coordinator = CoordinatorStateless([stale])

        response = coordinator.getv("x", min_version=5)

        self.assertEqual(response, f"{MIN_VERSION_UNAVAILABLE} min_version=5 best=3")


if __name__ == "__main__":
    unittest.main()
