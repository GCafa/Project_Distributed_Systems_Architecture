import unittest

from src.client import StatelessClient
from src.coordinator import CoordinatorStateless
from src.replica import ReplicaNode
from src.versioned_value import VersionedValue


class StatelessClientSessionTest(unittest.TestCase):
    def test_client_sends_min_version_learned_from_previous_read(self):
        fresh = ReplicaNode("fresh", {"x": VersionedValue("new", 5)})
        stale = ReplicaNode("stale", {"x": VersionedValue("old", 3)})
        coordinator = CoordinatorStateless([fresh, stale])
        client = StatelessClient(coordinator)

        first_response = client.getv("x")
        fresh.available = False
        second_response = client.getv("x")

        self.assertEqual(first_response, "OK x new VERSION 5")
        self.assertEqual(client.min_versions["x"], 5)
        self.assertEqual(second_response, "ERR min_version_unavailable min_version=5 best=3")

    def test_client_updates_min_version_after_set(self):
        replica = ReplicaNode("r1")
        coordinator = CoordinatorStateless([replica])
        client = StatelessClient(coordinator)

        response = client.set("x", "written")

        self.assertEqual(response, "OK x written VERSION 1")
        self.assertEqual(client.min_versions["x"], 1)

    def test_client_updates_min_version_after_successful_cas(self):
        replica = ReplicaNode("r1", {"x": VersionedValue("old", 2)})
        coordinator = CoordinatorStateless([replica])
        client = StatelessClient(coordinator)

        response = client.cas("x", expected_version=2, value="new")

        self.assertEqual(response, "OK x new VERSION 3")
        self.assertEqual(client.min_versions["x"], 3)


if __name__ == "__main__":
    unittest.main()
