import unittest

from src.client import StatelessClient
from src.coordinator import CoordinatorStateless
from src.replica import ReplicaNode
from src.versioned_value import VersionedValue


class ReadYourWritesTest(unittest.TestCase):
    def test_client_read_after_write_does_not_return_older_replica_value(self):
        current = ReplicaNode("current")
        stale = ReplicaNode("stale", {"x": VersionedValue("old", 0)})
        coordinator = CoordinatorStateless([stale, current])
        client = StatelessClient(coordinator)

        write_response = client.set("x", "new")
        stale.force_set("x", "old", 0)
        read_response = client.getv("x")

        self.assertEqual(write_response, "OK x new VERSION 1")
        self.assertEqual(read_response, "OK x new VERSION 1")


if __name__ == "__main__":
    unittest.main()
