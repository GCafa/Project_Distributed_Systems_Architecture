import subprocess
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def run_main(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "main.py", *args],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        timeout=5,
    )


class MainEntrypointTest(unittest.TestCase):
    def test_help_lists_execution_commands(self) -> None:
        result = run_main("--help")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("cluster", result.stdout)
        self.assertIn("client", result.stdout)
        self.assertIn("test", result.stdout)

    def test_cluster_dry_run_prints_stateless_processes(self) -> None:
        result = run_main(
            "cluster",
            "--mode",
            "stateless",
            "--coordinator-port",
            "6520",
            "--replica-base-port",
            "6521",
            "--dry-run",
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("replica_node.py", result.stdout)
        self.assertIn("--node-id R0", result.stdout)
        self.assertIn("--port 6521", result.stdout)
        self.assertIn("coordinator_stateless.py", result.stdout)
        self.assertIn("--port 6520", result.stdout)
        self.assertIn("client.py", result.stdout)
        self.assertIn("--mode stateless", result.stdout)


if __name__ == "__main__":
    unittest.main()
