import subprocess
import sys
import unittest
from pathlib import Path


class MainEntrypointTest(unittest.TestCase):
    def test_main_runs_when_executed_as_script(self):
        project_root = Path(__file__).resolve().parents[1]

        result = subprocess.run(
            [sys.executable, "src/main.py"],
            cwd=project_root,
            input="",
            text=True,
            capture_output=True,
            timeout=5,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Stateless KV client.", result.stdout)


if __name__ == "__main__":
    unittest.main()
