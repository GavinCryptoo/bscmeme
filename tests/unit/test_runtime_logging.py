import tempfile
import unittest
from pathlib import Path

from meme_system.runtime_logging import configure_runtime_logging, shutdown_runtime_logging


class RuntimeLoggingTests(unittest.TestCase):
    def tearDown(self) -> None:
        shutdown_runtime_logging()

    def test_runtime_event_has_stable_context_and_json_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "runtime.log"
            logger = configure_runtime_logging(
                path,
                strategy_mode="paper",
                chain="bsc",
                datasource="fixture",
            )
            logger.event("runtime_starting", pid=123)
            shutdown_runtime_logging()

            line = path.read_text(encoding="utf-8").strip()
            self.assertRegex(line, r"^20\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z INFO ")
            self.assertIn("strategy_mode=paper", line)
            self.assertIn("chain=bsc", line)
            self.assertIn("datasource=fixture", line)
            self.assertIn('"event": "runtime_starting"', line)
            self.assertIn('"pid": 123', line)


if __name__ == "__main__":
    unittest.main()
