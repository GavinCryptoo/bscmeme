from __future__ import annotations

import threading
import time
import unittest

from run_realtime import _AsyncReadOnlySource


class AsyncReadOnlySourceTests(unittest.TestCase):
    def test_discovery_stall_does_not_block_owner_poll(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        class Source:
            def fetch_once(self):
                entered.set()
                release.wait(timeout=1)
                return ("fresh",)

        source = _AsyncReadOnlySource(Source())
        started = time.monotonic()
        self.assertEqual(source.fetch_once(), ())
        self.assertLess(time.monotonic() - started, 0.1)
        self.assertTrue(entered.wait(timeout=0.5))
        # The owner loop remains responsive while the network worker is slow.
        self.assertEqual(source.fetch_once(), ())
        release.set()
        deadline = time.monotonic() + 0.5
        while source.stats()["inflight"] and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(source.fetch_once(), ("fresh",))

