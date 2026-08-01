from __future__ import annotations

import unittest

from meme_system.config.dashboard import DashboardConfig


class DashboardConfigTests(unittest.TestCase):
    def test_default_is_local_read_only_port_8788(self) -> None:
        config = DashboardConfig()
        config.validate()
        self.assertEqual(config.host, "127.0.0.1")
        self.assertEqual(config.port, 8788)
        self.assertTrue(config.read_only)

    def test_non_local_binding_requires_authentication(self) -> None:
        with self.assertRaises(ValueError):
            DashboardConfig(host="0.0.0.0").validate()

