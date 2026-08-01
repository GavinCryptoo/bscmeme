from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from meme_system.config.runtime import RuntimePaths
from meme_system.storage.database import initialize_database


class StorageTests(unittest.TestCase):
    def test_database_uses_wal_and_schema_migration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "runtime.db")
            journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            self.assertEqual(journal_mode.lower(), "wal")
            self.assertIn("schema_migrations", tables)
            self.assertIn("signals", tables)
            self.assertIn("virtual_positions", tables)
            self.assertIn("lifecycle_events", tables)
            versions = [
                row[0]
                for row in connection.execute(
                    "SELECT version FROM schema_migrations ORDER BY version"
                )
            ]
            self.assertEqual(versions, [1, 2, 3])
            connection.close()

    def test_paper_and_shadow_paths_are_distinct(self) -> None:
        RuntimePaths().validate_isolation()
