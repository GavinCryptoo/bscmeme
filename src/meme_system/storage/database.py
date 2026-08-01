"""SQLite WAL initialization and versioned migration runner."""

from __future__ import annotations

import sqlite3
from pathlib import Path


SCHEMA_VERSION = 2

MIGRATIONS = (
    (1, "001_initial.sql"),
    (2, "002_lifecycle_recovery.sql"),
)


def initialize_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations "
        "(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
    )
    applied = {
        row[0]
        for row in connection.execute("SELECT version FROM schema_migrations")
    }
    migration_dir = Path(__file__).with_name("migrations")
    for version, filename in MIGRATIONS:
        if version in applied:
            continue
        connection.executescript(
            (migration_dir / filename).read_text(encoding="utf-8")
        )
        connection.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (?, datetime('now'))",
            (version,),
        )
    connection.commit()
    return connection
