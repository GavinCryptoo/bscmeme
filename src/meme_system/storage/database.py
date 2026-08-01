"""SQLite WAL initialization and versioned migration runner."""

from __future__ import annotations

import sqlite3
from pathlib import Path


SCHEMA_VERSION = 3

MIGRATIONS = (
    (1, "001_initial.sql"),
    (2, "002_lifecycle_recovery.sql"),
    (3, "003_runtime_observability.sql"),
)


def initialize_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=5000")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations "
        "(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
    )
    migration_dir = Path(__file__).with_name("migrations")
    # Serialize migration discovery and application across Dashboard threads,
    # runners, and separate processes. executescript() implicitly commits and
    # can race on ALTER TABLE, so execute each simple migration statement in
    # one exclusive transaction instead.
    connection.execute("BEGIN EXCLUSIVE")
    try:
        applied = {
            row[0]
            for row in connection.execute("SELECT version FROM schema_migrations")
        }
        for version, filename in MIGRATIONS:
            if version in applied:
                continue
            script = (migration_dir / filename).read_text(encoding="utf-8")
            for statement in script.split(";"):
                statement = statement.strip()
                if statement:
                    connection.execute(statement)
            connection.execute(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (?, datetime('now'))",
                (version,),
            )
        connection.commit()
    except Exception:
        connection.rollback()
        connection.close()
        raise
    return connection
