# Runtime logging

Realtime runners write a small operational log to `logs/runtime.log` by
default. Set `RUNTIME_LOG_PATH` to choose another local path. The file uses a
rotating handler (10 MB per file, three backups) so a long run does not grow
without bound.

Each line has the same fields:

```text
UTC_TIMESTAMP LEVEL strategy_mode=... chain=... datasource=... {JSON event payload}
```

The JSON payload contains an `event` name and safe diagnostic fields such as
startup status, factory cursor progress, or slow result application. The
existing stdout JSON status records remain available for supervisors; this
file is the unified operational stream.

Runtime logs are intentionally ignored by Git. Only `logs/.gitkeep` is
tracked to keep the directory present in a fresh checkout. Do not put API
keys, secrets, wallet material, authentication sessions, or full credentials
in log fields. Runtime audit JSONL files and SQLite state remain separate and
are the source of truth for transactions and accounting.
