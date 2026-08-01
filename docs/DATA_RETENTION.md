# 数据留存与导出

事实来源：

- `data/solana/paper/runtime.db`
- `data/solana/shadow/runtime.db`
- 各自 `events.jsonl`

SQLite 使用 WAL；Paper 与 Shadow 永不共用数据库。JSONL 是追加式审计，CSV 是导出格式，Parquet 只有安装 `pyarrow` 时才生成，不会伪造文件。导出示例：

```bash
PYTHONPATH=src python3 scripts/export_runtime.py --mode paper --output data/solana/paper/archive/manual-export
```

每次导出带 `manifest.json`、行数和 SHA-256。当前代码不自动删除数据；归档、压缩和删除保留期必须由后续明确策略决定。推荐在保留前先完成 manifest 校验和恢复演练。
