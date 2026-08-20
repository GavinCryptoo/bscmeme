# meme0801 项目上下文与工作规则

## 项目范围

- 这是 Solana 与 BSC 的 Meme 策略项目；当前重点策略为 BSC Balanced Survivor。
- 常用入口：`run_realtime.py`；BSC Balanced Paper 使用 `run_bsc_balanced_paper.py`；Dashboard 入口为 `run_dashboard.py` 与 `run_bsc_balanced_live_dashboard.py`。
- BSC Balanced 状态根目录为 `data/bsc-balanced/`；Paper、Live 和执行记录必须隔离。
- 当前规则与优先级以 `CURRENT_DECISIONS.md` 为准；它与实际代码、有效配置或运行进程冲突时，后者优先，并更新 `PROJECT_STATE.md`。

## 最小工作路径

1. 先读本文件与 `PROJECT_STATE.md`。
2. 只在任务需要时读 `CURRENT_DECISIONS.md`、`ARCHITECTURE.md`、`DECISIONS.md` 及相关入口模块。
3. 涉及运行状态时，以当前 PID、有效进程环境、健康文件和短日志共同确认；不得只凭 `.env` 或 Dashboard 判断。
4. 改动前读取相关测试和调用入口；改动后运行与风险相称的测试和检查。运行时基础设施改动还须完成实际启动、故障隔离、重启恢复和 SQLite 检查后才可称为已验证。

常用命令（仅按任务显式执行）：

- Paper：`PYTHONPATH=src python3 run_bsc_balanced_paper.py --env-file .env`
- BSC Balanced Live（具备真实交易能力，未经明确授权禁止执行）：`PYTHONPATH=src python3 run_realtime.py --chain bsc --mode live --strategy-profile balanced --env-file .env`
- 测试：`PYTHONPATH=src python3 -m unittest discover -s tests`
- 只读数据库检查：`sqlite3 <runtime.db> 'PRAGMA quick_check;'`

## 交易与密钥安全

- 默认保持 Paper/Shadow；不得从 `.env` 中存在的开关推断当前正在 Live。必须先确认实际进程的有效环境。
- BSC Live 只能在用户明确授权、完整 Live 配置及所有安全校验均通过时使用独立 Live 进程、数据库、审计与锁；不得复用 Paper/Shadow 状态。
- 真实执行、签名、广播、approve、swap、钱包余额变动、启动/停止长期策略和修改 LaunchAgent 都需要明确任务授权。
- Solana Live、Solana 钱包、私钥读取、签名、广播和链上写入不属于本项目当前范围。
- 密钥、私钥、助记词、API Secret、认证会话和 `.env` 内容只能本地读取使用，不输出、不记录、不提交。Dashboard 与 Telegram 不能拥有下单或钱包控制权限。
- 交易路径必须 fail-closed：chain、余额、nonce、decimals、allowance、双向可执行报价、gas、最低到账、deadline、回执或状态对账任一关键项不可靠时，拒绝发送。

## 修改原则

- 保持任务范围最小；不要因单个 Token、旧数据或单次故障引入硬编码。
- 后台任务只获取和解析；runtime owner/main loop 是 SQLite runtime DB 的唯一写入者。Dashboard 使用独立只读连接。
- 任何可执行报价都不等于成交；真实订单必须经过提交、状态查询、receipt/余额对账和防重复处理。
- 临时诊断、回放、迁移或对账脚本结束后确认无残留子进程。

## Context Maintenance

每次任务完成后，判断本次任务是否造成有意义的项目状态变化。

- 只有新功能真正完成、重要问题解决或出现、运行版本/方式变化、最高优先级变化，或现有状态已失效时，才更新 `PROJECT_STATE.md`。
- 只有核心模块、关键数据流、接口关系、执行链路或整体架构实际变化时，才更新 `ARCHITECTURE.md`。
- 只有对未来开发仍有影响的重要技术取舍时，才更新 `DECISIONS.md`。
- 普通 bug fix、参数微调、UI 修改、重命名、格式化和小型代码调整不更新这些文件；它们不是 CHANGELOG，Git 承担代码历史。
- 更新时优先改写当前状态，删除失效信息，避免重复，并保持简短准确；不要不断追加流水账。
- 默认只读取完成当前任务所需的文件，不要在每次任务开始时全面读取项目、日志或历史文档。
