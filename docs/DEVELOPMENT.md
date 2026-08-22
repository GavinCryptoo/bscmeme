# 开发者使用指南

本文面向需要在本地安装、测试或扩展 `meme0801` 的开发者。默认使用
Paper/Shadow 和只读数据链路；不要把开发命令用于未经授权的真实交易。

## 安装

项目要求 Python 3.11 或更高版本。建议使用独立虚拟环境：

```bash
git clone https://github.com/GavinCryptoo/bscmeme.git
cd meme0801
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
```

按需安装可选依赖：

```bash
python -m pip install -e '.[bsc-wss]'
python -m pip install -e '.[bsc-live]'
```

复制环境模板后，只在本地填写需要的非提交配置：

```bash
cp .env.example .env
```

`.env`、钱包材料、API key 和运行数据库不能提交到 Git。Paper/Shadow
默认不启用钱包、签名或广播。

## 运行测试

运行完整单元和回归测试：

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_*.py' -v
```

运行语法检查：

```bash
PYTHONPATH=src python3 -m compileall -q src
```

测试数据源默认为 `fixture`。需要联网的实时探测必须显式使用
`DATA_SOURCE=binance_web3`，并遵守当前运行安全边界。

## 启动 Dashboard

Dashboard 是本地只读监控服务：

```bash
PYTHONPATH=src python3 run_dashboard.py
```

启动后访问 <http://127.0.0.1:8788/>。Dashboard 读取 runtime SQLite 和健康
快照；它不提供钱包、签名、广播或 Live 下单入口。

## 启动 Paper runner

BSC Balanced Paper 使用独立入口：

```bash
DATA_SOURCE=binance_web3 \
  PYTHONPATH=src python3 run_bsc_balanced_paper.py --env-file .env
```

通用入口也可以按模式启动：

```bash
# 只运行一次有限周期探测
DATA_SOURCE=binance_web3 PYTHONPATH=src \
  python3 run_realtime.py --chain bsc --mode paper \
  --strategy-profile balanced --once --env-file .env

# Solana Paper/Shadow 使用 run_realtime.py 的对应 --chain/--mode 参数
```

长期运行前确认目标数据库、锁目录、数据源和健康文件均属于目标运行实例。
使用 `Ctrl-C` 或 SIGTERM 优雅停止，避免强制终止造成 SQLite 状态未提交。

## 添加新数据源

数据源必须先标准化为现有领域记录，再交给同一个策略和运行时流程；不要在
策略代码里加入某个 Provider 的特殊判断。

1. 在 `src/meme_system/config/data_source.py` 的
   `SUPPORTED_DATA_SOURCES` 中登记稳定的数据源名称。
2. 在 `src/meme_system/adapters/` 新建适配器，负责网络请求、错误分类和
   标准化输出。实时信号源应提供：

   ```python
   def fetch_once(self) -> tuple[BinanceNormalizedSignal, ...]: ...
   ```

   返回的数据必须明确区分缺失值和真实的零值，不得猜测价格、流动性或市值。
3. 在 `run_realtime.py` 的 source 组装位置接入适配器，并保持策略只消费
   标准化结果。网络请求应留在 source/worker；runtime owner 负责状态推进和
   SQLite 写入。
4. 给适配器增加成功、空结果、超时、错误响应和重复数据测试，并验证数据源
   不可用时不会绕过既有 Gate 或伪造成交。
5. 更新本指南或对应接口文档，说明配置名、能力范围和已知限制。

新增数据源只解决数据获取，不自动获得 Quote、执行或 Live 能力。任何真实执行
能力仍必须单独经过安全校验、订单状态查询、receipt 和钱包余额对账。
