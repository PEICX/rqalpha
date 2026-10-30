# 本地回测使用说明（stockdb 数据源）

本仓库在 RQAlpha 6.5.0 基础上接入了本地 `stockdb` 数据库作为行情数据源，
不需要下载 HDF5 bundle。本文档说明在这台机器上怎么跑回测、怎么用本地数据，
以及容易踩的坑。**新接手的人先看第 1 节和第 5 节。**

---

## 1. 快速开始

### 1.1 前提

- 解释器是 `python3`，没有 `python` 命令。
- `rqalpha` **没有** `pip install` 到环境里，能 import 只是因为当前目录是仓库根目录。
  所以回测命令必须在下面这个目录执行：

  ```bash
  cd /root/pcx/rqalpha-release-6.5.0
  ```

  如果实在要在别的目录跑，需要设：

  ```bash
  export PYTHONPATH=/root/pcx/rqalpha-release-6.5.0
  ```

- 本地 stockdb 服务监听 `127.0.0.1:80`，服务目录 `/root/pcx/stockdb`。

### 1.2 日线回测

> **注意**：用户级配置 `/root/.rqalpha/mod_config.yml` 的默认数据源**已切换为
> quantdb**（见 `BACKTEST_QUANTDB.md`）。要用 stockdb 跑命令行回测，
> 先把用户级配置改回 stockdb（quantdb.enabled 改 false、stockdb.enabled 改 true），
> 或者每次命令行显式指定：

```bash
cd /root/pcx/rqalpha-release-6.5.0

python3 -m rqalpha run \
  -f strategy.py \
  -s 2024-01-02 -e 2024-12-31 \
  -a stock 100000 \
  -fq 1d \
  -o /tmp/out.pkl \
  -mc stockdb.enabled True \
  -mc stockdb.lib rqalpha.mod.rqalpha_mod_stockdb \
  -mc stockdb.server_dir /root/pcx/stockdb \
  -mc quantdb.enabled False
```

### 1.3 分钟线回测

分钟数据从 **2025-01-02** 开始；起始日早于这天会被自动裁到 2025-01-02。

```bash
python3 -m rqalpha run \
  -f strategy.py \
  -s 2025-01-02 -e 2025-01-31 \
  -a stock 100000 \
  -fq 1m \
  -o /tmp/out.pkl
```

### 1.4 直接跑仓库自带示例

```bash
python3 stockdb_examples/01_daily_dual_ma.py
python3 stockdb_examples/02_minute_ma.py
python3 stockdb_examples/03_api_showcase.py
python3 stockdb_examples/04_dividend_and_limit_check.py
```

这些示例内部调用的是 `rqalpha.run_code`，并且自己写了完整配置，
不依赖 `~/.rqalpha/mod_config.yml`。

### 1.5 读取回测结果

`-o out.pkl` 输出的是一个 dict，常用键：

```python
import pickle

r = pickle.load(open("/tmp/out.pkl", "rb"))
print(r["summary"])          # 收益、年化、回撤、sharpe 等
print(len(r["trades"]))      # 成交笔数
print(r["portfolio"])        # 每日净值/现金等
```

注意：这个版本的 `summary["total_trades"]` 是 `None`，要看成交笔数请用
`len(r["trades"])`。

---

## 2. 本地数据怎么接入的

`rqalpha/mod/rqalpha_mod_stockdb/` 是一个 RQAlpha mod，启动时把
`StockdbDataSource` 注册为数据源，替代默认的 HDF5 bundle。

数据能力：

| 能力 | 说明 |
|---|---|
| A 股日 K | 2000-01-04 至今，含 ETF；支持前复权/后复权/不复权 |
| 分钟 K（1m） | 2025-01-02 至今 |
| 交易日历 | 锚定股票日 K 并集，2000-01-04 至今 |
| 上市/退市日期 | 用首末 K 线 + `退市` 表推导 |
| 分红/送转/复权因子 | 来自 `复权` 表 |
| 停牌/ST | 日 K 缺失行 + `is_st` 字段 |
| 基准指数 | **没有指数行情**，用 ETF 代理（见下） |

默认的指数 → ETF 映射（`rqalpha/mod/rqalpha_mod_stockdb/universe.py`）：

| 指数 | ETF |
|---|---|
| 000300.XSHG | 510300.XSHG |
| 000905.XSHG | 510500.XSHG |
| 000016.XSHG | 510050.XSHG |
| 000852.XSHG | 512100.XSHG |
| 399006.XSHE | 159915.XSHE |

---

## 3. 配置方式与优先级

配置从低到高：

```
仓库默认 rqalpha/mod_config.yml
  < 用户级 ~/.rqalpha/mod_config.yml
  < 当前目录 config.yml / mod_config.yml
  < 策略文件里的 __config__
  < 命令行参数
```

当前用户级配置**默认启用的是 quantdb**（stockdb 段保留但 `enabled: false`，
完整内容见 `BACKTEST_QUANTDB.md` 1.2）。stockdb 与 quantdb 互斥，
**两者只能启用一个**。

如果要用 stockdb 作为默认数据源，把用户级配置改成：

```yaml
mod:
  stockdb:
    enabled: true
    lib: rqalpha.mod.rqalpha_mod_stockdb
    server_dir: /root/pcx/stockdb

  quantdb:
    enabled: false
```

或者不改默认配置，每次命令行显式指定（记得同时关掉 quantdb）：

```bash
python3 -m rqalpha run -f strategy.py -s 2024-01-02 -e 2024-12-31 \
  -a stock 100000 \
  -mc stockdb.enabled True \
  -mc stockdb.lib rqalpha.mod.rqalpha_mod_stockdb \
  -mc stockdb.server_dir /root/pcx/stockdb \
  -mc quantdb.enabled False
```

`-mc stockdb.lib ...` 不能省。`stockdb` 不是 `sys_*` 内置 mod，不给 `lib`
时会去找顶层的 `rqalpha_mod_stockdb` 包，而它实际在 `rqalpha.mod` 下面。

**方式 C：写在策略的 `__config__` 里**

```python
__config__ = {
    "base": {
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "frequency": "1d",
        "accounts": {"stock": 100000},
    },
    "mod": {
        "stockdb": {
            "enabled": True,
            "lib": "rqalpha.mod.rqalpha_mod_stockdb",
            "server_dir": "/root/pcx/stockdb",
        },
        # 用户级配置默认启用了 quantdb，这里显式关闭避免两个数据源互相覆盖
        "quantdb": {"enabled": False},
        "sys_analyser": {"enabled": True},
    },
}
```

---

## 4. 策略最小模板

```python
# strategy.py
def init(context):
    context.s = "000001.XSHE"
    context.n = 0


def handle_bar(context, bar_dict):
    context.n += 1
    if context.n == 3:
        order_target_percent(context.s, 0.9)
```

股票代码必须带交易所后缀，比如 `000001.XSHE`、`600000.XSHG`。
日期统一用 `YYYY-MM-DD`。

策略里可用 mod 额外注入的 `sb_*` API：
`sb_get_bars`、`sb_valuation`、`sb_money_flow`、`sb_boards`、
`sb_stock_boards`、`sb_board_stocks`、`sb_indicator`。
用法见 `rqalpha/mod/rqalpha_mod_stockdb/README.md` 和
`stockdb_examples/03_api_showcase.py`。

---

## 5. 注意事项（重点）

### 5.1 不要设置基准

**不要**在 `sys_analyser` 里写 `"benchmark": "000300.XSHG"`，也**不要**传 `-bm`。

原因有两个：

1. stockdb 没有指数行情，指数会被映射成 ETF。很多 ETF（如 510300）2024 年
   才有数据。
2. `sys_analyser` 一旦设置了 benchmark，会注册
   `AVAILABLE_DATA_RANGE` hook，把回测区间改成：

   ```
   start = max(请求起点, 基准第二根日线的日期)
   end   = min(请求终点, 基准最后一根日线的日期)
   ```

   结果是起始日被推迟到 2024 年；如果请求区间整体早于 ETF 上市日，还会直接报错：

   ```
   CustomException: benchmark 000300.XSHG missing data between backtest start
   date 2020-01-02 and end date 2020-02-03
   ```

去掉基准后，`benchmark_total_returns`、`alpha`、`beta`、`information_ratio`
这些相对指标会变成 `None`/`NaN`，绝对收益、年化、最大回撤、Sharpe 仍然正常。

### 5.2 只支持 1d 和 1m

- `1d`：日线。
- `1m`：分钟线，数据从 2025-01-02 开始。
- `5m/15m/30m/60m` 没有实现，`tick` 也没有。

### 5.3 数据缺失与区间裁剪

回测起止日会被限制在“数据实际可用范围”内。请求 1999 年会被裁到 2000-01-04；
分钟回测请求 2024 年会被裁到 2025-01-02。看到 `start_date` 和传入值不一致时，
先查数据范围。

### 5.4 首次运行慢是正常的

首次运行会拉取全市场代码的首末 K 线日期（约 1–2 分钟），之后缓存到
`~/.rqalpha/stockdb_cache/`，后续会快很多。

批量或长区间回测前建议先预热：

```bash
cd /root/pcx/rqalpha-release-6.5.0

# 全市场日 K（约 2.5GB）
PYTHONPATH=/root/pcx/rqalpha-release-6.5.0 \
  python3 -m rqalpha.mod.rqalpha_mod_stockdb.prewarm --day --stocks --etf

# 指定标的的分钟 K（约 20MB/只）
PYTHONPATH=/root/pcx/rqalpha-release-6.5.0 \
  python3 -m rqalpha.mod.rqalpha_mod_stockdb.prewarm --minute --codes 510300,000001

# 指定分钟数据起始日
PYTHONPATH=/root/pcx/rqalpha-release-6.5.0 \
  python3 -m rqalpha.mod.rqalpha_mod_stockdb.prewarm \
    --minute --codes 510300 --start 2025-06-01
```

### 5.5 并发

- 机器配置 2026-10-30 起调整为 limit=56核，100 并发批量回测已验证可行，推荐并发上限 100。
- 热缓存下回测是 CPU 密集、每进程单线程。
- 冷缓存时瓶颈是 stockdb 服务的串行查询，多进程不会加速，**先预热**。
- 日 K/分钟 K 磁盘缓存用 mmap 加载，多进程共享物理内存页，内存不会按进程数翻倍。

### 5.6 stockdb 没有的数据

以下数据只在在线接口提供，本 mod 没有集成，策略里不要依赖：

- 财务报表、财务快报、业绩预告
- 因子值
- 指数成分、指数行情
- 龙虎榜、融资融券

### 5.7 涨跌停价是推导出来的

stockdb 的 `pre_close` 字段基准不一致，mod 一律用前一日收盘价加除权事件自行推导
基准价，再按板块规则计算涨跌停：

- 主板 10%
- 创业板/科创板 20%
- 北交所 30%
- ST 5%（科创/创业 ST 仍按 20%）

如果策略依赖精确的涨跌停价，需要自己再核对。

### 5.8 一条无害的警告

每次运行都会看到：

```
WARN: The strategy requires explicit configuration of base.capital_gain_tax_rate ...
```

这是 RQAlpha 的提示，本地回测可以忽略。

---

## 6. 常见报错

| 报错 | 原因 / 处理 |
|---|---|
| `ModuleNotFoundError: No module named 'rqalpha'` | 不在仓库根目录。`cd /root/pcx/rqalpha-release-6.5.0` 或设 `PYTHONPATH` |
| `benchmark ... missing data between ...` | 设了基准但对应 ETF 在该区间没数据，去掉 benchmark / `-bm` |
| `There is no data between ... and ...` | 请求区间超出数据范围，检查起止日（分钟数据 ≥ 2025-01-02） |
| stockdb 查询超时 | 配置了 `server_dir` 会自动重启服务并重试；没配置就补上 |
| `stockdb: failed to build trading calendar` | stockdb 服务不可用，检查 `127.0.0.1:80` 是否在跑 |

---

## 7. 相关文件

- `rqalpha/mod/rqalpha_mod_stockdb/README.md` — mod 的数据能力、API、性能说明
- `stockdb_examples/` — 4 个可直接运行的策略
- `CLAUDE.md` — 仓库开发约定
- `docs/source/intro/tutorial.rst` — RQAlpha 策略教程
- `docs/source/api/base_api.rst` — RQAlpha API 手册
