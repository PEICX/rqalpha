# 本地回测使用说明（quantdb 数据源）

本仓库在 RQAlpha 6.5.0 基础上接入了本地 `quantdb`（DuckDB catalog + Parquet）作为
行情数据源，不需要下载 HDF5 bundle。相比 stockdb 数据源，**benchmark 用的是真实指数
行情，并且有历史 ST 与因子/财务等扩展数据**。

快速切换、常见坑见本文；mod 的完整能力与限制见
`rqalpha/mod/rqalpha_mod_quantdb/README.md`。

---

## 1. 快速开始

### 1.1 前提

- 解释器是 `python3`（无 `python` 命令）。
- `rqalpha` 没有 pip install，必须在仓库根目录执行，或设 `PYTHONPATH`：

  ```bash
  cd /root/pcx/rqalpha-release-6.5.0
  export PYTHONPATH=/root/pcx/rqalpha-release-6.5.0
  ```

- Python 依赖必须齐全。**本机曾缺失以下包导致 `import rqalpha` 直接失败**，
  已用 pip 补装，换环境时需要重装：

  ```bash
  pip3 install h5py logbook rqrisk matplotlib msgpack TA-Lib click \
      python-dateutil filelock jsonpickle methodtools simplejson pyyaml \
      typing_extensions requests duckdb pandas pyarrow numpy
  ```

- quantdb catalog 在 `/root/pcx/quantdb/catalog.duckdb`（只读打开，多进程安全）。

### 1.2 切换数据源（二选一）

**方式 A：用户级配置**（`~/.rqalpha/mod_config.yml`）。

**当前默认已经启用 quantdb**（stockdb 段保留但 `enabled: false`），内容如下：

```yaml
mod:
  quantdb:
    enabled: true
    lib: rqalpha.mod.rqalpha_mod_quantdb
    catalog_path: /root/pcx/quantdb/catalog.duckdb
    cache_dir: ~/.rqalpha/quantdb_cache
    risk_free_rate: 0.02
    threads: 2

  stockdb:
    enabled: false
    lib: rqalpha.mod.rqalpha_mod_stockdb
    server_dir: /root/pcx/stockdb
```

**两个数据源都会 set_data_source，同时启用会互相覆盖，只能启用一个**；
切换时把一边的 `enabled` 改 true、另一边改 false 即可。

如果换一台机器或用户级配置丢了，按上面内容重建（`mkdir -p ~/.rqalpha` 后写入）。

仓库自带的示例（`quantdb_examples/`、`stockdb_examples/`）都在自己的
`__config__` 里显式启用了数据源并禁用另一个，不依赖用户级配置，随时可跑。

**方式 B：命令行显式指定**（临时覆盖，不影响用户级配置）：

```bash
python3 -m rqalpha run -f strategy.py -s 2024-01-02 -e 2024-12-31 \
  -a stock 100000 -fq 1d \
  -mc quantdb.enabled True \
  -mc quantdb.lib rqalpha.mod.rqalpha_mod_quantdb \
  -mc quantdb.catalog_path /root/pcx/quantdb/catalog.duckdb
```

`-mc quantdb.lib ...` 不能省：`quantdb` 不是 `sys_*` 内置 mod，不给 `lib`
会去找顶层的 `rqalpha_mod_quantdb` 包，而它实际在 `rqalpha.mod` 下面。

### 1.3 日线回测（可用真实指数基准）

```bash
cd /root/pcx/rqalpha-release-6.5.0

python3 -m rqalpha run \
  -f strategy.py \
  -s 2024-01-02 -e 2024-12-31 \
  -a stock 100000 \
  -fq 1d \
  -bm 000300.XSHG \
  -o /tmp/out.pkl
```

- 数据范围：**2016-01-04 ~ 2026-09-30**，请求区间会被自动裁剪。
- `-bm 000300.XSHG` 来自 `v_index_daily` 的真实指数行情（46 个指数可选），
  `alpha/beta/benchmark_total_returns` 正常输出。这是与 stockdb 的关键差异：
  stockdb 无指数行情，设置基准会报错或把起始日推迟。

### 1.4 分钟线回测

```bash
python3 -m rqalpha run \
  -f strategy.py \
  -s 2026-08-03 -e 2026-08-06 \
  -a stock 100000 \
  -fq 1m \
  -o /tmp/out.pkl
```

- 分钟开放范围：**2016-01-04 ~ 2026-08-06**（2026-08-07 只有 52 根，自动截断；
  可用 `quantdb.minute_end` 配置覆盖）。
- 每个交易日 240 根 bar（09:31~11:30、13:01~15:00），09:30 竞价 bar 不进
  `history_bars`，但会用于 `get_open_auction_bar`（竞价量是真实的，不是全天量）。

### 1.5 跑仓库自带示例

```bash
python3 quantdb_examples/01_daily_dual_ma.py          # 日线双均线 + 指数基准
python3 quantdb_examples/02_minute_ma.py              # 分钟双均线
python3 quantdb_examples/03_adjust_dividend_check.py  # 复权/分红入账校验
python3 quantdb_examples/04_api_showcase.py           # qd_* 扩展 API 演示
```

示例内部用 `run_code` + 完整 `__config__`（含 `stockdb.enabled=False`），
不依赖 `~/.rqalpha/mod_config.yml`，随时可跑。

### 1.6 读取回测结果

```python
import pickle
r = pickle.load(open("/tmp/out.pkl", "rb"))["sys_analyser"]
print(r["summary"])        # 收益、年化、回撤、sharpe、alpha/beta
print(len(r["trades"]))    # 成交笔数
```

注意：`run_code`/`-o` 的顶层 key 是 `sys_analyser`；`summary["total_trades"]`
为 `None`，请用 `len(r["trades"])`。

---

## 2. 策略里的扩展 API（qd_*）

启用 quantdb mod 后自动注入，全部走本地 DuckDB（按 `dt` 分区裁剪，务必传起止日期）：

| API | 数据 | 说明 |
|---|---|---|
| `qd_bars(obid, start, end, adj='qfq'/'hfq'/'none')` | `v_daily_*` | 前复权锚定数据集最新交易日 |
| `qd_factors(obids, start, end, level='l1'/'l2'/'l1l2')` | `v_l1/l2/l1_l2_factors` | l1 从 2016 起，l1l2 从 2018 起 |
| `qd_financial(table, obids, as_of, latest)` | `v_financial_*` + 分红 | `as_of` 按公告日防未来函数 |
| `qd_valuation` / `qd_sentiment` / `qd_technical` | 估值/情绪/技术 | `future_return_*` 是标签，别当特征 |
| `qd_margin(obids, start, end)` | `v_margin` | 融资融券，2016 起 |
| `qd_sectors(obid=None, sector_code=None)` | `v_sectors` | 板块快照 |
| `qd_index_weights(index_obid)` | `v_index_weights` | 指数权重快照 |

代码既接受 `600519.XSHG`（order_book_id）也接受 `600519.SH`（quantdb symbol）。
**不要在 handle_bar 里对 5,000 只票逐只查询**；在 `init` 或每日盘前批量加载。

---

## 3. 缓存与预热

冷启动首次运行会自动做 3 次全表扫描（universe/复权因子/ST，共约 5s）并落盘缓存；
批量或并发回测前建议先预热：

```bash
cd /root/pcx/rqalpha-release-6.5.0

# 全市场日K + 46 指数（约 20s，856MB）
PYTHONPATH=$PWD python3 -m rqalpha.mod.rqalpha_mod_quantdb.prewarm --day

# 指定标的的分钟K（约 1.3s/只，51MB/只）
PYTHONPATH=$PWD python3 -m rqalpha.mod.rqalpha_mod_quantdb.prewarm --minute --codes 600519,000001
```

- 日K/指数缓存用 mmap 加载，多进程共享物理内存页，并发内存不翻倍。
- 机器配置 2026-10-30 起为 limit=56核，100 并发批量回测已验证可行；并发回测时保持
  `threads: 2`（每进程 DuckDB 线程数），避免线程爆炸。
- 缓存新鲜度按"数据探测"判断（探测到 quantdb 数据更新才重建），
  数据快照不更新就不会反复重扫。

---

## 4. 注意事项（重点）

### 4.1 生存偏差

日线只有**当前仍在上市**的 5,574 只股票，没有历史退市股。跨多年回测会偏乐观。
273 只在分钟数据里存在但日线缺失（多为退市/长期停牌）的标的未注册。

### 4.2 只支持 1d / 1w / 1m

无 tick、无 5m/15m/30m/60m。无 ETF/LOF/可转债行情，标的类型仅股票 + 指数；
指数只有日线没有分钟。

### 4.3 日线从 2016 年开始

分钟原始数据虽然从 2010 年开始，但 2016 年前没有日线和复权因子，
竞价/涨跌停会缺数据，第一阶段不开放（2016 前请求会被裁到 2016-01-04）。

### 4.4 复权语义

- `history_bars(adjust_type='pre')` 的锚点是**回测当前日**（RQAlpha 语义），
  不要拿 `qdata.daily(adj='forward')` 的绝对值硬比（它锚定数据集最新交易日），
  要按同一锚点换算后再比。
- 后复权锚定 2016-01-04（`v_adjust_factors` 口径），绝对值与"自上市累计"不同，
  回测内部自洽。

### 4.5 分红/配股

- 分红只有除权日，登记日/公告日/派息日近似为同一天（当天入账）。
- 配股（`allotment`）不建模持仓数量变化；价格连续性由复权因子保证。

### 4.6 ST 与涨跌停

- ST 来自 `v_features_daily.is_st` 的**历史逐日**值；`v_features_daily` 比日线
  少约 27 万行，缺失日按非 ST 兜底。
- 涨跌停按"昨收 + 除权事件 + 当日 is_st + 板块规则"推导，交易所口径四舍五入到分。
  **不要用 `v_features_daily.zt_price/dt_price`**（快照值，长期不变，且 ST 按 10% 算，是错的）。
- `st_mode: none` 可关闭 ST 收窄（全部按板块常规幅度）。

### 4.7 一条无害的警告

```
WARN: The strategy requires explicit configuration of base.capital_gain_tax_rate ...
```

RQAlpha 的提示，本地回测可忽略。

---

## 5. 常见报错

| 报错 | 原因 / 处理 |
|---|---|
| `ModuleNotFoundError: No module named 'rqalpha'` | 不在仓库根目录。`cd` 或设 `PYTHONPATH` |
| `No module named 'h5py'/'logbook'/...` | 环境依赖缺失，按 1.1 的清单 pip 安装 |
| 数据源行为像 stockdb | stockdb 与 quantdb 同时启用，互相覆盖；只启用一个 |
| `There is no data between ... and ...` | 请求区间超出数据范围（日线 ≥ 2016-01-04，分钟 ≤ 2026-08-06） |
| 分钟回测很慢 | 冷缓存触发全历史拉取，先 `prewarm --minute --codes ...` |
| `universe.json` 反复重建 | 正常情况不会；若数据在更新中，探测到新日期会主动重建一次 |

---

## 6. 相关文件

- `rqalpha/mod/rqalpha_mod_quantdb/README.md` — mod 的数据能力、口径、限制、性能
- `quantdb_examples/` — 4 个可直接运行的示例
- `reports/verification.md` — 验收记录（复权/分红/基准/停牌/数据一致性逐项）
- `BACKTEST.md` — stockdb 数据源的使用说明（另一个数据源）
- `/root/pcx/quantdb/README.md` — quantdb 本身的数据说明
