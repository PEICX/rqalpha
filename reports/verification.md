# rqalpha_mod_quantdb 验收记录

验收日期：2026-10-29。环境：`/root/pcx/rqalpha-release-6.5.0`（仓库根目录执行，
`PYTHONPATH` 同路径），quantdb catalog `/root/pcx/quantdb/catalog.duckdb`，
缓存 `~/.rqalpha/quantdb_cache`（预热后 856MB 日K + 3 只示例分钟）。

> 环境说明：验收开始时 `import rqalpha` 因缺依赖失败（h5py、logbook、rqrisk、
> matplotlib、msgpack、TA-Lib 均未安装，stockdb 示例同样跑不通）。
> 已 `pip3 install` 补齐后 stockdb 基线恢复，再进行以下验收。
> 验收过程中用户级 `~/.rqalpha/mod_config.yml` 曾临时切换为 quantdb，
> 验收结束后先恢复为 stockdb；**随后按用户要求，默认数据源已正式切换为
> quantdb**（stockdb 段保留但 `enabled: false`，切换方法见
> `BACKTEST_QUANTDB.md` 1.2）。stockdb_examples/ 的 4 个示例已相应补上
> `quantdb.enabled=False`，切换后仍按 stockdb 数据运行。

---

## 0. 数据契约核对（写代码前，直接查 DuckDB）

| 假设 | 核对结果 |
|---|---|
| 日线 10,759,271 行 / 5,574 只 / 2016-01-04~2026-09-30 | ✅ 一致 |
| `amount` 单位万元（`close×volume/1e4 ≈ amount`） | ✅ 600519 2024-06-03：496,604 vs 496,097 |
| 指数 46 个 / 114,293 行 | ✅ 一致 |
| `factor = 后复权收盘 / 不复权收盘`，锚定 2016-01-04 = 1.0 | ✅ 逐日核对一致 |
| 分红 58,605 条；600519 2026-06-26 `interest=280.24`（每 10 股） | ✅ 一致 |
| 000990.SZ 2026-07-30 `stock_bonus=4`（10 送 4） | ✅ 一致 |
| `v_features_daily.is_st` 逐年都有值（2016:8,619 → 2026:34,518 ST 行） | ✅ 一致 |
| `v_features_daily.list_date` = 真实上市日（600519 → 2001-08-27） | ✅（348 只缺失回退 meta_symbols/首根日线，其中 4 只是脏值 '0'） |
| 分钟每交易日 241 根含 09:30 竞价（O=H=L=C 有量） | ✅ 000990 2026-07-29: 241 根，09:30 volume=122,600 |
| 2026-08-07 只有 52 根（10:21 截止） | ✅ 一致 |
| 分钟 `turnover` 单位元（vwap=turnover/volume ≈ 价格） | ✅ 一致 |
| 2026-09-30 零成交量 11 只 | ✅ 一致 |
| 分钟有、日线无的 symbol 273 只 | ✅ 一致 |
| 无退市股（`delisting_date` 全空） | ✅ 一致 |

---

## 1. 日线回测

```bash
cd /root/pcx/rqalpha-release-6.5.0
python3 -m rqalpha run \
  -f /tmp/qdb_cli/strategy_day.py -s 2024-01-02 -e 2024-12-31 -a stock 100000 -fq 1d \
  -bm 000300.XSHG -o /tmp/qdb_day.pkl
# （stockdb.enabled False + quantdb.enabled True 显式指定）
```

结果（热缓存 2.7s）：

```
total_returns = 0.013356   benchmark_total_returns = 0.146833
alpha = -0.0968   beta = 0.6801   sharpe = 0.0803   max_drawdown = 0.1419
trades = 25
```

**信号逐笔对比**：用 `qdata.daily("000001.SZ", adj="forward")` 手算 MA5/MA20
（RQAlpha 日线 `history_bars` 在 handle_bar 时包含当日收盘，当日收盘价成交，
为框架固有语义；`volume>0` 对应 `skip_suspended=True`）：

```
预期笔数: 25  实际笔数: 25
逐笔信号对比 mismatch = 0
前3笔 预期: [('2024-01-03','BUY'), ('2024-01-08','SELL'), ('2024-01-16','BUY')]
前3笔 实际: [('2024-01-03','BUY'), ('2024-01-08','SELL'), ('2024-01-16','BUY')]
```

成交价即当日收盘（如 2024-01-03 BUY 10,300 股 @ 9.20）。

## 2. 分钟回测

```bash
python3 -m rqalpha run \
  -f /tmp/qdb_cli/strategy_min.py -s 2026-08-03 -e 2026-08-06 -a stock 100000 -fq 1m \
  -o /tmp/qdb_min.pkl
```

结果：`total_returns = -0.005517`，7 笔成交，2.8s。

**bar 结构校验**（直接读 minute store）：

```
000001 20260803: n=240 首=093100 尾=150000 09:30存在=False
000001 20260804: n=240 首=093100 尾=150000 09:30存在=False
000001 20260805: n=240 首=093100 尾=150000 09:30存在=False
000001 20260806: n=240 首=093100 尾=150000 09:30存在=False
```

**收盘价对比 qdata.minute**：

```
20260806 @09:31 mod=11.22 qdata=11.22 一致=True
20260806 @15:00 mod=11.27 qdata=11.27 一致=True
20260803 @10:30 mod=11.56 qdata=11.56 一致=True
```

**竞价 bar**（供 `get_open_auction_bar` 用）与 qdata 09:30 行一致：
open=11.22, volume=639,000（全天 volume=104,634,256 —— 默认实现会把全天量当竞价量，
本 mod 用的是真实竞价量）。

**全区间分钟冒烟**（2016-01-04 ~ 2026-08-06，空策略）：
617,520 根 bar = 2,573 交易日 × 240 根，12.3s 完成。

## 3. 复权（000990.XSHE 2026-07-30，10 送 4）

数据源层（`history_bars`，锚点=回测日 2026-07-30 15:00）：

| 口径 | 07-29 收 | 07-30 收 | 隔夜跳变 |
|---|---|---|---|
| 前复权（锚 07-30） | 6.4857 | 6.5000 | **+0.2203%** |
| 不复权 | 9.0800 | 6.5000 | **-28.4141%** |
| 后复权 | 9.7099 | 9.7313 | +0.2203% |

- 前复权值 6.4857 与 `qdata.daily(adj='forward')` 完全一致（注意 qdata 前复权
  锚定数据集最新交易日，与本表"锚回测日"口径不同，但 07-30 之后无新事件，
  两口径在此时点等价）。
- 施工说明预期"+0.07%"是 07-29 收盘 → 07-30 **开盘**（6.486→6.490）的跳变；
  收盘→收盘为 +0.22%（07-30 收盘 6.50 略高于开盘 6.49）。不复权 -28.41%
  （收盘口径；开盘口径 -28.52%）。除权跳变被正确消除，语义符合预期。
- `ex_cum_factor` 首条自动补 (0, 1.0)，000990 在 2026-07 的唯一变化点 (20260730, 1.49712)。

回测层（示例 3 输出）：

```
07-30 前复权: 6.4857 -> 6.5000 (+0.2203%)
07-30 不复权: 9.0800 -> 6.5000 (-28.4141%)
```

## 4. 分红入账（600519.XSHG 2026-06-26，每 10 股 280.24 元）

示例 3（2026-06-24 买入 1000 股，跨除息日持有）：

```
持仓: 1000 股, 现金: 291353.86      （06-25 收盘）
除息日现金变动: +28024.00 (期望 +28024.00 税前)   （06-26）
```

`280.24 / 10 × 1000 = 28,024` 精确匹配。

## 5. 基准（真实指数）

- 2024 年回测 `-bm 000300.XSHG`：`benchmark_total_returns = 0.146833`，
  与沪深300 指数 2024 年实际涨幅（3,469.35 → 约 3,978）一致；
  `alpha/beta/benchmark_portfolio` 全部非空。
- 全区间（2016-01-04 ~ 2026-09-30）：`benchmark = 0.2561`（4,357.62 / 收盘基期，
  与 `v_index_daily` 一致）。
- 行情来自 `v_index_daily`（46 个真实指数），无 ETF 代理。
  对照实验：误用 stockdb 时同一命令的起始日被 ETF 数据推迟到 2024-01-03
  （BACKTEST.md 5.1 描述的已知行为），quantdb 无此问题。

## 6. 停牌 / 涨跌停 / ST

**停牌**（2026-09-30 的 11 只零成交量股票）：

```
000016 002667 002731 002813 002860 300082 600293 601059 601198 603183 688496
is_suspended(2026-09-30): 全部 True；正常股（000001/600519）: False
```

**ST**：000010.XSHE（*ST美丽，主板）2026-09-30 `is_st=True`，
涨跌停按 5%：pre_close=1.47 → up=1.54 / dn=1.40（期望一致）。
创业板 ST（300555）按 20%：12.18 → 14.62/9.74。北交所（920014/920873）按 30%。

**涨跌停抽查**：随机 20 只（seed=42）与"昨收 × 规则、四舍五入到分"全部一致（mismatch=0）。
除权日案例 000990 2026-07-30：参考价 round(9.08/1.4)=6.49，
涨跌停 7.14/5.84，符合交易所"参考价四舍五入到分再乘幅度"的规则。
（实现用整数运算 `分×(20±p)/20` half-up，避免 numpy banker's rounding
在 .5 分边界与交易所规则的 1 分钱偏差。）

**st_mode=none**：000010 is_st=False，涨跌停按 10%（1.62/1.32）。

## 7. 数据一致性

**随机 20 个 symbol-day（seed=2026）** 对比 `qdata.daily`：
open/close/high/low/volume/成交额（mod `amount×10000`）全部一致，mismatch=0。

**分钟聚合** 对比 `v_minute_daily`（000001，2026-08-03~06）：
竞价 bar + 240 根聚合的 OHLCV/成交额与 `v_minute_daily` **完全一致**
（v_minute_daily 含 09:30 共 241 根，mod 的 `get_day` 剥掉竞价，口径对齐后比对）。

**日线 vs 分钟聚合**（000001 2026-08-06）：volume 104,634,256 vs 104,634,300
（差 0.00004%），成交额差 98 元 —— 两个独立数据源的正常口径差。

## 8. 预热

```bash
PYTHONPATH=$PWD python3 -m rqalpha.mod.rqalpha_mod_quantdb.prewarm --day          # 19s
PYTHONPATH=$PWD python3 -m rqalpha.mod.rqalpha_mod_quantdb.prewarm --minute --codes 600519,000001,000990
```

- `--day`：5,574 只股票 + 46 个指数，19s，缓存 856MB（含 universe/因子/ST 缓存重建）。
- `--minute`：约 1.3s/只，51MB/只（2016-01-04 起）。
- 预热后日线回测冷启动 2.7s（未预热首次运行约 10s，含 3 次全表扫描落缓存）。

期间发现并修复一个 bug：全表流式扫描与循环内的分红表查询共用连接，
导致扫描被顶掉只产出 80 只；改为独立 cursor 后恢复 5,574 只。

## 9. 扩展 API（qd_*）

示例 4（策略内调用）全部通过：

```
qd_bars(qfq)         600519 2026-08-31 close=1299.52
qd_factors(l1)       turn_20/mom_ret_20d 正常返回
qd_financial(income) report_date=2026-06-30, ann_date=2026-08-15（as_of=2026-09-01，无未来函数）
qd_valuation         pe_ttm=19.95, pb=6.47
qd_margin            finance_balance 正常返回
qd_sectors           600519 → 贵州板块/白酒概念/高分红股 等
qd_index_weights     000300 → 300750(4.01%)/300308(3.86%)/600519(3.24%) ...
```

## 10. 其他

- **1w 周线**：继承 BaseDataSource，回测中 `history_bars(4, '1w')` 正常
  （周标签正确映射到交易日，含中秋等节假日跳过周五的情况）。
- **available_data_range**：`1d` = (2016-01-04, 2026-09-30)；
  `1m` = (2016-01-04, 2026-08-06)（自动探测，2026-08-07 仅 52 根被跳过）。
- **get_open_auction_bar**：000001 2026-08-06 竞价 volume=639,000 / turnover=7,169,692
  （真实竞价量，非全天量）。

## 11. 静态检查

```
$ python3 -m mypy rqalpha/mod/rqalpha_mod_quantdb/*.py
Success: no issues found in 8 source files
$ python3 -m ruff check rqalpha/mod/rqalpha_mod_quantdb/*.py
All checks passed!
```

## 12. 示例

```
quantdb_examples/01_daily_dual_ma.py          # 日线双均线 + 指数基准（25 笔，逐笔核对见第 1 节）
quantdb_examples/02_minute_ma.py              # 分钟双均线（7 笔）
quantdb_examples/03_adjust_dividend_check.py  # 复权 + 分红入账（见第 3/4 节）
quantdb_examples/04_api_showcase.py           # qd_* API 演示（见第 9 节）
```

## 13. 已知限制（摘录，全文见 mod README）

1. 生存偏差：日线仅当前上市的 5,574 只，无历史退市股。
2. 无 ETF/可转债行情；指数无分钟线。
3. 仅 1d/1w/1m；无 tick。
4. 分钟开放 2016-01-04 起（原始 2010 起但缺日线/复权因子配套），截止 2026-08-06。
5. 分红四日期近似为除权日同一天；配股不建模持仓变化（价格连续性由复权因子保证）。
6. `qd_bars` 前复权锚定数据集最新交易日；后复权锚定 2016-01-04。
