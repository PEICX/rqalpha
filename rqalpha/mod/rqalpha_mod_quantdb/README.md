# rqalpha_mod_quantdb — quantdb 本地数据源

把 `/root/pcx/quantdb`（DuckDB catalog + 原始 Parquet）接入 RQAlpha 作为行情数据源，
**不构建 HDF5 bundle**。实现方式对齐 `rqalpha_mod_stockdb`：继承 `BaseDataSource`
但不调用其 `__init__`，自行注册 stores / instruments / calendar。

## 数据能力

| 能力 | 说明 |
|---|---|
| A 股日 K | 2016-01-04 ~ 2026-09-30，5,574 只（不复权，复权在查询时按因子折算） |
| 分钟 K（1m） | 2016-01-04 ~ 2026-08-06 开放（原始数据 2010 起，2016 前无日线/复权因子配套） |
| 真实指数日线 | 46 个指数（000300.XSHG、399006.XSHE 等），benchmark 不再用 ETF 代理 |
| 交易日历 | `SELECT DISTINCT dt FROM v_daily_unadjusted`（2,611 个交易日） |
| 复权 | 前/后/不复权，`v_adjust_factors`（factor = 后复权收盘/不复权收盘） |
| 分红送配 | `v_dividend_factors`（每 10 股口径），现金分红入账、送转拆股 |
| 停牌 | 区间内无日K记录或 volume==0 |
| ST | `v_features_daily.is_st` 的**历史逐日**值（stockdb 没有的能力） |
| 涨跌停 | 前收 + 除权事件 + 当日 is_st + 板块规则推导（不用 zt_price/dt_price 快照） |
| 策略 API | `qd_*` 系列：因子/财务/估值/情绪/技术/融资融券/板块/指数权重 |

## 模块结构

```
rqalpha/mod/rqalpha_mod_quantdb/
├── __init__.py     # __config__ 默认值 + load_mod
├── mod.py          # AbstractMod 入口，set_data_source
├── data_source.py  # QuantdbDataSource(BaseDataSource)，1m 分支 + 竞价bar
├── client.py       # DuckDB 只读访问层（参数绑定、流式全表扫描、探测查询）
├── universe.py     # 标的/日历/代码映射，universe.json 磁盘缓存
├── stores.py       # 日K/指数/分钟/分红/拆股/复权/停牌/ST 各存储
├── prewarm.py      # 缓存预热 CLI
└── api.py          # qd_* 策略 API
```

## 配置

```yaml
mod:
  quantdb:
    enabled: true
    lib: rqalpha.mod.rqalpha_mod_quantdb
    catalog_path: /root/pcx/quantdb/catalog.duckdb
    cache_dir: ~/.rqalpha/quantdb_cache
    risk_free_rate: 0.02      # 平坦无风险利率（无收益率曲线数据）
    threads: 2                # 100 并发回测时每进程限 2 线程
    memory_limit: 2GB         # DuckDB 单进程内存上限
    minute_end: ""            # 空=自动探测最后一个分钟完整的交易日；或 YYYYMMDD
    minute_mem_days: 512      # 分钟K内存 LRU 天数
    st_mode: history          # history=历史逐日 is_st；none=全部按非 ST
```

**stockdb 与 quantdb 都会 `env.set_data_source`，只能启用其中一个。**

## 磁盘缓存（~/.rqalpha/quantdb_cache/）

| 文件 | 内容 | 刷新策略 |
|---|---|---|
| `universe.json` | 标的元数据 + 日历 | 探测到日线 max(dt) 更新时重建（全表扫描约 2s） |
| `day/<code>.npy` | 日K（mmap，多进程共享页缓存） | 单票 last 日期落后于 universe 记录时重建 |
| `index/<code>.npy` | 指数日K | 同上 |
| `minute/<code>/<date>.npy` | 分钟K（含 09:30 竞价 bar） | 按日不可变 |
| `ex_factors.pkl` | 复权因子变化点 | 探测更新时重建（全表扫描约 1.3s） |
| `st_dates.pkl` | ST 日期 | 探测更新时重建（全表扫描约 0.8s） |

冷启动首次运行会自动做上述全表扫描（共约 5s）；预热命令可提前完成：

```bash
PYTHONPATH=/root/pcx/rqalpha-release-6.5.0 \
  python3 -m rqalpha.mod.rqalpha_mod_quantdb.prewarm --day            # 全市场日K，约20s/856MB
PYTHONPATH=/root/pcx/rqalpha-release-6.5.0 \
  python3 -m rqalpha.mod.rqalpha_mod_quantdb.prewarm --minute --codes 600519,000001
```

## 关键口径（与 rqalpha 的对接）

- `CS_DTYPE` 与 stockdb 相同；日线 `datetime = YYYYMMDD000000`，分钟 `YYYYMMDDHHMMSS`
- `total_turnover` 统一为**元**：日线 `amount × 10000`（Alpha300 amount 单位万元），分钟 `turnover` 原样（单位已是元）
- 复权：`v_adjust_factors.factor` 与 rqalpha `ex_cum_factor` 语义一致；
  前复权 = `price × f(t) / f(adjust_orig)`，锚点是**回测当前日**（不是数据集末日）
- 分红：`dividend_cash_before_tax = interest`（每 10 股），`round_lot = 10`；
  登记日/公告日/派息日只有除权日，近似为同一天（当天入账）
- 拆股：`split_coefficient_from = 10`、`to = 10 + stock_bonus`（送转，每 10 股）；
  **配股（allotment）不建模持仓变化**，价格连续性由复权因子保证
- 除权日基准价：`(昨收 - interest/10) / (1 + (送转+配股)/10)`，四舍五入到分
- 涨跌停：整数运算实现交易所的四舍五入到分（`分 × (20±p) / 20`）；
  主板 10%、科创/创业板 20%（ST 仍 20%）、北交所 30%、主板 ST 5%；
  上市/数据首日无基准 → NaN（无涨跌停约束）
- 分钟：每交易日 241 根含 09:30 竞价 bar；`get_day` 返回前剥掉 09:30
  （RQAlpha 分钟时钟 09:31 起，不剥会把竞价 bar 当"上一分钟"），
  竞价 bar 由 `get_open_auction_bar` 使用（默认实现会把全天成交量当竞价量）
- 2026-08-07 只有 52 根分钟（数据集截断），`available_data_range('1m')`
  自动截到 2026-08-06；可用 `minute_end` 配置覆盖

## 已知限制

1. **生存偏差**：Alpha300 日线只有当前上市的 5,574 只，无历史退市股；
   跨多年回测结果会偏乐观。另有 273 只在分钟数据中存在但日线缺失（多为退市/长期停牌），
   第一阶段未注册。
2. 无 ETF/LOF/可转债行情（只有转债基础信息），标的类型仅 CS + INDX。
3. 无 tick、无 5m/15m/30m/60m（与 stockdb mod 一致，只支持 1d/1w/1m）。
4. 日线从 2016 年开始；分钟虽从 2010 年开始，但 2016 年前没有日线和复权因子，不开放。
   （第二阶段可用 `v_minute_daily` 回补 2010~2015 日线。）
5. 分钟数据截止 2026-08-07（当天仅 52 根），默认开放到 2026-08-06。
6. 分钟原始数据不复权；跨日视图通过 `ex_cum_factor` 在 `history_bars` 中复权。
7. 分红只有除权日，登记日/公告日/派息日近似为同一天。
8. 配股（`allotment`）不建模持仓数量变化；价格连续性由复权因子保证。
9. `v_features_daily` 比日线少约 27 万行，缺失日按非 ST 兜底。
10. 指数只有日线，没有指数分钟。
11. `qd_bars` 的前复权锚定数据集最新交易日（qdata 口径）；锚定回测日请用
    `history_bars(adjust_type='pre')`。
12. 后复权价锚定 2016-01-04（`v_adjust_factors` 的口径），与"自上市累计"的
    绝对值不同，跨回测内部一致。

## 性能参考（本机实测，8 线程预热）

| 操作 | 耗时 |
|---|---|
| universe + 因子 + ST 首次全表扫描 | ~5s（之后走缓存） |
| 全市场日K预热（5,574 只 + 46 指数） | ~19s，856MB |
| 分钟预热 | ~1.3s/只，约 51MB/只（2016 起） |
| 热缓存日线回测（单票 1 年） | ~2.7s |
| 热缓存分钟回测（4 个交易日） | ~2.8s |
