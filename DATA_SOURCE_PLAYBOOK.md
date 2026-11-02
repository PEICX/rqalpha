# 本地数据源接入手册（Playbook）

本仓库已两次完成「本地数据库 → rqalpha 可用回测数据源」的接入：

| 实例 | 查询层 | mod | 接入文档 |
|---|---|---|---|
| stockdb | `/root/pcx/stockdb`（分层 K-V + HTTP 服务） | `rqalpha/mod/rqalpha_mod_stockdb` | `BACKTEST.md` |
| quantdb | `/root/pcx/quantdb`（DuckDB catalog + Parquet） | `rqalpha/mod/rqalpha_mod_quantdb` | `BACKTEST_QUANTDB.md` |

本手册把两次接入沉淀为**标准化、可复用的流程**，适用于：

1. **接入新的本地数据库**（新数据集、新格式）——照 Step 0~7 走一遍；
2. **数据快照更新后做回归**——照 §9 复用流程走一遍；
3. **排查口径类回测问题**（价格错位、涨跌停误判、分红不对）——先查 §7 踩坑记录。

> 实例参考：quantdb 接入的完整验收记录在 `reports/verification.md`，
> 数据契约核对表（Step 1 的产出样板）在其第 0 节。

---

## 0. 总体架构与设计原则

```
原始数据（Parquet / K-V，一个字节不动）
   │  零拷贝：DuckDB 视图 / 只读连接
   ▼
查询层（quantdb / stockdb：统一 symbol、日期约定、分区裁剪、派生表）
   │  只读访问 + 口径转换全部集中在这
   ▼
mod 适配层（rqalpha_mod_*：标准化为 rqalpha 口径 + npy 缓存 + 预热）
   ▼
rqalpha 回测（1d / 1w / 1m，复权/分红/停牌/ST/涨跌停/指数基准）
```

**五条原则**（两次接入验证有效）：

1. **原始数据零拷贝**。查询层用视图/只读连接接数据，不搬运不转换；
   原始数据更新后视图自动生效。
2. **口径转换集中在 mod 适配层**，一张映射表说清（§3）。不在查询层改数据、
   不在策略层打补丁。
3. **缓存按访问粒度设计**：日K按标的、分钟按标的×交易日；mmap 让多进程共享页缓存。
4. **写代码前先做数据契约核对**（§2），每个假设都查库验证；
   **验收逐条留痕**（§6），复现性优先。
5. **不构建 HDF5 bundle**。mod 继承 `BaseDataSource` 但不调 `super().__init__`，
   自行注册 stores / instruments / calendar，避免 bundle 的全量构建成本。

---

## 1. Step 0 —— 环境准备

- 解释器是 `python3`（无 `python`）；rqalpha 未 pip install，必须仓库根目录执行或
  `export PYTHONPATH=/root/pcx/rqalpha-release-6.5.0`。
- 依赖清单（本机曾整体缺失导致 `import rqalpha` 失败，换环境需重装）：

  ```bash
  pip3 install h5py logbook rqrisk matplotlib msgpack TA-Lib click \
      python-dateutil filelock jsonpickle methodtools simplejson pyyaml \
      typing_extensions requests duckdb pandas pyarrow numpy
  ```

- 用户级配置 `~/.rqalpha/mod_config.yml`：**数据源 mod 互斥**。任何数据源 mod 都会
  `env.set_data_source`，同时启用互相覆盖，只能启用一个；切换 = 一边 `enabled: true`
  另一边 `false`。
- 命令行临时指定时 `-mc <name>.lib rqalpha.mod.rqalpha_mod_<name>` 不能省
  （非 `sys_*` 内置 mod 不给 `lib` 会找错包路径）。
- 示例策略在自己的 `__config__` 里显式启用一种数据源并禁用另一种，
  不依赖用户级配置，保证随时可跑。

---

## 2. Step 1 —— 数据盘点与契约核对（写代码前）

**这一步决定成败**：所有后续代码都建立在对源数据的假设上，假设必须查库验证。
quantdb 接入时的核对表见 `reports/verification.md` 第 0 节，全部 ✅ 才动工。

### 2.1 必查项（探测 SQL 模板）

```sql
-- 规模与范围：行数、标的数、起止日期
SELECT count(*), count(DISTINCT symbol), min(dt), max(dt) FROM v_daily_unadjusted;

-- 单位验证（金额单位是接入错误重灾区）
--   amount 万元？ close*volume/1e4 ≈ amount 即成立
--   turnover 元？   turnover/volume ≈ 价格 即成立

-- 复权因子语义：factor 应 = 后复权收盘 / 不复权收盘，逐日核对
-- 分钟结构：每个交易日根数（是否含 09:30 竞价 bar）、最后一根时间、尾部是否有截断日
-- 停牌表现：区间内无日K记录 or volume==0（两种口径要分别确认）
-- 退市/生存偏差：delisting_date 是否全空（全空 = 只有当前上市股票 = 有生存偏差）
-- 数据集交叉：日历 vs 日线末日是否一致；分钟有而日线缺的标的数
```

### 2.2 核对表产出格式

| 假设 | 核对结果 |
|---|---|
| 日线 N 行 / M 只 / 起止日期 | ✅ 一致 |
| `amount` 单位万元 | ✅ 抽样验证（附具体数值） |
| ... | ... |

**错的假设在这一步纠正，成本为零；写进代码后纠正，成本是回测结果全部作废。**

---

## 3. Step 2 —— 口径标准化映射表（核心资产）

rqalpha 期望的口径是固定的（`CS_DTYPE` + 各 Store 语义），源数据口径各不相同。
逐项写清转换规则，**这张表就是 mod 的 `stores.py` 的规格说明**：

| 维度 | rqalpha 期望 | 源数据（quantdb 为例） | 转换规则 |
|---|---|---|---|
| 行情数组 | `CS_DTYPE` 结构化数组 | 视图/表 | 逐列映射 + 补齐派生列 |
| datetime | 日线 `YYYYMMDD000000`，分钟 `YYYYMMDDHHMMSS`（int64） | `DATE` / timestamp | `date_int * 1_000_000` / 直接取 |
| 成交额 | `total_turnover` 单位**元** | 日线 `amount` 万元；分钟 `turnover` 元 | 日线 `× 10000`，分钟原样 |
| 复权因子 | `ex_cum_factor`（后复权基准的累积因子） | `factor = 后复权收盘/不复权收盘` | 语义一致直接用；首条不是 0 时框架自动补 `(0, 1.0)` |
| 分红 | `dividend_cash_before_tax / round_lot` = 每股现金 | `interest` 每 10 股 | 保持每 10 股口径 + `round_lot=10`；登记/公告/派息日只有除权日 → 四日期同日近似，写进已知限制 |
| 拆股 | `split_coefficient_from/to` | `stock_bonus` 每 10 股送转 | `from=10, to=10+stock_bonus`；**配股（allotment）不计入拆股**，价格连续性靠复权因子 |
| 涨跌停 | `limit_up/limit_down` | 无逐日值（快照列是错的） | **自行推导**：昨收 + 除权事件 + 当日 is_st + 板块规则；整数运算 half-up 到分 |
| ST | `is_st` 逐日 | `v_features_daily.is_st` 历史逐日 | 直接映射；缺失日按非 ST 兜底并记录 |
| 停牌 | 停牌日集合 | 区间无日K or volume==0 | 两种口径合并 |
| 分钟 bar | 240 根（09:31~11:30, 13:01~15:00） | 241 根含 09:30 竞价 | `get_day` 返回前**剥掉 09:30**；竞价 bar 单独供 `get_open_auction_bar`（真实竞价量，不是全天量） |
| 可用区间 | `available_data_range('1d'/'1m')` | 各表末日不一 | 自动探测最后一个**完整**交易日（如 2026-08-07 只有 52 根 → 截到 08-06），可用配置覆盖 |
| 上市日 | 真实上市日 | `list_date`（勿用首根K线） | 缺失回退元数据/首根日线，脏值清洗 |
| 指数 | INDX 类型标的做 benchmark | 指数日线表 | 代码映射 `.SH→XSHG / .SZ→XSHE / .BJ→BJSE`；`limit/pre_close=NaN, is_st=False` |
| 收益率曲线 | `get_yield_curve` | 无曲线数据 | 平坦利率 `risk_free_rate` 配置 |

### 3.1 涨跌停推导细则（最易错）

- 比例：主板 10%；科创/创业板 20%（**ST 仍 20%**）；北交所 30%；主板 ST 5%。
- 除权日基准价：`(昨收 - interest/10) / (1 + (送转+配股)/10)`，四舍五入到分后再乘幅度。
- **用整数运算**（`分 × (20±p) / 20` half-up），避免 numpy banker's rounding 在
  .5 分边界与交易所规则差 1 分钱。
- 上市/数据首日无基准 → NaN（无涨跌停约束）。
- **不要用源数据的 zt_price/dt_price 快照列**——长期不变且对 ST 按 10% 算，是错的。

---

## 4. Step 3 —— mod 七件套架构

```
rqalpha/mod/rqalpha_mod_<name>/
├── __init__.py     # __config__ 默认值 + load_mod
├── mod.py          # AbstractMod 入口，start_up 里 env.set_data_source(...)
├── data_source.py  # <Name>DataSource(BaseDataSource)：1m 分支 + 竞价bar + 平坦利率
├── client.py       # 源库只读访问层（参数绑定、流式扫描、探测查询）
├── universe.py     # 标的/日历/代码映射，universe.json 磁盘缓存（原子写）
├── stores.py       # 日K/指数/分钟/分红/拆股/复权/停牌/ST 各 Store
├── prewarm.py      # 缓存预热 CLI
└── api.py          # 扩展策略 API（可选，如 qd_* / sb_*）
```

要点：

- `data_source.py` 继承 `BaseDataSource` 但**不调 `super().__init__`**，自行注册
  stores / instruments / calendar（照抄 stockdb/quantdb 任一实现）。
- 必读的框架源码（接口不确定就读实现，不要猜）：
  `rqalpha/data/base_data_source/data_source.py`（`get_bar`/`history_bars`/
  `get_ex_cum_factor`/`get_open_auction_bar`）、`adjust.py`（复权公式）、
  `storage_interface.py`（Store 抽象）、`rqalpha/interface.py`、
  `rqalpha/mod/rqalpha_mod_sys_simulation/simulation_event_source.py`（分钟时钟）。
- 日历**不要用现成日历表**（可能比行情旧），用 `SELECT DISTINCT date FROM 日线表`。
- SQL 一律参数绑定，不拼 symbol 字符串。

---

## 5. Step 4 —— 缓存与并发设计

### 5.1 缓存布局（`~/.rqalpha/<name>_cache/`）

| 文件 | 内容 | 加载方式 | 刷新策略 |
|---|---|---|---|
| `universe.json` | 标的元数据 + 日历 | 启动加载 | 探测到行情 max(dt) 更新才重建 |
| `day/<code>.npy` | 日K | **mmap**（多进程共享页缓存） | 单票 last 日期落后于 universe 记录才重建 |
| `minute/<code>/<date>.npy` | 分钟K | 内存 LRU（默认 512 天）+ npy | 按日不可变 |
| `ex_factors.pkl` / `st_dates.pkl` | 复权因子变化点 / ST 日期 | 启动加载 | 探测更新才重建 |

### 5.2 写安全与并发规则（踩坑换来的，照抄）

1. **原子写**：临时文件 + `os.replace`，多进程/多线程同时写不损坏。
2. **内存 LRU 必须加锁**（`threading.Lock`），淘汰时捕获并发下的 `StopIteration/KeyError`。
3. **缓存未命中只加载请求的数据**（分钟=当日），**绝不全历史落盘**——
   quantdb 曾因未命中触发全历史写 npy，18 并发瞬间写出 1.4M 文件 / +12GB 磁盘。
4. **DuckDB 一连接一用**：全表流式扫描期间不要在同连接上插其他查询，
   否则扫描被顶掉（曾只产出 80/5574 只）；循环内查询用独立 cursor。
5. **每进程 `threads: 2` + `memory_limit`**：100 并发回测时避免线程爆炸；
   DuckDB 只读连接多进程安全。
6. mmap + 探测式刷新 = 并发内存不翻倍、快照不更新不重扫。

---

## 6. Step 5 —— 预热

冷启动会自动做全表扫描落缓存（quantdb 三次扫描约 5s），批量/并发回测**之前**先预热：

```bash
PYTHONPATH=$PWD python3 -m rqalpha.mod.rqalpha_mod_quantdb.prewarm --day            # 全市场日K
PYTHONPATH=$PWD python3 -m rqalpha.mod.rqalpha_mod_quantdb.prewarm --minute --codes 600519,000001
```

quantdb 实测参考：`--day` 5,574 只 + 46 指数 19s / 856MB；分钟 ~1.3s/只 / 51MB/只。
热缓存后单票日线回测 2.7s。并发上限与机器配置见 `BACKTEST_QUANTDB.md` §3。

---

## 7. Step 6 —— 验收清单（逐条留痕）

每条都要**给出实际命令和结果**，写入 `reports/verification.md`。
quantdb 的完整样板见该文件（13 节），必验项：

1. **数据契约核对表**（Step 1 产出，放第 0 节）。
2. **日线回测**：双均线策略跑通；**信号逐笔对比**——用独立通道（如 `qdata.daily`）
   手算 MA 信号，与回测 `trades` 逐笔核对（笔数 + 方向 + 日期，mismatch=0）。
3. **分钟回测**：每天 240 根、首根 09:31、无 09:30；抽样收盘价与独立通道一致。
4. **复权**：选一个除权事件（如 10 送 4），前复权隔夜跳变 ≈ 0、不复权 ≈ 除权幅度；
   注意 rqalpha 前复权锚点是**回测当前日**，与 qdata 锚定数据集末日不同，
   按同一锚点换算后再比。
5. **分红入账**：除权日前持有 N 股，现金增加精确等于 `每股分红 × N`（税前）。
6. **基准**：真实指数（quantdb）或 ETF 代理（stockdb，注意起始日推迟），
   `benchmark_total_returns` 与公开涨幅一致。
7. **停牌/涨跌停/ST**：零成交量日 `is_suspended=True`；ST 票按收窄幅度；
   随机抽 20 只涨跌停与「昨收×规则、四舍五入到分」全对。
8. **数据一致性**：随机 20 个 symbol-day 的 OHLCV 与独立通道一致；
   分钟聚合与派生日线表一致。
9. **静态检查**：`python3 -m mypy` + `python3 -m ruff check` 只过新增文件。

---

## 8. Step 7 —— 文档与示例沉淀

每个数据源 mod 交付四件：

1. **包内 `README.md`**：数据能力表、模块结构、配置、缓存、关键口径、已知限制、性能实测。
2. **仓库根 `BACKTEST_<NAME>.md`**：怎么切换、怎么跑、常见坑、常见报错表。
3. **`<name>_examples/`** ≥3 个：日线、分钟、复权/分红校验（示例自带完整
   `__config__`，显式禁用另一数据源）。
4. **`reports/verification.md`**：验收留痕（上节）。

**已知限制必须写进文档，不要隐瞒**（生存偏差、日期近似、配股不建模等）。

---

## 9. 踩坑记录（按「现象→根因→修复」沉淀）

| # | 现象 | 根因 | 修复 / 预防 |
|---|---|---|---|
| 1 | 回测里涨跌停价格长期不变、ST 股按 10% | 源表 `zt_price/dt_price` 是**快照值**非逐日值 | 一律自行推导（昨收+事件+is_st+板块），禁用快照列 |
| 2 | 卖单全被拒、误判跌停 | 分钟数据是**前复权**口径，日K/涨跌停基准是**不复权**，ETF 份额拆分后两者错位约一半 | 用「当日日K收盘 / 当日最后一根分钟收盘」把分钟价还原为不复权；口径不同的表 join 前先对齐 |
| 3 | 磁盘被写爆（18 并发 +12GB） | 缓存未命中时全历史落盘 npy | 未命中只加载请求的当日；任何「全历史」动作只允许出现在显式 prewarm |
| 4 | `history_bars` 在 09:31 拿到错误"上一分钟" | 09:30 竞价 bar 混入分钟数组 | `get_day` 返回前剥掉 09:30；竞价 bar 单独走 `get_open_auction_bar` |
| 5 | 竞价量异常大 | `get_open_auction_bar` 默认实现把**全天量**当竞价量 | 重写，用真实 09:30 bar |
| 6 | 回测起始日莫名被推迟 | stockdb 无指数行情，benchmark 用 ETF 代理，起始日被裁到 ETF 上市后 | quantdb 用 `v_index_daily` 真实指数；文档明示 stockdb 该行为 |
| 7 | 日历末日比行情旧 | `v_calendar` 维护滞后 | 日历一律 `SELECT DISTINCT date FROM 日线表` |
| 8 | 涨跌停与交易所差 1 分钱 | numpy 默认 banker's rounding，交易所是 half-up | 整数运算 `分×(20±p)/20` |
| 9 | 预热只产出 80/5574 只 | 全表流式扫描与循环内查询**共用 DuckDB 连接**，扫描被顶掉 | 独立 cursor；一连接一用 |
| 10 | 多线程下缓存偶发 KeyError | LRU 淘汰无锁 | `threading.Lock` + 淘汰捕获异常 |
| 11 | 100 并发时 CPU 打满、吞吐反降 | 每进程 DuckDB 默认线程数叠加 = 线程爆炸 | 每进程 `threads: 2` + `memory_limit` |
| 12 | 跨年回测收益偏乐观 | 日线只有当前上市股票，无退市股（生存偏差） | 数据侧无解，**必须写进已知限制**让使用者知情 |
| 13 | 复权绝对值对不上 | 前复权锚点语义混淆（rqalpha 锚回测日 vs qdata 锚数据集末日） | 文档写清两种锚点，对比前换算到同一锚点 |
| 14 | 分红金额差 10 倍 | 源数据每 10 股口径 vs 框架每股 | `round_lot=10` 配套；验收用精确数值比对 |

---

## 10. 数据更新后的复用流程

### 10.1 数据快照更新（同结构）

1. **不需要手工清缓存**：mod 按「探测」刷新——探测到行情 `max(dt)` 更新才重建
   universe/因子/ST 缓存，日K按票的 last 日期落后才重拉。数据不变就不会重扫。
2. 更新后重跑一次预热（分钟缓存按日不可变，增量写新日期）。
3. 回归：跑 `quantdb_examples/` 4 个示例 + `reports/verification.md` 的
   第 3/4/7 节（复权/分红/一致性）抽查。

### 10.2 换机器 / 环境重建

1. 按 §1 重装依赖、重建 `~/.rqalpha/mod_config.yml`（内容见 `BACKTEST_QUANTDB.md` 1.2）。
2. `prewarm --day` 重建缓存。
3. 跑示例确认。

### 10.3 接入全新数据集

Step 0 → 7 全走一遍；预计工作量参考：quantdb（数据契约已核对的前提下）
第一阶段日线+分钟约 2,000 行代码 + 验收记录。

---

## 11. 资产索引

| 资产 | 位置 |
|---|---|
| quantdb mod（能力/口径/限制） | `rqalpha/mod/rqalpha_mod_quantdb/README.md` |
| quantdb 使用说明 | `BACKTEST_QUANTDB.md` |
| quantdb 验收记录（本手册 §2/§7 的样板） | `reports/verification.md` |
| quantdb 可运行示例 | `quantdb_examples/`（4 个） |
| stockdb mod | `rqalpha/mod/rqalpha_mod_stockdb/README.md` |
| stockdb 使用说明 | `BACKTEST.md` |
| stockdb 示例 | `stockdb_examples/`（4 个） |
| quantdb 查询层（数据侧说明、构建脚本、qdata） | `/root/pcx/quantdb/README.md` |
| quantdb 接入施工说明（本流程的原始版本） | `/root/pcx/quantdb/PROMPT_rqalpha_integration.md` |
