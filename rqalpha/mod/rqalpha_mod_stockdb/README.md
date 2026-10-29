# rqalpha_mod_stockdb — 本地 stockdb 数据源

将本地 [stockdb](../../../docs) 分层 K-V 数据库接入 RQAlpha 作为行情数据源，
替代默认的 HDF5 bundle，无需下载 rqbundle。

## 数据能力

| 能力 | 来源表 | 说明 |
|---|---|---|
| A股日K（全市场股票+ETF） | `日k` | 前复权/后复权/不复权；涨跌停价自行计算 |
| 分钟K（1m） | `分钟k` | 数据自 2025-01-02 起 |
| 交易日历 | 锚定股日K并集 | 2000-01-04 至今 |
| 上市/退市日期 | 首末K线 + `退市` 表 | stockdb 无该字段，按首末K线推导 |
| 分红/送转/复权因子 | `复权` 表 | div=每股现金（已用 mult 交叉验证）；cum 为累积因子 |
| 停牌/ST | 日K缺失行 + is_st 字段 | |
| 基准指数 | 对应ETF代理 | 000300→510300 等，见 `DEFAULT_BENCHMARK_ETF_MAP` |

## 策略用法

```python
__config__ = {
    "base": {"start_date": "2024-01-01", "end_date": "2024-12-31",
             "frequency": "1d", "accounts": {"stock": 100000}},
    "mod": {
        "stockdb": {
            "enabled": True,
            "lib": "rqalpha.mod.rqalpha_mod_stockdb",
            # 可选:
            # "host": "127.0.0.1", "port": 80,
            # "server_dir": "/root/pcx/stockdb",  # 配置后查询超时自动重启本地服务
            # "cache_dir": "~/.rqalpha/stockdb_cache",
            # "risk_free_rate": 0.02,             # 平坦收益率曲线（用于 sharpe 等）
        },
        "sys_analyser": {"enabled": True, "benchmark": "000300.XSHG",
                         "output_file": "out.pkl"},
    },
}
```

## 策略 API（sb_* 系列，本地数据、无限流）

| API | 说明 |
|---|---|
| `sb_get_bars(obid, start_date, end_date, fq='qfq')` | 日K DataFrame（可复权） |
| `sb_valuation(obid, ...)` | pe_ttm/pb/总市值/流通市值/换手率/量比 |
| `sb_money_flow(obid, ...)` | 资金流向（主力/大/中/小单净流入） |
| `sb_boards(category)` / `sb_stock_boards(obid)` / `sb_board_stocks(name)` | 概念/申万行业板块双向查询 |
| `sb_indicator("ma,macd,kdj,rsi,boll", obid, ...)` | 技术指标（支持 cross 信号） |

## 性能与并发（K8s pod 实测，request=8核/limit=32核）

基准（分钟回测 230交易日 × 1标的，热缓存）：串行 ~42s/个；每进程内存 ~200MB
（mmap 缓存多进程共享物理页，不随进程数翻倍）。

| 并发数 | 总耗时(该并发数个回测) | 吞吐 | 单任务延迟 | 等效CPU |
|---|---|---|---|---|
| 8 | 52s / 8个 | 0.154 任务/s | ~51s | ~6.5核 |
| 32 | 95s / 32个 | 0.337 | ~92s | ~14核 |
| **64（推荐上限）** | **173s / 64个** | **0.370** | ~173s | ~15.6核 |
| 96 | 253s / 96个 | 0.379 | ~253s | ~15.9核（已平台） |

**推荐：批量回测最大并发 64**。超过 64 吞吐仅 +2~3% 而单任务延迟成倍增长；
CPU 随施压时长逐步分配（K8s burstable），等效核数在 ~16 核平台。

- 热缓存回测是 **CPU 密集**；**冷缓存**时瓶颈是 stockdb 服务的串行查询，多进程
  并发无加速——批量前先预热；
- 启动固定开销 ~2s/进程（缓存热）；分钟K冷取已优化为整历史单请求。

## 缓存预热

批量回测/并发之前先预热，消除冷启动瓶颈：

```bash
PYTHONPATH=<rqalpha目录> python3 -m rqalpha.mod.rqalpha_mod_stockdb.prewarm \
    --minute --codes 510300,000001            # 指定标的分钟K（~20MB/只）
PYTHONPATH=<rqalpha目录> python3 -m rqalpha.mod.rqalpha_mod_stockdb.prewarm \
    --day --stocks --etf                      # 全市场日K（~2.5GB）
PYTHONPATH=<rqalpha目录> python3 -m rqalpha.mod.rqalpha_mod_stockdb.prewarm \
    --minute --codes 510300 --start 2025-06-01  # 指定起始日
```

分钟K冷取已做**整历史单请求**优化（1 次请求替代 ~230 次逐日请求）。

## 内存相关配置

- `minute_mem_days`（默认 512）：分钟K内存缓存保留的交易日数，加大可提高重复访问命中率；
- 日K mmap 不设上限（按需分页，只有实际访问的标的占内存）。

## 已知限制与注意

- **基准指数用 ETF 代理**（本地无指数行情）；回测起始日会被裁剪到 ETF 上市后（如 510300 为 2024-01-02 起）。
- **财务报表、因子值、指数成分、龙虎榜、融资融券**等仅在线接口提供，未在本 mod 集成
  （在线服务有批量与频率限制；如需可在策略中经 stock_sdk 在线接口自行小量调用）。
- 涨跌停价按 `昨收(自行推导) × 板块比例` 计算：主板10%/创业科创20%/北交所30%/ST 5%（科创创业 ST 仍 20%）。
- stockdb 的 `pre_close` 字段基准不一致，本 mod 一律用前一日 close + 除权事件自行推导基准价。
- 5m/15m/30m/60m 周期合成未实现（与开源 rqalpha BaseDataSource 行为一致，只支持 1d/1w/1m）。
- 首次运行会拉取全市场代码的首末K线日期（约 1-2 分钟），之后增量更新并缓存到 `~/.rqalpha/stockdb_cache/`。
- 若查询超时，mod 会自动重启本地 stockdb 服务（需配置 `server_dir`）后重试。

## 文件结构

```
client.py      纯 Python HTTP+msgpack 客户端（超时重试+服务自愈重启）
universe.py    证券全集/交易日历构建（磁盘缓存，增量刷新）
stores.py      日K/分钟K/复权/分红/停牌/ST 存储适配
data_source.py StockdbDataSource（继承 BaseDataSource，扩展 1m 支持）
api.py         sb_* 策略 API 注入
mod.py         AbstractMod 入口
```
