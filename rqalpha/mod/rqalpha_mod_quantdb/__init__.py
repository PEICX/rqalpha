# -*- coding: utf-8 -*-
# quantdb 数据源 Mod：把 /root/pcx/quantdb（DuckDB catalog + Parquet）注册为 RQAlpha 数据源
#
# 文档: 见本包 README.md 与仓库根目录 BACKTEST_QUANTDB.md。
# quantdb 提供 A股日K(2016起)/分钟K(2016起开放)/真实指数日线/复权因子/分红送配/
# 历史ST/财务/因子等数据，本 mod 不构建 HDF5 bundle，直接查 DuckDB + 本地 npy 缓存。

__config__ = {
    "priority": 1,
    "catalog_path": "/root/pcx/quantdb/catalog.duckdb",
    "cache_dir": "~/.rqalpha/quantdb_cache",
    "risk_free_rate": 0.02,
    # 64 并发回测时每个进程限制 DuckDB 线程数，避免线程爆炸
    "threads": 2,
    "memory_limit": "2GB",
    # 分钟数据可用截止日(YYYYMMDD)；留空=自动探测最后一个"分钟完整"的交易日
    "minute_end": "",
    # 分钟K内存 LRU 天数
    "minute_mem_days": 512,
    # history: 使用 v_features_daily 的历史逐日 is_st；none: 全部按非 ST 处理
    "st_mode": "history",
}


def load_mod():
    from .mod import QuantdbMod
    return QuantdbMod()
