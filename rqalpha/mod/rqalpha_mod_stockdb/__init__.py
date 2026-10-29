# -*- coding: utf-8 -*-
# stockdb 本地数据库数据源 Mod
# 文档: 本地 stockdb（分层 K-V 数据库，HTTP+msgpack 协议）提供 A股日K/分钟K、
# 代码全集、除权除息事件与板块映射。本 mod 将其接入 RQAlpha 数据层。

__config__ = {
    "priority": 1,
    "host": "127.0.0.1",
    "port": 80,
    "timeout": 30,
    "server_dir": "",
    "auto_restart": None,
    "cache_dir": "~/.rqalpha/stockdb_cache",
    "benchmark_etf_map": {},
}


def load_mod():
    from .mod import StockdbMod
    return StockdbMod()
