# -*- coding: utf-8 -*-
# quantdb 数据源 Mod：将本地 quantdb（DuckDB catalog + Parquet）注册为 RQAlpha 数据源
#
# 策略启用方式（strategy __config__）:
#   __config__ = {
#       "base": {"start_date": "2024-01-01", "end_date": "2024-12-31",
#                "accounts": {"stock": 100000}, "frequency": "1d"},
#       "mod": {
#           "quantdb": {
#               "enabled": True,
#               "lib": "rqalpha.mod.rqalpha_mod_quantdb",
#               # 可选配置:
#               # "catalog_path": "/root/pcx/quantdb/catalog.duckdb",
#               # "cache_dir": "~/.rqalpha/quantdb_cache",
#               # "risk_free_rate": 0.02,
#               # "threads": 2,             # 64 并发回测时每进程限 2 线程
#               # "minute_end": "",         # 空=自动探测；或 YYYYMMDD
#               # "st_mode": "history",     # history | none
#           },
#       },
#   }
#
# 注意：stockdb 与 quantdb 都会 set_data_source，只能启用其中一个。

from rqalpha.interface import AbstractMod

from .data_source import QuantdbDataSource


class QuantdbMod(AbstractMod):
    def __init__(self):
        self._data_source = None
        # 注入策略 API（qd_* 系列）必须在初始化阶段完成
        from . import api  # noqa: F401  (import 副作用即完成 @export_as_api 注册)

    def start_up(self, env, mod_config):
        self._data_source = QuantdbDataSource(env.config.base, mod_config)
        env.set_data_source(self._data_source)

    def tear_down(self, code, exception=None):
        if self._data_source is not None:
            self._data_source._client.close()
        self._data_source = None
