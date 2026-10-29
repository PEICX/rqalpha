# -*- coding: utf-8 -*-
# stockdb 数据源 Mod：将本地 stockdb 数据库注册为 RQAlpha 数据源
#
# 策略启用方式（strategy __config__）:
#   __config__ = {
#       "base": {"start_date": "2024-01-01", "end_date": "2024-12-31",
#                "accounts": {"stock": 100000}, "frequency": "1d"},
#       "mod": {
#           "stockdb": {
#               "enabled": True,
#               "lib": "rqalpha.mod.rqalpha_mod_stockdb",
#               # 可选配置:
#               # "host": "127.0.0.1", "port": 80,
#               # "server_dir": "/root/pcx/stockdb",   # 配置后查询超时可自动重启本地服务
#               # "auto_restart": True,
#               # "cache_dir": "~/.rqalpha/stockdb_cache",
#           },
#       },
#   }

from rqalpha.interface import AbstractMod

from .data_source import StockdbDataSource


class StockdbMod(AbstractMod):
    def __init__(self):
        self._data_source = None
        # 注入策略 API（sb_* 系列）必须在初始化阶段完成
        from . import api  # noqa: F401  (import 副作用即完成 @export_as_api 注册)

    def start_up(self, env, mod_config):
        self._data_source = StockdbDataSource(env.config.base, mod_config)
        env.set_data_source(self._data_source)

    def tear_down(self, code, exception=None):
        self._data_source = None
