# -*- coding: utf-8 -*-
# StockdbDataSource: 以本地 stockdb 为后端的数据源
#
# 继承 BaseDataSource 并注册自定义存储，复用其 get_bar / history_bars(1d/1w) /
# 复权 / 集合竞价bar / 开退市过滤等全部机制；另实现分钟线 (1m) 支持。

import datetime
import os
from collections import ChainMap

import numpy as np
import pandas as pd

from rqalpha.const import INSTRUMENT_TYPE, MARKET, TRADING_CALENDAR_TYPE
from rqalpha.data.base_data_source.data_source import BaseDataSource
from rqalpha.data.base_data_source.adjust import FIELDS_REQUIRE_ADJUSTMENT, adjust_bars
from rqalpha.model.instrument import Instrument
from rqalpha.model.tick import TickObject
from rqalpha.utils.datetime_func import convert_dt_to_int
from rqalpha.utils.exception import RQInvalidArgument
from rqalpha.data.base_data_source.storage_interface import AbstractDateSet
from rqalpha.utils.risk_free_helper import YIELD_CURVE_TENORS

from .client import StockdbClient
from .stores import (CorpActionLoader, CS_DTYPE, IndexProxyDayBarStore, NullShareTransformation,
                     StockdbCalendarStore, StockdbDayBarStore, StockdbDividendStore,
                     StockdbExFactorStore, StockdbMinuteStore, StockdbSuspendDateSet,
                     StockdbSplitStore, StockdbSTDateSet)
from .universe import Universe

# stockdb 分钟数据起始日期（YYYYMMDD）
MINUTE_DATA_START = 20250102


def _to_date_int(d):
    if isinstance(d, datetime.datetime):
        return d.year * 10000 + d.month * 100 + d.day
    if isinstance(d, datetime.date):
        return d.year * 10000 + d.month * 100 + d.day
    return int(str(d)[:8].replace("-", ""))




def _cfg(mod_config, key, default=None):
    try:
        value = getattr(mod_config, key)
    except AttributeError:
        return default
    return default if value is None else value


def _cfg_dict(mod_config, key):
    value = _cfg(mod_config, key, None)
    if isinstance(value, dict):
        return dict(value)
    if value is None:
        return {}
    return {k: v for k, v in value.items()}


class StockdbDataSource(BaseDataSource):
    def __init__(self, base_config=None, mod_config=None):
        # 不调用 super().__init__（无 bundle 文件），自行注册存储
        mod_config = mod_config or {}
        self._mod_config = mod_config
        self._market = MARKET.CN
        # mmap 缓存会同时打开较多文件，尽量抬高软性 fd 上限
        try:
            import resource
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
        except (ImportError, ValueError, OSError):
            pass

        server_dir = _cfg(mod_config, "server_dir", "")
        auto_restart = _cfg(mod_config, "auto_restart")
        self._client = StockdbClient(
            host=_cfg(mod_config, "host", "127.0.0.1"),
            port=int(_cfg(mod_config, "port", 80)),
            timeout=float(_cfg(mod_config, "timeout", 30)),
            server_dir=server_dir,
            auto_restart=auto_restart,
        )
        cache_dir = _cfg(mod_config, "cache_dir", "~/.rqalpha/stockdb_cache")
        self._universe = Universe(self._client, cache_dir,
                                  benchmark_etf_map=_cfg_dict(mod_config, "benchmark_etf_map"))
        self._universe.build()
        self._calendar = self._universe.load_calendar()

        # 动态存储注册表（与 BaseDataSource 同构）
        self._ins_id_or_sym_type_map = {}
        self._day_bar_stores = {}
        self._dividend_stores = {}
        self._split_stores = {}
        self._calendar_stores = {}
        self._ex_factor_stores = {}
        self._id_instrument_map = {}
        self._sym_instrument_map = {}
        self._id_or_sym_instrument_map = ChainMap(self._id_instrument_map, self._sym_instrument_map)
        self._grouped_instruments = {}
        self._share_transformation = NullShareTransformation()

        loader = CorpActionLoader(self._client)
        self._corp_loader = loader
        cs_store = StockdbDayBarStore(self._client, cache_dir, self._universe, loader, etf=False)
        etf_store = StockdbDayBarStore(self._client, cache_dir, self._universe, loader, etf=True)
        index_store = IndexProxyDayBarStore(etf_store, self._calendar, self._universe._benchmark_etf_map)
        self._cs_store = cs_store
        self._etf_store = etf_store
        self.register_day_bar_store(INSTRUMENT_TYPE.CS, cs_store)
        self.register_day_bar_store(INSTRUMENT_TYPE.ETF, etf_store)
        self.register_day_bar_store(INSTRUMENT_TYPE.INDX, index_store)

        for ins_type in [INSTRUMENT_TYPE.CS, INSTRUMENT_TYPE.ETF]:
            self.register_dividend_store(ins_type, StockdbDividendStore(loader))
            self.register_split_store(ins_type, StockdbSplitStore(loader))
            self.register_ex_factor_store(ins_type, StockdbExFactorStore(loader))

        def _get_bars_fn(order_book_id):
            # 供停牌/ST 判定使用；指数代理不参与
            try:
                ins_type = INSTRUMENT_TYPE.CS
                return self._day_bar_stores[ins_type, MARKET.CN].get_bars(order_book_id)
            except KeyError:
                return None

        self._suspend_days = [StockdbSuspendDateSet(_get_bars_fn)]
        self._st_stock_days = StockdbSTDateSet(_get_bars_fn)

        self.register_calendar_store(TRADING_CALENDAR_TYPE.CN_STOCK, StockdbCalendarStore(self._calendar))
        self.register_instruments(Instrument(d) for d in self._universe.instrument_dicts())

        self._minute_store = StockdbMinuteStore(self._client, cache_dir, cs_store,
                                                mem_days=_cfg(mod_config, "minute_mem_days", 512))

    # ---- API 层辅助 ----

    def instrument_order_book_id(self, code):
        for ins in self.get_instruments(id_or_syms=[self._universe.code_to_order_book_id_safe(code)]):
            return ins.order_book_id
        return None

    def _board_rows(self):
        rows = getattr(self, "_board_cache", None)
        if rows is None:
            raw = self._client.query("get", "板块*")
            rows = [(k, v) for k, v in raw] if isinstance(raw, list) else []
            rows.sort(key=lambda kv: str(kv[1].get("category", "")) + str(kv[1].get("code", "")))
            self._board_cache = rows
        return rows

    # ---- 日历与数据范围 ----

    def available_data_range(self, frequency):
        latest = datetime.datetime.strptime(str(int(self._calendar[-1])), "%Y%m%d").date()
        earliest = datetime.datetime.strptime(str(int(self._calendar[0])), "%Y%m%d").date()
        if frequency == '1m':
            minute_start = datetime.datetime.strptime(str(MINUTE_DATA_START), "%Y%m%d").date()
            return max(earliest, minute_start), latest
        return earliest, latest

    def get_yield_curve(self, start_date, end_date, tenor=None):
        # stockdb 无收益率曲线数据；返回平坦曲线（默认年化 2%）供风险指标计算
        rate = float(_cfg(self._mod_config, "risk_free_rate", 0.02))
        lo = _to_date_int(start_date) if start_date is not None else 0
        hi = _to_date_int(end_date) if end_date is not None else 99999999
        mask = (self._calendar >= lo) & (self._calendar <= hi)
        dates = pd.to_datetime([str(int(d)) for d in self._calendar[mask]])
        if len(dates) == 0:
            return None
        df = pd.DataFrame(rate, index=dates, columns=list(YIELD_CURVE_TENORS.values()))
        if tenor is not None:
            return df[tenor]
        return df

    def get_futures_trading_parameters(self, instrument, dt):
        raise NotImplementedError

    # ---- 分钟线支持 ----

    def get_bar(self, instrument, dt, frequency):
        if frequency == '1d':
            return super(StockdbDataSource, self).get_bar(instrument, dt, frequency)
        if frequency != '1m':
            raise NotImplementedError
        code = instrument.order_book_id.split(".")[0]
        bars = self._minute_store.get_day(code, _to_date_int(dt))
        if len(bars) == 0:
            return None
        dt_int = convert_dt_to_int(dt) if isinstance(dt, datetime.datetime) else int(_to_date_int(dt)) * 1000000
        pos = bars['datetime'].searchsorted(np.uint64(dt_int))
        if pos >= len(bars) or int(bars['datetime'][pos]) != dt_int:
            return None
        return bars[pos]

    def history_bars(self, instrument, bar_count, frequency, fields, dt,
                     skip_suspended=True, include_now=False, adjust_type='pre',
                     adjust_orig=None):
        adjust_orig = adjust_orig if adjust_orig is not None else dt
        if frequency != '1m':
            return super(StockdbDataSource, self).history_bars(
                instrument, bar_count, frequency, fields, dt,
                skip_suspended=skip_suspended, include_now=include_now,
                adjust_type=adjust_type, adjust_orig=adjust_orig)
        return self._history_bars_1m(instrument, bar_count, fields, dt, skip_suspended,
                                     include_now, adjust_type, adjust_orig)

    def _history_bars_1m(self, instrument, bar_count, fields, dt, skip_suspended,
                         include_now, adjust_type, adjust_orig):
        code = instrument.order_book_id.split(".")[0]
        dt_int = convert_dt_to_int(dt)
        day_int = dt_int // 1000000

        pos = int(self._calendar.searchsorted(np.int64(day_int), side='right')) - 1
        # 按交易日从新到旧收集，最后统一按时间切片（避免逐日计数偏差）
        if bar_count is None:
            days_back = 260
        else:
            days_back = min(bar_count // 200 + 3, 400)
        collected = []
        for k in range(pos, max(pos - days_back, -1), -1):
            d = int(self._calendar[k])
            if d < MINUTE_DATA_START:
                break
            day_bars = self._minute_store.get_day(code, d)
            if len(day_bars) == 0:
                continue
            if skip_suspended:
                day_bars = day_bars[day_bars['volume'] > 0]
                if len(day_bars) == 0:
                    continue
            collected.append(day_bars)

        if not collected:
            return np.empty(0, dtype=CS_DTYPE)

        bars = collected[0] if len(collected) == 1 else np.concatenate(collected[::-1])
        side = 'right' if include_now else 'left'
        i = int(bars['datetime'].searchsorted(np.uint64(dt_int), side=side))
        left = 0 if bar_count is None else max(0, i - bar_count)
        bars = bars[left:i]

        if len(bars) == 0:
            return bars

        if fields is not None and not self._are_fields_valid(fields, bars.dtype.names):
            raise RQInvalidArgument("invalid fields: {}".format(fields))

        if adjust_type == 'none' or instrument.type in {'Future', 'INDX'}:
            return bars if fields is None else bars[fields]

        if isinstance(fields, str) and fields not in FIELDS_REQUIRE_ADJUSTMENT:
            return bars

        if adjust_orig is None:
            adjust_orig = dt
        bars = adjust_bars(bars, self.get_ex_cum_factor(instrument), fields, adjust_type, adjust_orig)
        return bars if fields is None else bars[fields]

    def current_snapshot(self, instrument, frequency, dt):
        if frequency != '1m':
            raise NotImplementedError
        bar = self.get_bar(instrument, dt, '1m')
        if bar is None:
            return None
        code = instrument.order_book_id.split(".")[0]
        day_bars = self._cs_store.get_bars(instrument.order_book_id)
        day_int = _to_date_int(dt)
        prev_close = np.nan
        limit_up, limit_down = np.nan, np.nan
        pos = int(day_bars['datetime'].searchsorted(np.int64(day_int * 1000000)))
        if pos < len(day_bars) and int(day_bars['datetime'][pos]) == day_int * 1000000:
            limit_up, limit_down = float(day_bars['limit_up'][pos]), float(day_bars['limit_down'][pos])
        if pos > 0:
            prev_close = float(day_bars['close'][pos - 1])
        d = {
            'datetime': dt,
            'open': float(bar['open']),
            'high': float(bar['high']),
            'low': float(bar['low']),
            'last': float(bar['close']),
            'volume': float(bar['volume']),
            'total_turnover': float(bar['total_turnover']),
            'prev_close': prev_close,
            'limit_up': limit_up,
            'limit_down': limit_down,
        }
        return TickObject(instrument, d)
