# -*- coding: utf-8 -*-
# QuantdbDataSource: 以本地 quantdb（DuckDB catalog）为后端的数据源
#
# 继承 BaseDataSource 并注册自定义存储，复用其 get_bar / history_bars(1d/1w) /
# 复权 / 开退市过滤等全部机制；另实现分钟线 (1m) 支持与真实指数基准。
# 与 stockdb mod 的关键差异：
#   - 指数是 v_index_daily 的真实行情（46 个），benchmark 可直接用 000300.XSHG
#   - ST 是 v_features_daily 的历史逐日值
#   - 分钟数据 2016-01-04 起开放（原始数据 2010 起，但 2016 前无日线/复权因子）

import datetime
from collections import ChainMap

import numpy as np
import pandas as pd

from rqalpha.const import INSTRUMENT_TYPE, MARKET, TRADING_CALENDAR_TYPE
from rqalpha.data.base_data_source.data_source import BaseDataSource
from rqalpha.data.base_data_source.adjust import FIELDS_REQUIRE_ADJUSTMENT, adjust_bars
from rqalpha.model.tick import TickObject
from rqalpha.utils.datetime_func import convert_dt_to_int
from rqalpha.utils.exception import RQInvalidArgument
from rqalpha.utils.risk_free_helper import YIELD_CURVE_TENORS

from .client import DuckdbClient, MINUTE_DATA_START
from .stores import (CS_DTYPE, CorpActionLoader, FactorLoader, NullShareTransformation,
                     QuantdbCalendarStore, QuantdbDayBarStore, QuantdbDividendStore,
                     QuantdbExFactorStore, QuantdbIndexDayBarStore, QuantdbMinuteStore,
                     QuantdbSuspendDateSet, QuantdbSplitStore, QuantdbSTDateSet, STLoader)
from .universe import Universe


def _to_date_int(d):
    if isinstance(d, (datetime.datetime, datetime.date)):
        return d.year * 10000 + d.month * 100 + d.day
    return int(str(d)[:8].replace("-", ""))


def _cfg(mod_config, key, default=None):
    try:
        value = getattr(mod_config, key)
    except AttributeError:
        return default
    return default if value is None else value


class QuantdbDataSource(BaseDataSource):
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

        self._client = DuckdbClient(
            catalog_path=_cfg(mod_config, "catalog_path", "/root/pcx/quantdb/catalog.duckdb"),
            threads=int(_cfg(mod_config, "threads", 2)),
            memory_limit=str(_cfg(mod_config, "memory_limit", "2GB")),
        )
        cache_dir = _cfg(mod_config, "cache_dir", "~/.rqalpha/quantdb_cache")
        self._universe = Universe(self._client, cache_dir)
        self._universe.build()
        self._calendar = self._universe.load_calendar()

        # 分钟数据可用截止日：配置显式指定，否则自动探测最后一个分钟完整的交易日
        minute_end_cfg = str(_cfg(mod_config, "minute_end", "") or "").replace("-", "")
        if minute_end_cfg:
            self._minute_end = int(minute_end_cfg)
        else:
            self._minute_end = self._client.probe_minute_end()

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

        st_mode = str(_cfg(mod_config, "st_mode", "history") or "history")
        corp_loader = CorpActionLoader(self._client)
        self._corp_loader = corp_loader
        factor_loader = FactorLoader(self._client, cache_dir)
        self._factor_loader = factor_loader
        st_loader = STLoader(self._client, cache_dir, st_mode=st_mode)

        cs_store = QuantdbDayBarStore(self._client, cache_dir, self._universe,
                                      corp_loader, st_loader)
        index_store = QuantdbIndexDayBarStore(self._client, cache_dir, self._universe)
        self._cs_store = cs_store
        self._index_store = index_store
        self.register_day_bar_store(INSTRUMENT_TYPE.CS, cs_store)
        self.register_day_bar_store(INSTRUMENT_TYPE.INDX, index_store)

        self.register_dividend_store(INSTRUMENT_TYPE.CS, QuantdbDividendStore(corp_loader))
        self.register_split_store(INSTRUMENT_TYPE.CS, QuantdbSplitStore(corp_loader))
        self.register_ex_factor_store(INSTRUMENT_TYPE.CS, QuantdbExFactorStore(factor_loader))

        def _get_bars_fn(order_book_id):
            # 供停牌/ST 判定使用；指数不参与
            try:
                ins_type = INSTRUMENT_TYPE.CS
                return self._day_bar_stores[ins_type, MARKET.CN].get_bars(order_book_id)
            except KeyError:
                return None

        self._suspend_days = [QuantdbSuspendDateSet(_get_bars_fn)]
        self._st_stock_days = QuantdbSTDateSet(_get_bars_fn)

        self.register_calendar_store(TRADING_CALENDAR_TYPE.CN_STOCK,
                                     QuantdbCalendarStore(self._calendar))
        from rqalpha.model.instrument import Instrument
        self.register_instruments(Instrument(d) for d in self._universe.instrument_dicts())

        self._minute_store = QuantdbMinuteStore(self._client, cache_dir, cs_store,
                                                self._universe,
                                                mem_days=_cfg(mod_config, "minute_mem_days", 512))

    # ---- API 层辅助 ----

    def instrument_order_book_id(self, code):
        for ins in self.get_instruments(id_or_syms=[self._universe.code_to_order_book_id_safe(code)]):
            return ins.order_book_id
        return None

    # ---- 日历与数据范围 ----

    def available_data_range(self, frequency):
        latest = datetime.datetime.strptime(str(int(self._calendar[-1])), "%Y%m%d").date()
        earliest = datetime.datetime.strptime(str(int(self._calendar[0])), "%Y%m%d").date()
        if frequency == '1m':
            minute_start = datetime.datetime.strptime(str(MINUTE_DATA_START), "%Y%m%d").date()
            minute_end = datetime.datetime.strptime(str(self._minute_end), "%Y%m%d").date()
            return max(earliest, minute_start), min(latest, minute_end)
        return earliest, latest

    def get_yield_curve(self, start_date, end_date, tenor=None):
        # quantdb 无收益率曲线数据；返回平坦曲线供风险指标计算
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
            return super(QuantdbDataSource, self).get_bar(instrument, dt, frequency)
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
            return super(QuantdbDataSource, self).history_bars(
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
            if d < MINUTE_DATA_START or d > self._minute_end:
                continue
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
        if include_now:
            i = int(bars['datetime'].searchsorted(np.uint64(dt_int), side='right'))
        else:
            i = int(bars['datetime'].searchsorted(np.uint64(dt_int), side='left'))
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

        bars = adjust_bars(bars, self.get_ex_cum_factor(instrument), fields, adjust_type, adjust_orig)
        return bars if fields is None else bars[fields]

    def get_open_auction_bar(self, instrument, dt):
        # 用 09:30 竞价 bar 的真实量额；无分钟数据时回退到日K
        # （默认实现会把全天成交量当成竞价量，明显偏大）
        if instrument.type == 'CS':
            code = instrument.order_book_id.split(".")[0]
            day_int = _to_date_int(dt)
            auction = self._minute_store.get_auction(code, day_int)
            if auction is not None:
                return {
                    "datetime": auction["datetime"],
                    "open": float(auction["open"]),
                    "limit_up": float(auction["limit_up"]) if np.isfinite(auction["limit_up"]) else np.nan,
                    "limit_down": float(auction["limit_down"]) if np.isfinite(auction["limit_down"]) else np.nan,
                    "volume": float(auction["volume"]),
                    "total_turnover": float(auction["total_turnover"]),
                    "last": float(auction["open"]),
                }
        return super(QuantdbDataSource, self).get_open_auction_bar(instrument, dt)

    def current_snapshot(self, instrument, frequency, dt):
        if frequency != '1m':
            raise NotImplementedError
        bar = self.get_bar(instrument, dt, '1m')
        if bar is None:
            return None
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
