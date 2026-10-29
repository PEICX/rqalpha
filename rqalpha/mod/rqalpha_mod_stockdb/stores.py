# -*- coding: utf-8 -*-
# stockdb -> rqalpha 各类存储适配
#
# 日K/分钟K统一转为 rqalpha 的 numpy 结构化数组（BaseDataSource 消费格式）。
# stockdb 无涨跌停价字段，按 pre_close 与板块规则计算：
#   主板10% / 创业板·科创板20% / 北交所30% / ST 5%（科创创业ST仍为20%）
# stockdb"复权"表映射为 rqalpha 的分红、拆股与累积复权因子：
#   div 为每股现金分红（已用 mult = pre_close/(pre_close-div) 交叉验证）
#   give/trans 为每10股送/转股数；cum 为累积复权因子（随时间递增），
#   与 rqalpha ex_cum_factor 语义一致（qfq = price * cum(t)/cum(base)）

import datetime
import os

import numpy as np
import pandas as pd

from rqalpha.data.base_data_source.storage_interface import (AbstractCalendarStore, AbstractDateSet,
                                                             AbstractDayBarStore, AbstractDividendStore,
                                                             AbstractSimpleFactorStore)
from rqalpha.data.base_data_source.storages import DateSet, ShareTransformationStore
from rqalpha.utils.datetime_func import convert_int_to_datetime
from rqalpha.utils.functools import lru_cache
from rqalpha.utils.logger import system_log

from .client import DAY_ROW_AP, MINUTE_ROW_AP
from .universe import _etf_limit_ratio, _stock_limit_ratio

CS_DTYPE = np.dtype([
    ('datetime', np.int64),
    ('open', np.float64),
    ('close', np.float64),
    ('high', np.float64),
    ('low', np.float64),
    ('volume', np.float64),
    ('total_turnover', np.float64),
    ('limit_up', np.float64),
    ('limit_down', np.float64),
    ('pre_close', np.float64),
    ('is_st', np.bool_),
])


def _to_date_int(d):
    if isinstance(d, (int, np.integer)):
        return int(d // 1000000) if d > 100000000 else int(d)
    return d.year * 10000 + d.month * 100 + d.day


class StockdbDayBarStore(AbstractDayBarStore):
    """日K存储：从 stockdb 拉取全历史，转为 CS_DTYPE 结构化数组，带内存+磁盘缓存

    注意：stockdb 的 pre_close 字段基准不一致（部分时段为前复权价），
    涨跌停基准价一律用前一日 close 推导（除权日按 div/give/trans 调整）。
    """

    def __init__(self, client, cache_dir, universe, corp_action_loader, etf=False):
        self._client = client
        self._cache_dir = os.path.join(os.path.abspath(os.path.expanduser(cache_dir)), "day")
        self._universe = universe
        self._corp_actions = corp_action_loader
        self._etf = etf
        self._bars = {}
        self._meta_checked = set()  # 本会话已向服务端确认过最新日期的代码

    def get_bars(self, order_book_id):
        code = order_book_id.split(".")[0]
        bars = self._bars.get(code)
        if bars is not None:
            return bars
        bars = self._load(code)
        self._bars[code] = bars
        return bars

    def _cache_path(self, code):
        return os.path.join(self._cache_dir, code + ".npy")

    def _is_cache_valid(self, code, arr):
        if len(arr) == 0:
            return False
        cache_last = int(arr["datetime"][-1]) // 1000000
        today = _to_date_int(datetime.date.today())
        meta = self._universe._meta.get(code, {})
        meta_last = meta.get("last")
        if meta.get("delisted"):
            # 已退市代码数据不再增长
            return meta_last is not None and cache_last >= meta_last
        end = min(meta_last, today) if meta_last else today
        if meta_last is not None and end <= cache_last < today:
            # 缓存已覆盖已知数据但未到今日：meta.last 在增量模式下被冻结，
            # 需向服务端确认是否有更新的数据（每会话每代码一次）。
            # 缓存已覆盖到今日的代码不可能有更新数据，无需确认。
            self._check_newer_data(code)
            meta = self._universe._meta.get(code, {})
            meta_last = meta.get("last")
            end = min(meta_last, today) if meta_last else today
        return cache_last >= end

    def _check_newer_data(self, code):
        if code in self._meta_checked:
            return
        self._meta_checked.add(code)
        try:
            info = self._client.get_first_last_info(code)
            if info is None:
                return
            first, last, name = info
        except Exception as e:
            system_log.warning("stockdb: refresh meta failed for {}: {}".format(code, e))
            return
        self._universe.refresh_last(code, first, last, name)

    def _load(self, code):
        os.makedirs(self._cache_dir, exist_ok=True)
        path = self._cache_path(code)
        if os.path.exists(path):
            try:
                # mmap 加载: 多个回测进程共享物理内存页（操作系统页缓存），
                # 全市场日K 的多进程并发内存占用从 N×2.5GB 降为 ~2.5GB
                arr = np.load(path, mmap_mode="r")
                if self._is_cache_valid(code, arr):
                    return arr
                del arr
            except (OSError, ValueError) as e:
                system_log.warning("stockdb day cache load failed for {}: {}".format(code, e))
        rows = self._client.get_day_rows(code, ap=DAY_ROW_AP)
        arr = self._build(code, rows)
        try:
            np.save(path, arr)
        except OSError as e:
            system_log.warning("stockdb day cache save failed for {}: {}".format(code, e))
        return arr

    def _build(self, code, rows):
        n = len(rows)
        arr = np.empty(n, dtype=CS_DTYPE)
        if n == 0:
            return arr
        name = self._universe.name_of(code)

        def _ratio_for(cd, is_st):
            if self._etf:
                return _etf_limit_ratio(name)
            return _stock_limit_ratio(cd, is_st)

        # 除权除息事件: date -> (div, give, trans)
        events = self._corp_actions.get(code)
        event_map = {int(e["date"]): (e["div"], e["give"], e["trans"]) for e in events} \
            if len(events) else {}
        pre_close = np.nan
        for i, row in enumerate(rows):
            date_int = int(row[0])
            open_, close = float(row[1]), float(row[2])
            high, low = float(row[3]), float(row[4])
            volume, amount = float(row[5]), float(row[6])
            is_st = bool(row[7]) if len(row) > 7 else False
            if i > 0:
                prev_close = float(rows[i - 1][2])
                if date_int in event_map:
                    div, give, trans = event_map[date_int]
                    pre_close = (prev_close - div) / (1 + (give + trans) / 10.0)
                else:
                    pre_close = prev_close
            ratio = _ratio_for(code, is_st)
            arr[i] = (
                date_int * 1000000, open_, close, high, low, volume, amount,
                _limit_price(pre_close, ratio, True),
                _limit_price(pre_close, ratio, False),
                pre_close, is_st,
            )
        return arr


def _limit_price(pre_close, ratio, up):
    if not np.isfinite(pre_close) or pre_close <= 0:
        return np.nan
    price = pre_close * (1 + ratio) if up else pre_close * (1 - ratio)
    return float(np.round(price, 2))


class IndexProxyDayBarStore(AbstractDayBarStore):
    """指数代理存储：用对应ETF日K补齐成完整交易日序列（基准指数数据源）"""

    def __init__(self, etf_store, calendar, benchmark_etf_map):
        self._etf_store = etf_store
        self._calendar = calendar
        self._map = benchmark_etf_map
        self._bars = {}

    def get_bars(self, order_book_id):
        bars = self._bars.get(order_book_id)
        if bars is not None:
            return bars
        etf_obid = self._map.get(order_book_id)
        if etf_obid is None:
            bars = np.empty(0, dtype=CS_DTYPE)
            self._bars[order_book_id] = bars
            return bars
        etf_bars = self._etf_store.get_bars(etf_obid)
        bars = self._fill(etf_bars)
        self._bars[order_book_id] = bars
        return bars

    def _fill(self, etf_bars):
        if len(etf_bars) == 0:
            return etf_bars
        first = int(etf_bars["datetime"][0]) // 1000000
        last = int(etf_bars["datetime"][-1]) // 1000000
        cal = self._calendar
        lo = int(cal.searchsorted(first, side="left"))
        hi = int(cal.searchsorted(last, side="right"))
        fill_dates = cal[lo:hi]
        if len(fill_dates) == len(etf_bars):
            return etf_bars
        result = np.empty(len(fill_dates), dtype=CS_DTYPE)
        etf_dates = etf_bars["datetime"] // 1000000
        j = 0
        prev_close = float(etf_bars["open"][0])
        for i, d in enumerate(fill_dates):
            d = int(d)
            if j < len(etf_bars) and int(etf_dates[j]) == d:
                result[i] = etf_bars[j]
                prev_close = float(etf_bars["close"][j])
                j += 1
            else:
                result[i] = (
                    d * 1000000, prev_close, prev_close, prev_close, prev_close,
                    0.0, 0.0, np.nan, np.nan, prev_close, False,
                )
        return result


class StockdbMinuteStore(object):
    """分钟K存储：按 (code, 交易日) 提供数据。

    缓存未命中时一次性拉取该标的全部分钟历史（单请求），按日切片落盘——
    用一次大请求替代数百次逐日请求，大幅降低 stockdb 串行查询压力。
    内存中持有最近使用日的数组（条数可配，配大内存可提高容量）。
    """

    def __init__(self, client, cache_dir, day_bar_store, mem_days=512):
        self._client = client
        self._cache_dir = os.path.join(os.path.abspath(os.path.expanduser(cache_dir)), "minute")
        self._day_bar_store = day_bar_store
        self._mem_days = int(mem_days)
        self._mem = {}
        self._loaded_full = set()  # 已做过整历史加载的代码

    def get_day(self, code, date_int):
        key = (code, date_int)
        arr = self._mem.get(key)
        if arr is not None:
            return arr
        path = os.path.join(self._cache_dir, code, "{}.npy".format(date_int))
        if not os.path.exists(path) and code not in self._loaded_full:
            # 冷缓存: 一次请求拉全历史并逐日落盘（含当日）
            self._load_full_history(code)
        if os.path.exists(path):
            try:
                arr = np.load(path, mmap_mode="r")
                self._mem[key] = arr
                return arr
            except (OSError, ValueError):
                pass
        rows = self._client.get_minute_day(code, date_int)
        arr = self._build(code, date_int, rows)
        self._mem_put(key, arr)
        try:
            os.makedirs(os.path.join(self._cache_dir, code), exist_ok=True)
            np.save(path, arr)
        except OSError as e:
            system_log.warning("stockdb minute cache save failed: {}".format(e))
        return arr

    def _mem_put(self, key, arr):
        self._mem[key] = arr
        while len(self._mem) > self._mem_days:
            self._mem.pop(next(iter(self._mem)))

    def _load_full_history(self, code):
        """单请求拉取该标的全部分钟K，按交易日切片缓存"""
        self._loaded_full.add(code)
        rows = self._client.get_minute_all(code)
        if not rows:
            return
        by_day = {}  # type: dict
        for r in rows:
            by_day.setdefault(int(str(r[0])[:8]), []).append(r)
        os.makedirs(os.path.join(self._cache_dir, code), exist_ok=True)
        for date_int, day_rows in by_day.items():
            path = os.path.join(self._cache_dir, code, "{}.npy".format(date_int))
            if os.path.exists(path):
                continue
            arr = self._build(code, date_int, day_rows)
            self._mem_put((code, date_int), arr)
            try:
                np.save(path, arr)
            except OSError as e:
                system_log.warning("stockdb minute cache save failed: {}".format(e))

    def _build(self, code, date_int, rows):
        n = len(rows)
        arr = np.empty(n, dtype=CS_DTYPE)
        if n == 0:
            return arr
        # 涨跌停基准价与 ST 标志取自当日日K
        pre_close, is_st = self._day_info(code, date_int)
        if code.startswith(("51", "56", "58", "15")):
            ratio = _etf_limit_ratio(self._day_bar_store._universe.name_of(code))
        else:
            ratio = _stock_limit_ratio(code, is_st)
        for i, row in enumerate(rows):
            open_, close = float(row[1]), float(row[2])
            high, low = float(row[3]), float(row[4])
            arr[i] = (
                int(row[0]), open_, close, high, low, float(row[5]), float(row[6]),
                _limit_price(pre_close, ratio, True), _limit_price(pre_close, ratio, False),
                pre_close, is_st,
            )
        return arr

    def _day_info(self, code, date_int):
        """返回 (当日日K pre_close, is_st)"""
        bars = self._day_bar_store.get_bars(code + ".XSHG" if code.startswith(("5", "6")) else code + ".XSHE")
        if len(bars) == 0:
            return np.nan, False
        pos = bars["datetime"].searchsorted(np.int64(date_int * 1000000))
        if pos < len(bars) and int(bars["datetime"][pos]) == date_int * 1000000:
            return float(bars["pre_close"][pos]), bool(bars["is_st"][pos])
        return np.nan, False


class CorpActionLoader(object):
    """除权除息事件加载与缓存（升序）"""

    def __init__(self, client):
        self._client = client
        self._events = {}

    def get(self, code):
        events = self._events.get(code)
        if events is None:
            rows = self._client.get_corp_actions(code)
            events = np.zeros(len(rows), dtype=[
                ('date', np.int64), ('div', np.float64), ('give', np.float64),
                ('trans', np.float64), ('cum', np.float64),
            ])
            for i, (date_int, row) in enumerate(rows):
                events[i] = (date_int, float(row.get("div", 0) or 0),
                             float(row.get("give", 0) or 0), float(row.get("trans", 0) or 0),
                             float(row.get("cum", 1) or 1))
            self._events[code] = events
        return events


class StockdbDividendStore(AbstractDividendStore):
    def __init__(self, loader):
        self._loader = loader

    @lru_cache(1024)
    def get_dividend(self, order_book_id):
        code = order_book_id.split(".")[0]
        events = self._loader.get(code)
        events = events[events["div"] > 0]
        if len(events) == 0:
            return None
        arr = np.zeros(len(events), dtype=[
            ('book_closure_date', '<i8'), ('announcement_date', '<i8'),
            ('dividend_cash_before_tax', '<f8'), ('ex_dividend_date', '<i8'),
            ('payable_date', '<i8'), ('round_lot', '<f8'),
        ])
        for i, e in enumerate(events):
            arr[i] = (e["date"], e["date"], e["div"] * 100.0, e["date"], e["date"], 100.0)
        return arr


class StockdbSplitStore(AbstractSimpleFactorStore):
    def __init__(self, loader):
        self._loader = loader

    @lru_cache(1024)
    def get_factors(self, order_book_id):
        code = order_book_id.split(".")[0]
        events = self._loader.get(code)
        events = events[(events["give"] + events["trans"]) > 0]
        if len(events) == 0:
            return None
        arr = np.zeros(len(events), dtype=[
            ('ex_date', '<i8'), ('split_factor', '<f8'),
            ('split_coefficient_to', '<f8'), ('split_coefficient_from', '<f8'),
        ])
        for i, e in enumerate(events):
            to = 10.0 + e["give"] + e["trans"]
            arr[i] = (e["date"] * 1000000, to / 10.0, to, 10.0)
        return arr


class StockdbExFactorStore(AbstractSimpleFactorStore):
    def __init__(self, loader):
        self._loader = loader

    @lru_cache(1024)
    def get_factors(self, order_book_id):
        code = order_book_id.split(".")[0]
        events = self._loader.get(code)
        if len(events) == 0:
            return None
        arr = np.zeros(len(events), dtype=[
            ('start_date', '<i8'), ('ex_cum_factor', '<f8'),
        ])
        for i, e in enumerate(events):
            arr[i] = (e["date"] * 1000000, e["cum"])
        return arr


class StockdbCalendarStore(AbstractCalendarStore):
    def __init__(self, calendar):
        self._calendar = calendar  # np.ndarray int64 YYYYMMDD

    def get_trading_calendar(self):
        return pd.to_datetime([str(int(d)) for d in self._calendar])


class StockdbSuspendDateSet(AbstractDateSet):
    """停牌判定：区间内无日K记录或成交量为0视为停牌"""

    def __init__(self, get_bars_fn):
        self._get_bars = get_bars_fn
        self._days = {}

    def _bar_dates(self, code):
        days = self._days.get(code)
        if days is None:
            bars = self._get_bars(code)
            if bars is None or len(bars) == 0:
                return None
            trading = bars[bars["volume"] > 0]
            days = (set(int(d // 1000000) for d in trading["datetime"]),
                    int(bars["datetime"][0]) // 1000000,
                    int(bars["datetime"][-1]) // 1000000)
            self._days[code] = days
        return days

    def contains(self, order_book_id, dates):
        days = self._bar_dates(order_book_id.split(".")[0])
        if days is None:
            return None
        trading_dates, first, last = days
        result = []
        for d in dates:
            di = _to_date_int(d)
            result.append(first <= di <= last and di not in trading_dates)
        return result


class StockdbSTDateSet(DateSet):
    """ST 判定：基于日K记录中的 is_st 标志"""

    def __init__(self, get_bars_fn):
        self._get_bars = get_bars_fn
        self._days = {}

    def _st_dates(self, code):
        days = self._days.get(code)
        if days is None:
            bars = self._get_bars(code)
            if bars is None or len(bars) == 0:
                return None
            days = set(int(d // 1000000) for d, st in zip(bars["datetime"], bars["is_st"]) if st)
            self._days[code] = days
        return days

    def contains(self, order_book_id, dates):
        days = self._st_dates(order_book_id.split(".")[0])
        if days is None:
            return None
        return [(_to_date_int(d) in days) for d in dates]


def convert_int_to_datetime_safe(dt_int):
    return convert_int_to_datetime(int(dt_int))

class NullShareTransformation(ShareTransformationStore):
    """无换股合并信息"""

    def __init__(self):
        pass

    def get_share_transformation(self, order_book_id):
        return None
