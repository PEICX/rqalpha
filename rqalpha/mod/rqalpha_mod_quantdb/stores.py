# -*- coding: utf-8 -*-
# quantdb -> rqalpha 各类存储适配
#
# 日K/指数/分钟K统一转为 rqalpha 的 numpy 结构化数组（BaseDataSource 消费格式）。
# 关键口径（已逐一核对过 quantdb 数据）：
#   - 日线 amount 单位万元 -> total_turnover = amount * 10000（元）
#   - 分钟 turnover 单位元 -> total_turnover 原样
#   - 分钟 datetime = YYYYMMDDHHMMSS；每交易日 241 根含 09:30 竞价 bar
#     （O=H=L=C 有量）。RQAlpha 股票分钟时钟只有 09:31~15:00，
#     get_day 返回前剥掉 09:30，竞价数据由 get_auction 单独提供。
#   - v_adjust_factors.factor = 后复权收盘/不复权收盘，与 rqalpha
#     ex_cum_factor 语义一致（qfq = price * f(t)/f(anchor)），压缩到变化日存储
#   - v_dividend_factors 的 interest/stock_bonus/allotment 均为每 10 股口径；
#     dividend_cash_before_tax 保持每 10 股 + round_lot=10，配股不建模拆股
#   - 涨跌停不用 v_features_daily.zt_price/dt_price（快照值，不可靠），
#     用前一日收盘 + 当日 is_st + 板块规则逐日推导
#
# 磁盘缓存（~/.rqalpha/quantdb_cache/）：
#   day/<code>.npy   日K（mmap 加载，多进程共享页缓存）
#   index/<code>.npy 指数日K
#   minute/<code>/<date>.npy  分钟K（含 09:30）
#   ex_factors.pkl / st_dates.pkl  复权因子变化点 / ST 日期（避免每进程全表扫描）

import os
import pickle
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from rqalpha.data.base_data_source.storage_interface import (AbstractCalendarStore, AbstractDateSet,
                                                             AbstractDayBarStore, AbstractDividendStore,
                                                             AbstractSimpleFactorStore)
from rqalpha.data.base_data_source.storages import DateSet, ShareTransformationStore
from rqalpha.utils.functools import lru_cache
from rqalpha.utils.logger import system_log

from .universe import _stock_limit_ratio, order_book_id_to_symbol

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

# client.load_day_symbol 返回行的结构（dt, open, close, high, low, volume, amount）
DAY_RAW_DTYPE = np.dtype([
    ('dt', np.int64), ('open', np.float64), ('close', np.float64),
    ('high', np.float64), ('low', np.float64), ('volume', np.float64),
    ('amount', np.float64),
])

# client.load_minute_* 返回行的结构（ts, open, high, low, close, volume, turnover）
MINUTE_RAW_DTYPE = np.dtype([
    ('ts', np.int64), ('open', np.float64), ('high', np.float64),
    ('low', np.float64), ('close', np.float64), ('volume', np.float64),
    ('turnover', np.float64),
])


def _to_date_int(d):
    if isinstance(d, (int, np.integer)):
        return int(d // 1000000) if d > 100000000 else int(d)
    return d.year * 10000 + d.month * 100 + d.day


def _atomic_save_npy(path: str, arr: np.ndarray):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = "{}.{}.tmp.npy".format(path, os.getpid())
    try:
        np.save(tmp, arr)
        os.replace(tmp, path)
    except OSError as e:
        system_log.warning("quantdb cache save failed for {}: {}".format(path, e))
        try:
            os.remove(tmp)
        except OSError:
            pass


def _limit_price(pre_close, ratio, up):
    """涨跌停价（标量）。整数运算实现交易所的四舍五入到分：

    分为单位时 0.05/0.1/0.2/0.3 对应 p=1/2/4/6（比例 = p/20），
    (cents*(20±p) + 10) // 20 即 half-up 取整到分，无浮点误差。
    """
    if not np.isfinite(pre_close) or pre_close <= 0:
        return np.nan
    cents = int(round(pre_close * 100.0))
    p = int(round(ratio * 20.0))
    num = cents * (20 + p) if up else cents * (20 - p)
    return ((num + 10) // 20) / 100.0


def _limit_prices(pre_close, ratios):
    """涨跌停价（数组版），返回 (limit_up, limit_down)。规则同 _limit_price。"""
    up = np.full(len(pre_close), np.nan)
    dn = np.full(len(pre_close), np.nan)
    ok = np.isfinite(pre_close) & (pre_close > 0)
    if not ok.any():
        return up, dn
    cents = np.round(pre_close[ok] * 100.0).astype(np.int64)
    p = np.round(ratios[ok] * 20.0).astype(np.int64)
    up[ok] = ((cents * (20 + p) + 10) // 20) / 100.0
    dn[ok] = ((cents * (20 - p) + 10) // 20) / 100.0
    return up, dn


def _round_to_cents(x: np.ndarray) -> np.ndarray:
    """四舍五入到分（除权参考价用，交易所规则保留 2 位小数）"""
    result = x.copy()
    ok = np.isfinite(result)
    result[ok] = np.floor(result[ok] * 100.0 + 0.5) / 100.0
    return result


class CorpActionLoader(object):
    """除权除息事件加载与缓存（分红送配，升序）。

    v_dividend_factors 全表只有 5.8 万行，一次加载进内存即可，
    不需要磁盘缓存。
    """

    def __init__(self, client):
        self._client = client
        self._events: Optional[Dict[str, np.ndarray]] = None

    def _ensure_loaded(self):
        if self._events is not None:
            return
        raw = self._client.load_dividends()
        events = {}
        for symbol, rows in raw.items():
            arr = np.array(rows, dtype=[
                ('date', np.int64), ('interest', np.float64),
                ('stock_bonus', np.float64), ('allotment', np.float64),
            ])
            events[symbol] = arr
        self._events = events

    def get(self, symbol: str) -> np.ndarray:
        self._ensure_loaded()
        assert self._events is not None
        return self._events.get(symbol, np.zeros(0, dtype=[
            ('date', np.int64), ('interest', np.float64),
            ('stock_bonus', np.float64), ('allotment', np.float64)]))


class FactorLoader(object):
    """复权因子变化点加载与缓存（symbol -> [(date_int, factor)] 升序）。

    v_adjust_factors 每股每天一行（1076 万行），SQL 里压缩到因子变化日
    （约 74 万行）后仍然要全表扫描 ~1.3s；落一份磁盘缓存让并发进程
    各自只读本地文件。新鲜度按数据探测日期判断。
    """

    def __init__(self, client, cache_dir):
        self._client = client
        self._cache_file = os.path.join(
            os.path.abspath(os.path.expanduser(cache_dir)), "ex_factors.pkl")
        self._factors: Optional[Dict[str, np.ndarray]] = None

    def _ensure_loaded(self):
        if self._factors is not None:
            return
        data_last = self._client.probe_daily_max()
        if self._load_cache(data_last):
            return
        system_log.info("quantdb: loading adjust factors (full scan) ...")
        raw = self._client.load_ex_factor_changes()
        factors = {}
        for symbol, rows in raw.items():
            factors[symbol] = np.array(rows, dtype=[('date', np.int64), ('factor', np.float64)])
        self._factors = factors
        self._save_cache(data_last)

    def _load_cache(self, data_last: int) -> bool:
        if not os.path.exists(self._cache_file):
            return False
        try:
            with open(self._cache_file, "rb") as f:
                data = pickle.load(f)
            if int(data.get("data_last", 0)) < data_last:
                return False
            self._factors = data.get("factors", {})
            return True
        except (OSError, ValueError, pickle.UnpicklingError) as e:
            system_log.warning("quantdb ex factor cache load failed: {}".format(e))
            return False

    def _save_cache(self, data_last: int):
        os.makedirs(os.path.dirname(self._cache_file), exist_ok=True)
        tmp = "{}.{}.tmp".format(self._cache_file, os.getpid())
        try:
            with open(tmp, "wb") as f:
                pickle.dump({"data_last": data_last, "factors": self._factors}, f,
                            protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(tmp, self._cache_file)
        except OSError as e:
            system_log.warning("quantdb ex factor cache save skipped: {}".format(e))
            try:
                os.remove(tmp)
            except OSError:
                pass

    def get(self, symbol: str) -> np.ndarray:
        self._ensure_loaded()
        assert self._factors is not None
        return self._factors.get(symbol, np.zeros(0, dtype=[('date', np.int64), ('factor', np.float64)]))


class STLoader(object):
    """历史 ST 日期加载与缓存（symbol -> date_int 数组，升序）。

    v_features_daily 全表扫描 ~0.8s，同样落磁盘缓存。
    st_mode == 'none' 时视为无 ST（涨跌停不再按 5% 收窄）。
    """

    def __init__(self, client, cache_dir, st_mode="history"):
        self._client = client
        self._st_mode = st_mode or "history"
        self._cache_file = os.path.join(
            os.path.abspath(os.path.expanduser(cache_dir)), "st_dates.pkl")
        self._dates: Optional[Dict[str, np.ndarray]] = None

    def _ensure_loaded(self):
        if self._dates is not None:
            return
        if self._st_mode == "none":
            self._dates = {}
            return
        data_last = self._client.probe_daily_max()
        if self._load_cache(data_last):
            return
        system_log.info("quantdb: loading ST dates (full scan) ...")
        raw = self._client.load_st_dates()
        self._dates = {symbol: np.array(days, dtype=np.int64)
                       for symbol, days in raw.items()}
        self._save_cache(data_last)

    def _load_cache(self, data_last: int) -> bool:
        if not os.path.exists(self._cache_file):
            return False
        try:
            with open(self._cache_file, "rb") as f:
                data = pickle.load(f)
            if int(data.get("data_last", 0)) < data_last:
                return False
            self._dates = data.get("dates", {})
            return True
        except (OSError, ValueError, pickle.UnpicklingError) as e:
            system_log.warning("quantdb st cache load failed: {}".format(e))
            return False

    def _save_cache(self, data_last: int):
        os.makedirs(os.path.dirname(self._cache_file), exist_ok=True)
        tmp = "{}.{}.tmp".format(self._cache_file, os.getpid())
        try:
            with open(tmp, "wb") as f:
                pickle.dump({"data_last": data_last, "dates": self._dates}, f,
                            protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(tmp, self._cache_file)
        except OSError as e:
            system_log.warning("quantdb st cache save skipped: {}".format(e))
            try:
                os.remove(tmp)
            except OSError:
                pass

    def get(self, symbol: str) -> np.ndarray:
        self._ensure_loaded()
        assert self._dates is not None
        return self._dates.get(symbol, np.zeros(0, dtype=np.int64))


def _is_st_per_day(dates: np.ndarray, st_days: np.ndarray) -> np.ndarray:
    """逐日 ST 标志（st_days 为该股票 ST 日期升序数组）"""
    if len(st_days) == 0:
        return np.zeros(len(dates), dtype=bool)
    idx = np.clip(st_days.searchsorted(dates), 0, len(st_days) - 1)
    return st_days[idx] == dates


def _ex_day_pre_close(prev_close: np.ndarray, dates: np.ndarray,
                      events: np.ndarray) -> np.ndarray:
    """除权除息日基准价：(昨收 - interest/10) / (1 + (送转+配股)/10)。

    prev_close[0] 为 NaN（首日无基准）；事件日期不在日K中则忽略。
    """
    pre = prev_close.copy()
    if len(events) > 0:
        ev_dates = events["date"]
        pos = dates.searchsorted(ev_dates)
        ok = (pos < len(dates)) & (dates[np.minimum(pos, len(dates) - 1)] == ev_dates)
        idx = pos[ok]
        if len(idx):
            interest = np.zeros(len(dates))
            bonus = np.zeros(len(dates))
            interest[idx] = events["interest"][ok] / 10.0
            bonus[idx] = (events["stock_bonus"][ok] + events["allotment"][ok]) / 10.0
            pre = (pre - interest) / (1.0 + bonus)
    pre[0] = np.nan
    return pre


class QuantdbDayBarStore(AbstractDayBarStore):
    """股票日K存储：CS_DTYPE 结构化数组，内存 + 磁盘缓存（mmap）。

    涨跌停基准价一律用前一日 close 推导（除权日按 interest/送转/配股折算），
    上市/数据首日无基准 -> limit_up/limit_down = NaN。
    """

    def __init__(self, client, cache_dir, universe, corp_loader, st_loader):
        self._client = client
        self._cache_dir = os.path.join(os.path.abspath(os.path.expanduser(cache_dir)), "day")
        self._universe = universe
        self._corp = corp_loader
        self._st = st_loader
        self._bars: Dict[str, np.ndarray] = {}

    def get_bars(self, order_book_id: str) -> np.ndarray:
        code = order_book_id.split(".")[0]
        bars = self._bars.get(code)
        if bars is not None:
            return bars
        bars = self._load(code)
        self._bars[code] = bars
        return bars

    def _cache_path(self, code: str) -> str:
        return os.path.join(self._cache_dir, code + ".npy")

    def _is_cache_valid(self, code: str, arr: np.ndarray) -> bool:
        if len(arr) == 0:
            return False
        cache_last = int(arr["datetime"][-1]) // 1000000
        meta_last = self._universe.last_of(code)
        return cache_last >= meta_last

    def _load(self, code: str) -> np.ndarray:
        path = self._cache_path(code)
        if os.path.exists(path):
            try:
                # mmap 加载: 多个回测进程共享物理内存页
                arr = np.load(path, mmap_mode="r")
                if self._is_cache_valid(code, arr):
                    return arr
                del arr
            except (OSError, ValueError) as e:
                system_log.warning("quantdb day cache load failed for {}: {}".format(code, e))
        rows = self._client.load_day_symbol(self._universe.symbol_of(code))
        arr = self._build(code, rows)
        _atomic_save_npy(path, arr)
        return arr

    def _build(self, code: str, rows: List[tuple]) -> np.ndarray:
        n = len(rows)
        arr = np.empty(n, dtype=CS_DTYPE)
        if n == 0:
            return arr
        raw = np.array(rows, dtype=DAY_RAW_DTYPE)
        dates = raw["dt"]

        prev_close = np.full(n, np.nan)
        prev_close[1:] = raw["close"][:-1]
        events = self._corp.get(self._universe.symbol_of(code))
        pre_close = _ex_day_pre_close(prev_close, dates, events)
        # 除权参考价按交易所规则四舍五入到分（普通日昨收本来就是 2 位小数）
        pre_close = _round_to_cents(pre_close)

        is_st = _is_st_per_day(dates, self._st.get(self._universe.symbol_of(code)))
        ratios = np.where(is_st, _stock_limit_ratio(code, True), _stock_limit_ratio(code, False))

        arr["datetime"] = dates * 1000000
        arr["open"] = raw["open"]
        arr["close"] = raw["close"]
        arr["high"] = raw["high"]
        arr["low"] = raw["low"]
        arr["volume"] = raw["volume"]
        # amount 单位万元 -> 元
        arr["total_turnover"] = raw["amount"] * 10000.0
        arr["limit_up"], arr["limit_down"] = _limit_prices(pre_close, ratios)
        arr["pre_close"] = pre_close
        arr["is_st"] = is_st
        return arr


class QuantdbIndexDayBarStore(AbstractDayBarStore):
    """指数日K存储：真实指数行情（v_index_daily），无涨跌停/ST 概念"""

    def __init__(self, client, cache_dir, universe):
        self._client = client
        self._cache_dir = os.path.join(os.path.abspath(os.path.expanduser(cache_dir)), "index")
        self._universe = universe
        self._bars: Dict[str, np.ndarray] = {}

    def get_bars(self, order_book_id: str) -> np.ndarray:
        code = order_book_id.split(".")[0]
        bars = self._bars.get(code)
        if bars is not None:
            return bars
        bars = self._load(code)
        self._bars[code] = bars
        return bars

    def _cache_path(self, code: str) -> str:
        return os.path.join(self._cache_dir, code + ".npy")

    def _load(self, code: str) -> np.ndarray:
        path = self._cache_path(code)
        if os.path.exists(path):
            try:
                arr = np.load(path, mmap_mode="r")
                if len(arr) and int(arr["datetime"][-1]) // 1000000 >= self._universe.index_last_of(code):
                    return arr
                del arr
            except (OSError, ValueError) as e:
                system_log.warning("quantdb index cache load failed for {}: {}".format(code, e))
        rows = self._client.load_index_symbol(self._universe.index_symbol_of(code))
        arr = self._build(rows)
        _atomic_save_npy(path, arr)
        return arr

    @staticmethod
    def _build(rows: List[tuple]) -> np.ndarray:
        n = len(rows)
        arr = np.empty(n, dtype=CS_DTYPE)
        if n == 0:
            return arr
        raw = np.array(rows, dtype=DAY_RAW_DTYPE)
        arr["datetime"] = raw["dt"] * 1000000
        arr["open"] = raw["open"]
        arr["close"] = raw["close"]
        arr["high"] = raw["high"]
        arr["low"] = raw["low"]
        arr["volume"] = raw["volume"]
        arr["total_turnover"] = raw["amount"] * 10000.0
        arr["limit_up"] = np.nan
        arr["limit_down"] = np.nan
        arr["pre_close"] = np.nan
        arr["is_st"] = False
        return arr


class QuantdbMinuteStore(object):
    """分钟K存储：按 (code, 交易日) 提供数据。

    磁盘缓存/内存持有完整 241 根（含 09:30 竞价 bar）；
    get_day 返回时剥掉 09:30（RQAlpha 分钟时钟从 09:31 开始，
    不剥的话 history_bars(include_now=False) 在 09:31 会把竞价 bar
    当成"上一分钟"），竞价 bar 由 get_auction 单独暴露给
    get_open_auction_bar 使用。

    缓存未命中时一次拉取该标的全部分钟历史（单文件扫描），按日切片落盘。
    """

    def __init__(self, client, cache_dir, day_bar_store, universe, mem_days=512):
        self._client = client
        self._cache_dir = os.path.join(os.path.abspath(os.path.expanduser(cache_dir)), "minute")
        self._day_bar_store = day_bar_store
        self._universe = universe
        self._mem_days = int(mem_days)
        self._mem: Dict[Tuple[str, int], np.ndarray] = {}
        self._loaded_full = set()  # 已做过整历史加载的代码
        self._empty = set()        # 已确认无数据的 (code, date)

    def get_day(self, code: str, date_int: int) -> np.ndarray:
        """当日分钟 bar（不含 09:30 竞价）"""
        arr = self._load_full(code, date_int)
        return self._strip_auction(arr)

    def get_auction(self, code: str, date_int: int) -> Optional[np.ndarray]:
        """当日 09:30 竞价 bar（无则 None）"""
        arr = self._load_full(code, date_int)
        if len(arr) and int(arr["datetime"][0]) % 1000000 == 93000:
            return arr[0]
        return None

    @staticmethod
    def _strip_auction(arr: np.ndarray) -> np.ndarray:
        if len(arr) and int(arr["datetime"][0]) % 1000000 == 93000:
            return arr[1:]
        return arr

    def _load_full(self, code: str, date_int: int) -> np.ndarray:
        key = (code, date_int)
        arr = self._mem.get(key)
        if arr is not None:
            return arr
        if key in self._empty:
            return np.empty(0, dtype=CS_DTYPE)
        path = os.path.join(self._cache_dir, code, "{}.npy".format(date_int))
        if not os.path.exists(path) and code not in self._loaded_full:
            self._load_full_history(code)
        if os.path.exists(path):
            try:
                arr = np.load(path, mmap_mode="r")
                self._mem_put(key, arr)
                return arr
            except (OSError, ValueError):
                pass
        rows = self._client.load_minute_day(self._universe.symbol_of(code), date_int)
        if not rows:
            self._empty.add(key)
            return np.empty(0, dtype=CS_DTYPE)
        arr = self._build(code, date_int, rows)
        self._mem_put(key, arr)
        _atomic_save_npy(path, arr)
        return arr

    def _mem_put(self, key: Tuple[str, int], arr: np.ndarray):
        self._mem[key] = arr
        while len(self._mem) > self._mem_days:
            self._mem.pop(next(iter(self._mem)))

    def _load_full_history(self, code: str):
        """单文件拉取该标的全部分钟K，按交易日切片缓存"""
        self._loaded_full.add(code)
        rows = self._client.load_minute_all(self._universe.symbol_of(code))
        if not rows:
            return
        by_day: Dict[int, List[tuple]] = {}
        for r in rows:
            by_day.setdefault(int(r[0]) // 1000000, []).append(r)
        for date_int, day_rows in by_day.items():
            path = os.path.join(self._cache_dir, code, "{}.npy".format(date_int))
            if os.path.exists(path):
                continue
            arr = self._build(code, date_int, day_rows)
            self._mem_put((code, date_int), arr)
            _atomic_save_npy(path, arr)

    def _build(self, code: str, date_int: int, rows: List[tuple]) -> np.ndarray:
        n = len(rows)
        arr = np.empty(n, dtype=CS_DTYPE)
        if n == 0:
            return arr
        raw = np.array(rows, dtype=MINUTE_RAW_DTYPE)
        # 涨跌停基准价与 ST 标志取自当日日K
        pre_close, is_st, limit_up, limit_down = self._day_info(code, date_int)
        ratio = _stock_limit_ratio(code, bool(is_st))
        up = _limit_price(pre_close, ratio, True)
        down = _limit_price(pre_close, ratio, False)
        arr["datetime"] = raw["ts"]
        arr["open"] = raw["open"]
        arr["close"] = raw["close"]
        arr["high"] = raw["high"]
        arr["low"] = raw["low"]
        arr["volume"] = raw["volume"]
        # 分钟 turnover 单位已是元
        arr["total_turnover"] = raw["turnover"]
        arr["limit_up"] = up
        arr["limit_down"] = down
        arr["pre_close"] = pre_close
        arr["is_st"] = bool(is_st)
        return arr

    def _day_info(self, code: str, date_int: int) -> Tuple[float, bool, float, float]:
        """返回 (当日日K pre_close, is_st, limit_up, limit_down)"""
        bars = self._day_bar_store.get_bars(self._universe.code_to_order_book_id_safe(code))
        if len(bars) == 0:
            return np.nan, False, np.nan, np.nan
        pos = int(bars["datetime"].searchsorted(np.int64(date_int * 1000000)))
        if pos < len(bars) and int(bars["datetime"][pos]) == date_int * 1000000:
            return (float(bars["pre_close"][pos]), bool(bars["is_st"][pos]),
                    float(bars["limit_up"][pos]), float(bars["limit_down"][pos]))
        return np.nan, False, np.nan, np.nan


class QuantdbDividendStore(AbstractDividendStore):
    """分红存储：interest 为每 10 股现金（税前），round_lot=10。

    quantdb 只有除权日，登记日/公告日/派息日均近似为除权日（当天入账）。
    """

    def __init__(self, loader: CorpActionLoader):
        self._loader = loader

    @lru_cache(1024)
    def get_dividend(self, order_book_id: str) -> Optional[np.ndarray]:
        events = self._loader.get(_symbol_of_loader(order_book_id))
        events = events[events["interest"] > 0]
        if len(events) == 0:
            return None
        arr = np.zeros(len(events), dtype=[
            ('book_closure_date', '<i8'), ('announcement_date', '<i8'),
            ('dividend_cash_before_tax', '<f8'), ('ex_dividend_date', '<i8'),
            ('payable_date', '<i8'), ('round_lot', '<f8'),
        ])
        for i, e in enumerate(events):
            # dividend_cash_before_tax 与 round_lot 同口径（每 10 股），
            # rqalpha 按两者相除得到每股现金
            arr[i] = (e["date"], e["date"], e["interest"], e["date"], e["date"], 10.0)
        return arr


class QuantdbSplitStore(AbstractSimpleFactorStore):
    """拆股存储：stock_bonus 为每 10 股送转数；配股(allotment)不计入。

    rqalpha position_model 会把 ex_date // 1000000 后比较，
    因此这里存 YYYYMMDDHHMMSS（date_int * 1e6）。
    """

    def __init__(self, loader: CorpActionLoader):
        self._loader = loader

    @lru_cache(1024)
    def get_factors(self, order_book_id: str) -> Optional[np.ndarray]:
        events = self._loader.get(_symbol_of_loader(order_book_id))
        events = events[events["stock_bonus"] > 0]
        if len(events) == 0:
            return None
        arr = np.zeros(len(events), dtype=[
            ('ex_date', '<i8'), ('split_factor', '<f8'),
            ('split_coefficient_to', '<f8'), ('split_coefficient_from', '<f8'),
        ])
        for i, e in enumerate(events):
            to = 10.0 + e["stock_bonus"]
            arr[i] = (e["date"] * 1000000, to / 10.0, to, 10.0)
        return arr


class QuantdbExFactorStore(AbstractSimpleFactorStore):
    """累积复权因子存储：factor = 后复权收盘/不复权收盘（变化点）。

    BaseDataSource.get_ex_cum_factor 会按上市区间过滤并在首条
    start_date != 0 时自动补 (0, 1.0)。
    """

    def __init__(self, loader: FactorLoader):
        self._loader = loader

    @lru_cache(1024)
    def get_factors(self, order_book_id: str) -> Optional[np.ndarray]:
        changes = self._loader.get(_symbol_of_loader(order_book_id))
        if len(changes) == 0:
            return None
        arr = np.zeros(len(changes), dtype=[
            ('start_date', '<i8'), ('ex_cum_factor', '<f8'),
        ])
        for i, e in enumerate(changes):
            arr[i] = (e["date"] * 1000000, e["factor"])
        return arr


def _symbol_of_loader(order_book_id: str) -> str:
    """order_book_id -> quantdb symbol（loader 以 quantdb symbol 为键）"""
    return order_book_id_to_symbol(order_book_id)


class QuantdbCalendarStore(AbstractCalendarStore):
    def __init__(self, calendar: np.ndarray):
        self._calendar = calendar  # np.ndarray int64 YYYYMMDD

    def get_trading_calendar(self) -> pd.DatetimeIndex:
        return pd.to_datetime([str(int(d)) for d in self._calendar])


class QuantdbSuspendDateSet(AbstractDateSet):
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

    def contains(self, order_book_id: str, dates) -> Optional[List[bool]]:
        days = self._bar_dates(order_book_id.split(".")[0])
        if days is None:
            return None
        trading_dates, first, last = days
        result = []
        for d in dates:
            di = _to_date_int(d)
            result.append(first <= di <= last and di not in trading_dates)
        return result


class QuantdbSTDateSet(DateSet):
    """ST 判定：基于日K记录中的 is_st 标志（来自 v_features_daily 历史逐日值）"""

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

    def contains(self, order_book_id: str, dates) -> Optional[List[bool]]:
        days = self._st_dates(order_book_id.split(".")[0])
        if days is None:
            return None
        return [(_to_date_int(d) in days) for d in dates]


class NullShareTransformation(ShareTransformationStore):
    """无换股合并信息"""

    def __init__(self):
        pass

    def get_share_transformation(self, order_book_id):
        return None
