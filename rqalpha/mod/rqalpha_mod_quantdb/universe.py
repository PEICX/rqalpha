# -*- coding: utf-8 -*-
# 从 quantdb 构建证券全集与交易日历
#
# 标的范围（第一阶段）：
#   CS   = v_daily_unadjusted 的 5,574 只股票（当前上市全集，无历史退市股 -> 生存偏差）
#   INDX = v_index_daily 的 46 个真实指数（benchmark 用，不再用 ETF 代理）
# 交易日历 = SELECT DISTINCT dt FROM v_daily_unadjusted（2016-01-04 ~ 2026-09-30）；
#   不用 v_calendar（只到 2026-09-28）。
# listed_date = v_features_daily.list_date（真实上市日，如 600519 -> 2001-08-27），
#   缺失时回退 meta_symbols.listing_date，再回退首根日线日期。
#
# 结果缓存到 ~/.rqalpha/quantdb_cache/universe.json；新鲜度用数据探测判断
# （探测到的日线最大日期 > 缓存记录时才重建），数据快照不更新就不会反复重扫。

import json
import os
from typing import Dict, List, Optional

import numpy as np

from rqalpha.utils.logger import system_log

CACHE_VERSION = 1
ACTIVE_DATE = "2999-12-31"

# quantdb symbol 后缀 -> rqalpha exchange
EXCHANGE_MAP = {"SH": "XSHG", "SZ": "XSHE", "BJ": "BJSE"}
# rqalpha exchange -> quantdb symbol 后缀
EXCHANGE_REVERSE = {"XSHG": "SH", "XSHE": "SZ", "BJSE": "BJ"}

def symbol_to_order_book_id(symbol: str) -> str:
    # "600519.SH" -> "600519.XSHG"
    code, _, suffix = symbol.partition(".")
    return "{}.{}".format(code, EXCHANGE_MAP.get(suffix, "XSHG"))


def order_book_id_to_symbol(order_book_id: str) -> str:
    # "600519.XSHG" -> "600519.SH"（未知后缀按前缀推断）
    code, _, suffix = order_book_id.partition(".")
    if suffix in EXCHANGE_REVERSE:
        return "{}.{}".format(code, EXCHANGE_REVERSE[suffix])
    return "{}.{}".format(code, _exchange_of_code(code))


def code_to_order_book_id(code: str) -> str:
    """6位代码 -> order_book_id（按代码段推断交易所）"""
    return "{}.{}".format(code, _exchange_of_code(code))


def code_to_symbol(code: str) -> str:
    """6位代码 -> quantdb symbol"""
    return "{}.{}".format(code, _exchange_of_code(code))


def _exchange_of_code(code: str) -> str:
    if code.startswith(("6", "5", "9")):
        return "XSHG"
    if code.startswith("920") or code.startswith(("8", "4")):
        return "BJSE"
    return "XSHE"


def _board_type(code: str) -> str:
    if code.startswith(("688", "689")):
        return "KSH"
    if code.startswith(("300", "301", "302")):
        return "GEM"
    if code.startswith("92") or code.startswith(("8", "4")):
        return "BJS"
    return "MainBoard"


def _stock_limit_ratio(code: str, is_st: bool) -> float:
    """涨跌停幅度：科创/创业板 20%，北交所 30%，ST 5%（科创/创业 ST 仍 20%），主板 10%"""
    if code.startswith(("300", "301", "302", "688", "689")):
        return 0.2
    if code.startswith("92") or code.startswith(("8", "4")):
        return 0.3
    if is_st:
        return 0.05
    return 0.1


def _to_datestr(date_int: int) -> str:
    s = str(int(date_int))
    return "{}-{}-{}".format(s[:4], s[4:6], s[6:8])


class Universe(object):
    """证券全集 + 交易日历，含磁盘缓存（多进程原子写）"""

    def __init__(self, client, cache_dir: str):
        self._client = client
        self._cache_dir = os.path.abspath(os.path.expanduser(cache_dir))
        self._cache_file = os.path.join(self._cache_dir, "universe.json")
        # code(6位) -> {"symbol","name","first","last","list_date"}
        self._meta: Dict[str, dict] = {}
        # 指数 code(6位) -> {"symbol","category","first","last"}
        self._indexes: Dict[str, dict] = {}
        self._calendar: Optional[np.ndarray] = None  # int64 YYYYMMDD 升序
        self._data_last = 0  # 构建缓存时的日线最大日期

    # ---------- 缓存 ----------

    def _load_cache(self, data_last: int) -> bool:
        if not os.path.exists(self._cache_file):
            return False
        try:
            with open(self._cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("version") != CACHE_VERSION or int(data.get("data_last", 0)) < data_last:
                return False
            self._meta = data.get("meta", {})
            self._indexes = data.get("indexes", {})
            self._calendar = np.array(data.get("calendar", []), dtype=np.int64)
            self._data_last = int(data.get("data_last", 0))
            return bool(self._meta and self._calendar is not None and len(self._calendar))
        except (OSError, ValueError) as e:
            system_log.warning("quantdb universe cache load failed: {}".format(e))
            return False

    def _save_cache(self):
        # 并发安全: 每进程写独立临时文件 + 原子 replace
        assert self._calendar is not None
        os.makedirs(self._cache_dir, exist_ok=True)
        tmp = "{}.{}.tmp".format(self._cache_file, os.getpid())
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"version": CACHE_VERSION, "data_last": self._data_last,
                           "meta": self._meta, "indexes": self._indexes,
                           "calendar": [int(d) for d in self._calendar]},
                          f, ensure_ascii=False, separators=(",", ":"))
            os.replace(tmp, self._cache_file)
        except OSError as e:
            system_log.warning("quantdb universe cache save skipped: {}".format(e))
            try:
                os.remove(tmp)
            except OSError:
                pass

    # ---------- 构建 ----------

    def build(self):
        if self._meta:
            return
        data_last = self._client.probe_daily_max()
        if self._load_cache(data_last):
            return
        system_log.info("quantdb: building universe (full scan) ...")
        names = self._client.load_names()
        day_meta = self._client.load_day_meta()
        list_dates = self._client.load_list_dates()
        index_meta = self._client.load_index_meta()
        calendar = self._client.load_calendar()

        self._meta = {}
        for symbol, (first, last) in day_meta.items():
            code = symbol.split(".")[0]
            # v_features_daily.list_date 有少量脏值（如 '0'），非法时回退首根日线
            list_date = list_dates.get(symbol) or ""
            if not (len(list_date) == 8 and list_date.isdigit() and list_date >= "19000101"):
                list_date = str(first)
            self._meta[code] = {
                "symbol": symbol,
                "name": names.get(symbol, ""),
                "first": int(first),
                "last": int(last),
                "list_date": list_date,
            }
        self._indexes = {}
        for symbol, (first, last, category) in index_meta.items():
            code = symbol.split(".")[0]
            self._indexes[code] = {
                "symbol": symbol,
                "category": category,
                "first": int(first),
                "last": int(last),
            }
        self._calendar = np.array(calendar, dtype=np.int64)
        self._data_last = data_last
        self._save_cache()

    def load_calendar(self) -> np.ndarray:
        if self._calendar is None:
            self.build()
        assert self._calendar is not None
        return self._calendar

    # ---------- 输出 ----------

    def instrument_dicts(self):
        dicts = []
        for code, info in self._meta.items():
            listed = _to_datestr(int(info["list_date"]))
            dicts.append({
                "order_book_id": symbol_to_order_book_id(info["symbol"]),
                "symbol": info["name"] or code,
                "round_lot": 100,
                "listed_date": listed,
                "de_listed_date": ACTIVE_DATE,
                "type": "CS",
                "exchange": EXCHANGE_MAP.get(info["symbol"].partition(".")[2], "XSHE"),
                "board_type": _board_type(code),
                "market_tplus": 1,
                "status": "Active",
            })
        for code, info in self._indexes.items():
            suffix = info["symbol"].partition(".")[2]
            dicts.append({
                "order_book_id": symbol_to_order_book_id(info["symbol"]),
                "symbol": "{}({})".format(code, info["category"] or "指数"),
                "round_lot": 100,
                "listed_date": "1990-01-01",
                "de_listed_date": ACTIVE_DATE,
                "type": "INDX",
                "exchange": EXCHANGE_MAP.get(suffix, "XSHG"),
                "market_tplus": 1,
                "status": "Active",
            })
        return dicts

    # ---------- 查询辅助 ----------

    def symbol_of(self, code: str) -> str:
        info = self._meta.get(code)
        return info["symbol"] if info else code_to_symbol(code)

    def index_symbol_of(self, code: str) -> str:
        info = self._indexes.get(code)
        return info["symbol"] if info else ""

    def name_of(self, code: str) -> str:
        info = self._meta.get(code)
        return info["name"] if info else ""

    def last_of(self, code: str) -> int:
        info = self._meta.get(code)
        return int(info["last"]) if info else 0

    def index_last_of(self, code: str) -> int:
        info = self._indexes.get(code)
        return int(info["last"]) if info else 0

    def codes(self) -> List[str]:
        return sorted(self._meta.keys())

    def index_codes(self) -> List[str]:
        return sorted(self._indexes.keys())

    def has_code(self, code: str) -> bool:
        return code in self._meta

    def is_index_code(self, code: str) -> bool:
        return code in self._indexes

    def code_to_order_book_id_safe(self, code: str) -> str:
        """代码 -> order_book_id（指数优先精确匹配，其余按代码段推断）"""
        code = str(code).split(".")[0]
        if code in self._indexes:
            return symbol_to_order_book_id(self._indexes[code]["symbol"])
        if code in self._meta:
            return symbol_to_order_book_id(self._meta[code]["symbol"])
        return code_to_order_book_id(code)
