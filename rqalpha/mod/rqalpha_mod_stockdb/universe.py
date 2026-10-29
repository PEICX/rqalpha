# -*- coding: utf-8 -*-
# 从 stockdb 构建证券全集与交易日历
#
# stockdb 没有上市/退市日期字段，采用以下推导方案：
#   listed_date   = 该标的在"日k"表中的首条记录日期
#   de_listed_date= 退市表内标的取"日k"末条记录日期；其余为 2999-12-31
# 交易日历 = 若干长期上市、成交活跃的锚定标的日K日期并集
#
# 结果缓存到本地 JSON，后续运行增量刷新（股票代码/退市表每次重读，
# 仅新代码或新退市的代码才补充首末日期查询）。

import datetime
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor

import numpy as np

from rqalpha.utils.logger import system_log

CACHE_VERSION = 1
ACTIVE_DATE = "2999-12-31"
CALENDAR_ANCHORS = ["000001", "600000", "600009", "600519", "000651", "601398"]

# 常用指数 -> 对应ETF（stockdb 无指数行情，基准指数用 ETF 代替）
DEFAULT_BENCHMARK_ETF_MAP = {
    "000300.XSHG": "510300.XSHG",  # 沪深300
    "000905.XSHG": "510500.XSHG",  # 中证500
    "000016.XSHG": "510050.XSHG",  # 上证50
    "000852.XSHG": "512100.XSHG",  # 中证1000
    "399006.XSHE": "159915.XSHE",  # 创业板指
}


def _to_datestr(date_int):
    d = str(date_int)
    return "{}-{}-{}".format(d[:4], d[4:6], d[6:8])


def code_to_order_book_id(code):
    """6位代码 -> rqalpha order_book_id"""
    if code.startswith(("6", "5")):
        return code + ".XSHG"
    if code.startswith("920"):
        return code + ".BJSE"
    return code + ".XSHE"


def code_to_exchange(code):
    if code.startswith(("6", "5")):
        return "XSHG"
    if code.startswith("920"):
        return "BJSE"
    return "XSHE"


def is_etf_code(code):
    # 沪市: 51x/56x/588；深市: 159
    return bool(re.match(r"^5(1|6|88)\d{4}$", code)) or code.startswith("159")


def _board_type(code):
    if code.startswith(("688", "689")):
        return "KSH"
    if code.startswith(("300", "301")):
        return "GEM"
    return "MainBoard"


def _etf_limit_ratio(name):
    if "科创" in name or "创业" in name or "双创" in name:
        return 0.2
    return 0.1


def _stock_limit_ratio(code, is_st):
    if code.startswith(("300", "301", "688", "689")):
        return 0.2
    if code.startswith("920"):
        return 0.3
    if is_st:
        return 0.05
    return 0.1


class Universe(object):
    """证券全集 + 交易日历，含磁盘缓存"""

    def __init__(self, client, cache_dir, benchmark_etf_map=None, max_workers=8):
        self._client = client
        self._cache_dir = os.path.abspath(os.path.expanduser(cache_dir))
        self._max_workers = max_workers
        self._benchmark_etf_map = dict(DEFAULT_BENCHMARK_ETF_MAP)
        if benchmark_etf_map:
            self._benchmark_etf_map.update(benchmark_etf_map)
        self._cache_file = os.path.join(self._cache_dir, "universe.json")
        self._meta = {}       # code -> {"name":..., "first":int, "last":int, "delisted":bool, "etf":bool}
        self._missing = set()  # 查询过但服务端无数据的代码（避免每次启动重查）
        self._calendar = None  # np.ndarray int64 YYYYMMDD 升序

    # ---------- 缓存 ----------

    def _load_cache(self):
        if self._meta:
            return
        if os.path.exists(self._cache_file):
            try:
                with open(self._cache_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if data.get("version") == CACHE_VERSION:
                    self._meta = data.get("meta", {})
                    self._missing = set(data.get("missing", []))
            except (OSError, ValueError) as e:
                system_log.warning("stockdb universe cache load failed: {}".format(e))
                self._meta = {}
                self._missing = set()

    def _save_cache(self):
        # 并发安全: 每进程写独立临时文件 + 原子 replace；其他进程已先替换时
        # 本次 replace 可能失败，内容等价，忽略即可。
        os.makedirs(self._cache_dir, exist_ok=True)
        tmp = self._cache_file + ".%d.tmp" % os.getpid()
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"version": CACHE_VERSION, "meta": self._meta,
                           "missing": sorted(self._missing)}, f,
                          ensure_ascii=False, separators=(",", ":"))
            os.replace(tmp, self._cache_file)
        except OSError as e:
            system_log.warning("stockdb universe cache save skipped: {}".format(e))
            try:
                os.remove(tmp)
            except OSError:
                pass

    # ---------- 构建 ----------

    def build(self):
        self._load_cache()
        self._load_calendar_cache()
        groups = self._client.get_code_groups()
        delisted = self._client.get_delisted_codes()

        stock_codes = list(groups.get("0", [])) + list(groups.get("3", [])) + list(groups.get("6", []))
        stock_codes += [c for c in groups.get("9", []) if c.startswith("920")]
        etf_codes = [c for c in groups.get("5", []) if is_etf_code(c)]
        etf_codes += [c for c in groups.get("1", []) if c.startswith("159")]
        all_codes = set(stock_codes) | set(etf_codes) | set(delisted)

        todo = []
        for code in all_codes:
            if code in self._missing:
                continue
            info = self._meta.get(code)
            is_delisted = code in delisted
            if info is None or (is_delisted and not info.get("delisted")):
                todo.append(code)

        if todo:
            system_log.info("stockdb: fetching first/last bar dates for {} codes ...".format(len(todo)))
            with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
                results = list(pool.map(self._fetch_meta, todo))
            for code, meta in zip(todo, results):
                if meta is not None:
                    meta["delisted"] = code in delisted
                    meta["etf"] = is_etf_code(code)
                    self._meta[code] = meta
                else:
                    self._missing.add(code)
            self._save_cache()

        # 交易日历
        if not self._is_calendar_fresh():
            self._build_calendar()

    def _fetch_meta(self, code):
        try:
            info = self._client.get_first_last_info(code)
            if info is None:
                return None
            first, last, name = info
        except Exception as e:
            system_log.warning("stockdb: fetch meta failed for {}: {}".format(code, e))
            return None
        return {"name": name, "first": first, "last": last}

    def _load_calendar_cache(self):
        if self._calendar is not None:
            return
        cache_file = os.path.join(self._cache_dir, "calendar.npy")
        if not os.path.exists(cache_file):
            return
        try:
            self._calendar = np.load(cache_file)
        except (OSError, ValueError):
            self._calendar = None

    def _is_calendar_fresh(self):
        if self._calendar is None or len(self._calendar) == 0:
            return False
        cache_file = os.path.join(self._cache_dir, "calendar.npy")
        if not os.path.exists(cache_file):
            return False
        # 数据源一般收盘后才更新；允许 5 个自然日容差，避免"今天还没有K线"
        # 导致每个并发进程都重建日历、互相踩同一个临时文件。
        try:
            last = datetime.datetime.strptime(str(int(self._calendar[-1])), "%Y%m%d").date()
        except ValueError:
            return False
        return last >= datetime.date.today() - datetime.timedelta(days=5)

    def _build_calendar(self):
        dates = set()
        for anchor in CALENDAR_ANCHORS:
            try:
                dates.update(self._client.get_index_dates(anchor))
            except Exception as e:
                system_log.warning("stockdb: calendar anchor {} failed: {}".format(anchor, e))
        if not dates:
            raise RuntimeError("stockdb: failed to build trading calendar")
        self._calendar = np.array(sorted(dates), dtype=np.int64)
        os.makedirs(self._cache_dir, exist_ok=True)
        tmp = os.path.join(self._cache_dir, "calendar.%d.tmp.npy" % os.getpid())
        try:
            np.save(tmp, self._calendar)
            os.replace(tmp, os.path.join(self._cache_dir, "calendar.npy"))
        except OSError as e:
            system_log.warning("stockdb calendar cache save skipped: {}".format(e))
            try:
                os.remove(tmp)
            except OSError:
                pass

    def load_calendar(self):
        """构建（或从磁盘加载）交易日历，返回升序 int64 YYYYMMDD 数组"""
        if self._calendar is not None:
            return self._calendar
        cache_file = os.path.join(self._cache_dir, "calendar.npy")
        if os.path.exists(cache_file):
            self._calendar = np.load(cache_file)
            if self._is_calendar_fresh():
                return self._calendar
        self._build_calendar()
        return self._calendar

    # ---------- 输出 ----------

    def instrument_dicts(self):
        dicts = []
        for code, info in self._meta.items():
            ins_type = "ETF" if info["etf"] else "CS"
            d = {
                "order_book_id": code_to_order_book_id(code),
                "symbol": info["name"],
                "round_lot": 100,
                "listed_date": _to_datestr(info["first"]),
                "de_listed_date": _to_datestr(info["last"]) if info["delisted"] else ACTIVE_DATE,
                "type": ins_type,
                "exchange": code_to_exchange(code),
                "board_type": _board_type(code) if ins_type == "CS" else "MainBoard",
                "market_tplus": 1,
                "status": "Delisted" if info["delisted"] else "Active",
            }
            if ins_type == "ETF":
                d["fund_type"] = "Stock"
            dicts.append(d)
        # 指数 -> ETF 代理（基准指数等）
        etf_meta = {code_to_order_book_id(c): info for c, info in self._meta.items()}
        for index_obid, etf_obid in self._benchmark_etf_map.items():
            etf_code = etf_obid.split(".")[0]
            info = etf_meta.get(etf_obid)
            if info is None:
                system_log.warning("stockdb: benchmark ETF {} not found, skip index {}".format(etf_obid, index_obid))
                continue
            dicts.append({
                "order_book_id": index_obid,
                "symbol": index_obid.split(".")[0] + "(ETF代理)",
                "round_lot": 100,
                "listed_date": _to_datestr(info["first"]),
                "de_listed_date": ACTIVE_DATE,
                "type": "INDX",
                "exchange": index_obid.split(".")[1],
                "market_tplus": 1,
                "status": "Active",
            })
        return dicts

    def refresh_last(self, code, first, last, name):
        """刷新某代码的最新数据日期（服务端有新数据时更新并落盘）。

        build() 增量模式下不会重查存量代码，日K缓存覆盖到 meta.last 后
        由 DayBarStore 调用本方法确认服务端是否已有更新数据。
        """
        meta = self._meta.get(code)
        if meta is None or last <= meta.get("last", 0):
            return
        meta["first"] = first
        meta["last"] = last
        if name:
            meta["name"] = name
        self._save_cache()

    def etf_code_of_index(self, index_obid):
        return self._benchmark_etf_map.get(index_obid)

    def name_of(self, code):
        info = self._meta.get(code)
        return info["name"] if info else ""

    def code_to_order_book_id_safe(self, code):
        """代码 -> order_book_id（不校验是否存在）"""
        return code_to_order_book_id(code)


def _today_int():
    return datetime.date.today().year * 10000 + datetime.date.today().month * 100 + datetime.date.today().day
