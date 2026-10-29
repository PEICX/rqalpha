# -*- coding: utf-8 -*-
# stockdb 策略 API 层：把本地数据库能力注入 RQAlpha 策略命名空间
#
# 本地数据（零限制、无封禁风险）:
#   sb_get_bars      复权/原始 K 线 (DataFrame)
#   sb_valuation     估值数据 (pe_ttm/pb/总市值/流通市值/换手率/量比, 来自日K内嵌字段)
#   sb_money_flow    资金流向 (main_net/big_net/mid_net/small_net ...)
#   sb_boards        板块全集 / sb_stock_boards 股票->板块 / sb_board_stocks 板块->成分股
#   sb_indicator     技术指标 (ma/ema/macd/kdj/rsi/boll, 支持金叉死叉信号)
#
# 财务报表、因子值、指数成分等仅在线接口提供（有批量与频率限制），
# 需要在 mod 配置中显式开启 enable_online 才会注入 sb_fundamentals。

import datetime

import numpy as np
import pandas as pd

from rqalpha.api import export_as_api
from rqalpha.core.execution_context import ExecutionContext
from rqalpha.const import EXECUTION_PHASE


def _ds():
    from rqalpha.environment import Environment
    return Environment.get_instance().data_source


def _date_int(d):
    if d is None:
        return None
    if isinstance(d, (datetime.date, datetime.datetime)):
        return d.year * 10000 + d.month * 100 + d.day
    return int(str(d).replace("-", "")[:8])


_ALL_PHASES = (
    EXECUTION_PHASE.ON_INIT,
    EXECUTION_PHASE.BEFORE_TRADING,
    EXECUTION_PHASE.OPEN_AUCTION,
    EXECUTION_PHASE.ON_BAR,
    EXECUTION_PHASE.AFTER_TRADING,
    EXECUTION_PHASE.SCHEDULED,
)


def _apply_fq(code, bars, fq, loader):
    """按复权事件折算价格字段。qfq: price*cum(t)/cum(最新); hfq: price*cum(t)"""
    if bars is None or len(bars) == 0 or fq not in ("qfq", "hfq"):
        return bars
    events = loader.get(code)
    if len(events) == 0:
        return bars
    dates = events["date"]
    cums = events["cum"]
    day_ints = bars["datetime"] // 1000000
    idx = np.searchsorted(dates, day_ints, side="right") - 1
    factors = np.where(idx >= 0, cums[np.maximum(idx, 0)], 1.0)
    ratio = factors if fq == "hfq" else factors / cums[-1]
    bars = np.copy(bars)
    for f in ("open", "close", "high", "low"):
        bars[f] = bars[f] * ratio
    return bars


def _bars_df(code, start_date, end_date, fq, fields):
    from .universe import code_to_order_book_id
    ds = _ds()
    obid = code_to_order_book_id(code)
    store = ds._etf_store if code.startswith(("5", "1")) else ds._cs_store
    bars = store.get_bars(obid)
    lo, hi = _date_int(start_date), _date_int(end_date)
    if lo is not None or hi is not None:
        days = bars["datetime"] // 1000000
        mask = np.ones(len(bars), dtype=bool)
        if lo is not None:
            mask &= days >= lo
        if hi is not None:
            mask &= days <= hi
        bars = bars[mask]
    bars = _apply_fq(code, bars, fq, ds._corp_loader)
    cols = list(fields) if fields else [c for c in bars.dtype.names if c != "is_st"]
    df = pd.DataFrame({c: bars[c] for c in cols})
    df.index = pd.to_datetime([str(int(d // 1000000)) for d in bars["datetime"]])
    df.index.name = "date"
    return df


@export_as_api
@ExecutionContext.enforce_phase(*_ALL_PHASES)
def sb_get_bars(order_book_id, start_date=None, end_date=None, fq="qfq", fields=None):
    """获取本地日K（默认前复权），返回 DataFrame（索引为日期）。

    fields 可选: datetime/open/close/high/low/volume/total_turnover/limit_up/limit_down/pre_close
    """
    code = str(order_book_id).split(".")[0]
    return _bars_df(code, start_date, end_date, fq, fields)


_VALUATION_AP = "date,pe_ttm,pb,total_mv,float_mv,turnover,vol_ratio,pct_chg,amplitude"
_VALUATION_COLS = ["pe_ttm", "pb", "total_mv", "float_mv", "turnover", "vol_ratio", "pct_chg", "amplitude"]


@export_as_api
@ExecutionContext.enforce_phase(*_ALL_PHASES)
def sb_valuation(order_book_id, start_date=None, end_date=None, fields=None):
    """获取本地估值数据（日K内嵌字段），返回 DataFrame（索引为日期）。

    字段: pe_ttm/pb/total_mv/float_mv/turnover/vol_ratio/pct_chg/amplitude
    """
    code = str(order_book_id).split(".")[0]
    lo, hi = _date_int(start_date) or 19900101, _date_int(end_date) or 29991231
    rows = _ds()._client.query("vals", "日k", k1="key:{}".format(code),
                               k2="fwz:{},{}".format(lo, hi), ap=_VALUATION_AP)
    rows = rows if isinstance(rows, list) else []
    df = pd.DataFrame([list(r) for r in rows],
                      columns=["date"] + _VALUATION_COLS)
    df.index = pd.to_datetime([str(int(d)) for d in df.pop("date")])
    df.index.name = "date"
    if fields:
        return df[list(fields)]
    return df


def _range_rows(code, table, ap):
    # 全量 + 内存过滤（估值/资金流行都很小）
    rows = _ds()._client.query("vals", table, k1="key:{}".format(code), k2="all:", ap=ap)
    return rows if isinstance(rows, list) else []


@export_as_api
@ExecutionContext.enforce_phase(*_ALL_PHASES)
def sb_money_flow(order_book_id, start_date=None, end_date=None):
    """获取本地资金流向，返回 DataFrame。

    字段: main_in/main_out/main_net/jumbo_net/big_net/mid_net/small_net/retail_in/retail_out
    """
    code = str(order_book_id).split(".")[0]
    ap = "date,main_in,main_out,main_net,jumbo_net,big_net,mid_net,small_net,retail_in,retail_out"
    rows = _range_rows(code, "资金流", ap)
    lo, hi = _date_int(start_date), _date_int(end_date)
    data = []
    for r in rows:
        d = int(r[0])
        if lo is not None and d < lo:
            continue
        if hi is not None and d > hi:
            continue
        data.append(list(r))
    df = pd.DataFrame(data, columns=["date", "main_in", "main_out", "main_net", "jumbo_net",
                                     "big_net", "mid_net", "small_net", "retail_in", "retail_out"])
    df.index = pd.to_datetime([str(int(d)) for d in df.pop("date")])
    df.index.name = "date"
    return df


@export_as_api
@ExecutionContext.enforce_phase(*_ALL_PHASES)
def sb_boards(category=None):
    """获取板块全集。category: 0/概念, 1/申万一级, 2/申万二级, 3/申万三级; None=全部。

    返回 DataFrame: code/name/source/type/group/category
    """
    rows = _ds()._board_rows()
    cat_map = {0: "概念", 1: "申万一级", 2: "申万二级", 3: "申万三级"}
    want = cat_map.get(category, category) if isinstance(category, int) else category
    data = []
    for _, b in rows:
        if want is not None and b.get("category") != want:
            continue
        data.append([b.get("code"), b.get("name"), b.get("source"), b.get("type"),
                     b.get("group"), b.get("category")])
    return pd.DataFrame(data, columns=["code", "name", "source", "type", "group", "category"])


@export_as_api
@ExecutionContext.enforce_phase(*_ALL_PHASES)
def sb_stock_boards(order_book_id, category=None):
    """查询股票所属板块（可多板块）。category 同 sb_boards。

    返回 DataFrame: code/name/category
    """
    code = str(order_book_id).split(".")[0]
    rows = _ds()._board_rows()
    cat_map = {0: "概念", 1: "申万一级", 2: "申万二级", 3: "申万三级"}
    want = cat_map.get(category, category) if isinstance(category, int) else category
    data = []
    for _, b in rows:
        if want is not None and b.get("category") != want:
            continue
        if code in (b.get("symbols") or []):
            data.append([b.get("code"), b.get("name"), b.get("category")])
    return pd.DataFrame(data, columns=["code", "name", "category"])


@export_as_api
@ExecutionContext.enforce_phase(*_ALL_PHASES)
def sb_board_stocks(query, category=None):
    """按板块名称/代码模糊查询成分股。category 同 sb_boards。

    返回 dict: {板块code: 板块name, 'symbols': [...]}
    """
    rows = _ds()._board_rows()
    cat_map = {0: "概念", 1: "申万一级", 2: "申万二级", 3: "申万三级"}
    want = cat_map.get(category, category) if isinstance(category, int) else category
    q = str(query)
    for _, b in rows:
        if want is not None and b.get("category") != want:
            continue
        if q == b.get("code") or q == b.get("name") or q in b.get("name", ""):
            return {"code": b.get("code"), "name": b.get("name"),
                    "category": b.get("category"), "symbols": list(b.get("symbols") or [])}
    return {}


# ---- 技术指标（纯 numpy 实现） ----

def _ma(arr, n):
    out = np.full(len(arr), np.nan)
    if len(arr) >= n:
        c = np.cumsum(np.insert(arr, 0, 0.0))
        out[n - 1:] = (c[n:] - c[:-n]) / n
    return out


def _ema(arr, n):
    out = np.full(len(arr), np.nan)
    if len(arr):
        a = 2.0 / (n + 1)
        out[0] = arr[0]
        for i in range(1, len(arr)):
            out[i] = arr[i] * a + out[i - 1] * (1 - a)
    return out


def _cross(a, b):
    """金叉=1, 死叉=-1, 无=0"""
    out = np.zeros(len(a), dtype=int)
    for i in range(1, len(a)):
        if np.isfinite(a[i]) and np.isfinite(b[i]):
            if a[i] > b[i] and a[i - 1] <= b[i - 1]:
                out[i] = 1
            elif a[i] < b[i] and a[i - 1] >= b[i - 1]:
                out[i] = -1
    return out


@export_as_api
@ExecutionContext.enforce_phase(*_ALL_PHASES)
def sb_indicator(name, order_book_id, start_date=None, end_date=None, n=None, fq="qfq",
                 cross=False):
    """计算技术指标。name: ma/ema/macd/kdj/rsi/boll（可逗号组合）。

    n: ma/ema 的周期（如 "5,10,20"）；macd 默认 12,26,9；kdj 默认 9,3,3；
    rsi 默认 24；boll 默认 "20,2"。
    cross=True 时附带 {name}_cross 信号列（1金叉/-1死叉/0无）。
    返回 DataFrame（索引为日期）。
    """
    code = str(order_book_id).split(".")[0]
    df = _bars_df(code, start_date, end_date, fq, None)
    close = df["close"].to_numpy(dtype=float)
    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    names = [s.strip() for s in str(name).split(",") if s.strip()]
    periods_arg = None
    if n is not None:
        if isinstance(n, str):
            periods_arg = [int(x) for x in n.split(",") if x.strip()]
        elif isinstance(n, (list, tuple)):
            periods_arg = [int(x) for x in n]
        else:
            periods_arg = [int(n)]

    for i, nm in enumerate(names):
        if nm in ("ma", "ema"):
            periods = periods_arg or [5, 10, 20]
            fn = _ma if nm == "ma" else _ema
            for p in periods:
                df["{}_{}".format(nm, p)] = fn(close, p)
        elif nm == "macd":
            f, s, sig = (12, 26, 9)
            dif = _ema(close, f) - _ema(close, s)
            dea = _ema(np.nan_to_num(dif, nan=0.0), sig)
            df["dif"], df["dea"], df["macd"] = dif, dea, 2 * (dif - dea)
            if cross:
                df["macd_cross"] = _cross(dif, dea)
        elif nm == "kdj":
            k = np.full(len(close), np.nan)
            d = np.full(len(close), np.nan)
            j = np.full(len(close), np.nan)
            prev_k, prev_d = 50.0, 50.0
            for i2 in range(len(close)):
                lo_w = low[max(0, i2 - 8):i2 + 1].min()
                hi_w = high[max(0, i2 - 8):i2 + 1].max()
                rsv = (close[i2] - lo_w) / (hi_w - lo_w) * 100 if hi_w > lo_w else 50.0
                prev_k = 2.0 / 3 * prev_k + 1.0 / 3 * rsv
                prev_d = 2.0 / 3 * prev_d + 1.0 / 3 * prev_k
                k[i2], d[i2], j[i2] = prev_k, prev_d, 3 * prev_k - 2 * prev_d
            df["k"], df["d"], df["j"] = k, d, j
            if cross:
                df["kdj_cross"] = _cross(k, d)
        elif nm == "rsi":
            period = periods_arg[0] if periods_arg else 24
            diff = np.diff(close, prepend=close[0])
            up = np.where(diff > 0, diff, 0.0)
            dn = np.where(diff < 0, -diff, 0.0)
            with np.errstate(invalid="ignore", divide="ignore"):
                rsi = 100 - 100 / (1 + pd.Series(up).ewm(alpha=1.0 / period).mean() /
                                   pd.Series(dn).ewm(alpha=1.0 / period).mean().replace(0, np.nan))
            df["rsi"] = rsi.to_numpy()
        elif nm == "boll":
            period, mult = (20, 2.0)
            if periods_arg:
                period = periods_arg[0]
            mid = _ma(close, period)
            std = pd.Series(close).rolling(period).std().to_numpy()
            df["boll_mid"], df["boll_up"], df["boll_low"] = mid, mid + mult * std, mid - mult * std
        else:
            raise ValueError("sb_indicator: unsupported indicator {}".format(nm))
    return df
