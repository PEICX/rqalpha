# -*- coding: utf-8 -*-
# quantdb 策略 API 层：把 quantdb 的扩展数据注入 RQAlpha 策略命名空间
#
# 回测行情走数据源（get_bar/history_bars）；这里的 qd_* 提供行情之外的数据：
#   qd_bars          复权日线 (v_daily_forward/backward/unadjusted)
#   qd_factors       L1/L2/L1+L2 因子 (v_l1_factors / v_l2_factors / v_l1_l2_factors)
#   qd_financial     财务八表 + 分红送配 (v_financial_*，按公告日防未来函数)
#   qd_valuation     估值 (v_valuation)
#   qd_sentiment     盘口情绪 (v_market_sentiment)
#   qd_technical     技术指标 (v_technical_indicators)
#   qd_margin        融资融券 (v_margin)
#   qd_sectors       板块成分 (v_sectors)
#   qd_index_weights 指数权重 (v_index_weights)
#
# 使用建议：不要在 handle_bar 里对 5,000 只票逐只查询；在 init 或盘前
# 批量加载。所有查询按 dt 分区裁剪，务必传 start/end。

import datetime
from typing import Iterable, List, Optional

import pandas as pd

from rqalpha.api import export_as_api
from rqalpha.core.execution_context import ExecutionContext
from rqalpha.const import EXECUTION_PHASE

from .universe import code_to_symbol, order_book_id_to_symbol

_ALL_PHASES = (
    EXECUTION_PHASE.ON_INIT,
    EXECUTION_PHASE.BEFORE_TRADING,
    EXECUTION_PHASE.OPEN_AUCTION,
    EXECUTION_PHASE.ON_BAR,
    EXECUTION_PHASE.AFTER_TRADING,
    EXECUTION_PHASE.SCHEDULED,
    EXECUTION_PHASE.ON_TICK,
)

# 财务表白名单
_FINANCIAL_TABLES = {
    "balance": "v_financial_balance",
    "income": "v_financial_income",
    "cashflow": "v_financial_cashflow",
    "capital": "v_financial_capital",
    "holder_num": "v_financial_holder_num",
    "per_share": "v_financial_per_share",
    "pershare_index": "v_financial_pershare_index",
    "dividend": "v_dividend_factors",
}

# 因子表白名单
_FACTOR_TABLES = {
    "l1": "v_l1_factors",
    "l2": "v_l2_factors",
    "l1l2": "v_l1_l2_factors",
}

_DAILY_TABLES = {
    "none": "v_daily_unadjusted",
    "unadjusted": "v_daily_unadjusted",
    "qfq": "v_daily_forward",
    "forward": "v_daily_forward",
    "hfq": "v_daily_backward",
    "backward": "v_daily_backward",
}

# 按 dt 分区的衍生表（估值/情绪/技术/融资融券）
_DT_PANEL_TABLES = {
    "valuation": "v_valuation",
    "sentiment": "v_market_sentiment",
    "technical": "v_technical_indicators",
    "margin": "v_margin",
}


def _ds():
    from rqalpha.environment import Environment
    return Environment.get_instance().data_source


def _date_int(d) -> Optional[int]:
    if d is None:
        return None
    if isinstance(d, (datetime.date, datetime.datetime)):
        return d.year * 10000 + d.month * 100 + d.day
    return int(str(d)[:10].replace("-", ""))


def _to_qd_symbol(id_or_code) -> str:
    """order_book_id / quantdb symbol / 6位代码 -> quantdb symbol"""
    s = str(id_or_code)
    if "." in s:
        code, suffix = s.split(".", 1)
        if suffix in ("SH", "SZ", "BJ"):
            return s
        if suffix in ("XSHG", "XSHE", "BJSE"):
            return order_book_id_to_symbol(s)
        s = code
    return code_to_symbol(s)


def _to_qd_symbols(id_or_codes) -> Optional[List[str]]:
    if id_or_codes is None:
        return None
    if isinstance(id_or_codes, str):
        return [_to_qd_symbol(id_or_codes)]
    return [_to_qd_symbol(i) for i in id_or_codes]


def _select(columns: Optional[Iterable[str]]) -> str:
    if not columns:
        return "*"
    cols = [str(c) for c in columns]
    if not all(c.replace("_", "").isalnum() for c in cols):
        raise ValueError("columns must be plain identifiers")
    return ", ".join('"{}"'.format(c) for c in cols)


def _dt_range_clauses(start, end) -> tuple:
    clauses, params = [], []
    lo, hi = _date_int(start), _date_int(end)
    if lo is not None:
        clauses.append("dt >= ?")
        params.append(lo)
    if hi is not None:
        clauses.append("dt <= ?")
        params.append(hi)
    return clauses, params


@export_as_api
@ExecutionContext.enforce_phase(*_ALL_PHASES)
def qd_bars(order_book_id, start=None, end=None, adj="qfq", fields=None) -> pd.DataFrame:
    """复权日线，返回 DataFrame（索引 date）。

    :param adj: 'qfq' 前复权 / 'hfq' 后复权 / 'none' 不复权。
        前复权锚定数据集最新交易日（2026-09-30），与 qdata 口径一致；
        若要锚定回测当日请用 history_bars(adjust_type='pre')。
    :param fields: 可选列（open/high/low/close/volume/amount...），默认全部。
        注意 amount 单位为万元，volume 单位为股。
    """
    table = _DAILY_TABLES.get(str(adj).lower())
    if table is None:
        raise ValueError("adj must be 'qfq' / 'hfq' / 'none'")
    clauses, params = _dt_range_clauses(start, end)
    clauses.append("symbol = ?")
    params.append(_to_qd_symbol(order_book_id))
    where = " WHERE " + " AND ".join(clauses)
    df = _ds()._client.query_df(
        "SELECT date, {} FROM {}{} ORDER BY date".format(_select(fields), table, where), params)
    if df.empty:
        return df
    df.index = pd.to_datetime(df.pop("date"))
    df.index.name = "date"
    if "date_1" in df.columns:  # fields 含 date 时 pandas 会重名
        df = df.drop(columns=["date_1"])
    return df


@export_as_api
@ExecutionContext.enforce_phase(*_ALL_PHASES)
def qd_factors(order_book_ids, start=None, end=None, level="l1", fields=None) -> pd.DataFrame:
    """因子面板（L1/L2/L1+L2），返回 DataFrame（索引 date，含 symbol 列）。

    v_l1_l2_factors 从 2018 年起；v_l1_factors 从 2016 年起。
    按 dt 分区裁剪，务必传 start/end；大范围全市场查询建议在 init 里做。
    """
    table = _FACTOR_TABLES.get(str(level).lower())
    if table is None:
        raise ValueError("level must be 'l1' / 'l2' / 'l1l2'")
    clauses, params = _dt_range_clauses(start, end)
    symbols = _to_qd_symbols(order_book_ids)
    if symbols is not None:
        clauses.append("symbol = ANY(?)")
        params.append(symbols)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    df = _ds()._client.query_df(
        "SELECT symbol, date, {} FROM {}{} ORDER BY date, symbol".format(
            _select(fields), table, where), params)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df.set_index("date")


@export_as_api
@ExecutionContext.enforce_phase(*_ALL_PHASES)
def qd_financial(table, order_book_ids=None, as_of=None, latest=False,
                 fields=None) -> pd.DataFrame:
    """财务数据（八表）或分红送配（table='dividend'）。

    :param as_of: 只取公告日（分红为除权日）<= as_of 的记录，防未来函数；
        回测中建议传 context.now。
    :param latest: 每只股票只保留最新一期。
    """
    key = str(table).lower()
    if key not in _FINANCIAL_TABLES:
        raise ValueError("unknown table {!r}; choose from {}".format(
            table, sorted(_FINANCIAL_TABLES)))
    view = _FINANCIAL_TABLES[key]
    is_div = key == "dividend"
    date_col = "date" if is_div else "report_date"
    clauses: List[str] = []
    params: List[object] = []
    symbols = _to_qd_symbols(order_book_ids)
    if symbols is not None:
        clauses.append("symbol = ANY(?)")
        params.append(symbols)
    if as_of is not None:
        clauses.append("{} <= ?".format("date" if is_div else "ann_date"))
        params.append(str(as_of)[:10])
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    cols = _select(fields)
    if latest:
        order = "date DESC" if is_div else "report_date DESC, ann_date DESC"
        q = ("SELECT {cols} FROM (SELECT * EXCLUDE (rn) FROM ("
             "SELECT *, row_number() OVER ("
             "PARTITION BY symbol ORDER BY {order}) AS rn FROM {view}{where}"
             ") WHERE rn = 1) ORDER BY symbol").format(cols=cols, order=order, view=view, where=where)
    else:
        q = "SELECT {} FROM {}{} ORDER BY symbol, {}".format(cols, view, where, date_col)
    return _ds()._client.query_df(q, params)


def _dt_panel_api(table_key, order_book_ids, start, end, fields):
    table = _DT_PANEL_TABLES[table_key]
    clauses, params = _dt_range_clauses(start, end)
    symbols = _to_qd_symbols(order_book_ids)
    if symbols is not None:
        clauses.append("symbol = ANY(?)")
        params.append(symbols)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    df = _ds()._client.query_df(
        "SELECT symbol, date, {} FROM {}{} ORDER BY date, symbol".format(
            _select(fields), table, where), params)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df.set_index("date")


@export_as_api
@ExecutionContext.enforce_phase(*_ALL_PHASES)
def qd_valuation(order_book_ids, start=None, end=None, fields=None) -> pd.DataFrame:
    """估值数据（pe/pb/市值等 16 列），索引 date，含 symbol 列。"""
    return _dt_panel_api("valuation", order_book_ids, start, end, fields)


@export_as_api
@ExecutionContext.enforce_phase(*_ALL_PHASES)
def qd_sentiment(order_book_ids, start=None, end=None, fields=None) -> pd.DataFrame:
    """盘口情绪数据（17 列），索引 date，含 symbol 列。"""
    return _dt_panel_api("sentiment", order_book_ids, start, end, fields)


@export_as_api
@ExecutionContext.enforce_phase(*_ALL_PHASES)
def qd_technical(order_book_ids, start=None, end=None, fields=None) -> pd.DataFrame:
    """技术指标（36 列）。注意 future_return_* 是标签列，不能当特征用。"""
    return _dt_panel_api("technical", order_book_ids, start, end, fields)


@export_as_api
@ExecutionContext.enforce_phase(*_ALL_PHASES)
def qd_margin(order_book_ids, start=None, end=None, fields=None) -> pd.DataFrame:
    """融资融券数据（2016 起），索引 date，含 symbol 列。"""
    return _dt_panel_api("margin", order_book_ids, start, end, fields)


@export_as_api
@ExecutionContext.enforce_phase(*_ALL_PHASES)
def qd_sectors(order_book_id=None, sector_code=None) -> pd.DataFrame:
    """板块成分（快照）。按股票查所属板块或按板块查成分股。

    返回列: sector_code / sector_name / sector_type / symbol
    """
    clauses: List[str] = []
    params: List[object] = []
    if order_book_id is not None:
        clauses.append("symbol = ?")
        params.append(_to_qd_symbol(order_book_id))
    if sector_code is not None:
        codes = [sector_code] if isinstance(sector_code, str) else list(sector_code)
        clauses.append("sector_code = ANY(?)")
        params.append(codes)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return _ds()._client.query_df(
        "SELECT sector_code, sector_name, sector_type, symbol FROM v_sectors{} "
        "ORDER BY sector_code, symbol".format(where), params)


@export_as_api
@ExecutionContext.enforce_phase(*_ALL_PHASES)
def qd_index_weights(index_order_book_id) -> pd.DataFrame:
    """指数成分权重（快照），返回列: symbol / weight（按权重降序）。"""
    return _ds()._client.query_df(
        "SELECT symbol, weight FROM v_index_weights WHERE index_code = ? "
        "ORDER BY weight DESC", [_to_qd_symbol(index_order_book_id)])
