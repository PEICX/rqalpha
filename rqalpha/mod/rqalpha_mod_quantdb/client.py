# -*- coding: utf-8 -*-
# quantdb DuckDB 只读访问层
#
# catalog.duckdb 里注册了全部视图（日线/指数/复权因子/分红/特征/财务/分钟），
# 这里封装 mod 需要的取数接口：
#   - 探测查询（数据最新日期）用于缓存新鲜度判断，全部走 Parquet 统计信息，亚秒级
#   - 全表扫描（日线按 symbol 分组流式、复权因子变化点、ST 日期）用于预热/建缓存
#   - 单标的查询（日线/分钟/复权因子）用于缓存未命中时按需加载
# 所有 SQL 一律参数绑定，不拼接 symbol 字符串。

import os
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

import duckdb

# 分钟数据根目录（meta_minute_instruments.path 相对于它）
MINUTE_ROOT = "/root/pcx/china-a-share-1min-ohlcv"

# 分钟数据开放起始日（YYYYMMDD）。原始分钟数据从 2010 年起，
# 但 2016 年前没有日线与复权因子，第一阶段不开放。
MINUTE_DATA_START = 20160104

# 一根列名 -> 视图
DAY_COLS = "symbol, dt, open, close, high, low, volume, amount"


class DuckdbClient(object):
    """catalog.duckdb 只读连接（多进程同时只读打开是安全的）"""

    def __init__(self, catalog_path: str, threads: int = 2, memory_limit: str = "2GB"):
        self._path = os.path.abspath(os.path.expanduser(catalog_path))
        if not os.path.exists(self._path):
            raise RuntimeError("quantdb catalog not exist: {}".format(self._path))
        self._con = duckdb.connect(self._path, read_only=True)
        self._con.execute("PRAGMA threads={}".format(max(1, int(threads))))
        self._con.execute("PRAGMA memory_limit='{}'".format(memory_limit))
        self._minute_paths: Dict[str, str] = {}

    def close(self):
        try:
            self._con.close()
        except Exception:
            pass

    # ---------- 探测查询（缓存新鲜度判断） ----------

    def probe_daily_max(self) -> int:
        """日线数据最新交易日（YYYYMMDD int）"""
        row = self._con.execute("SELECT max(dt) FROM v_daily_unadjusted").fetchone()
        return int(row[0]) if row else 0

    def probe_features_max(self) -> int:
        row = self._con.execute("SELECT max(dt) FROM v_features_daily").fetchone()
        return int(row[0]) if row else 0

    def probe_minute_end(self) -> int:
        """最后一个"分钟完整"（全市场最多根数 >= 240）的交易日。

        2026-08-07 只有 52 根（数据集截断），会被跳过，得到 2026-08-06。
        """
        rows = self._con.execute(
            "SELECT date, max(bars) FROM v_minute_daily "
            "WHERE date > (SELECT max(date) - INTERVAL 10 DAY FROM v_minute_daily) "
            "GROUP BY date ORDER BY date"
        ).fetchall()
        result = 0
        for d, bars in rows:
            if bars is not None and bars >= 240:
                result = d.year * 10000 + d.month * 100 + d.day
        if result == 0 and rows:
            d = rows[-1][0]
            result = d.year * 10000 + d.month * 100 + d.day
        return result

    # ---------- 元数据（构建 universe 用） ----------

    def load_names(self) -> Dict[str, str]:
        rows = self._con.execute("SELECT symbol, name FROM v_instruments").fetchall()
        return {r[0]: (r[1] or "") for r in rows}

    def load_day_meta(self) -> Dict[str, Tuple[int, int]]:
        """每只股票日线的 (first_dt, last_dt)"""
        rows = self._con.execute(
            "SELECT symbol, min(dt), max(dt) FROM v_daily_unadjusted GROUP BY symbol"
        ).fetchall()
        return {r[0]: (int(r[1]), int(r[2])) for r in rows}

    def load_list_dates(self) -> Dict[str, str]:
        """真实上市日（YYYYMMDD 字符串）。

        v_features_daily.list_date 优先（覆盖 5226 只），
        meta_symbols.listing_date 兜底（覆盖 323 只，多为 2025-26 新股）。
        """
        rows = self._con.execute(
            "SELECT symbol, max(list_date) FROM v_features_daily "
            "WHERE list_date IS NOT NULL GROUP BY symbol"
        ).fetchall()
        result = {r[0]: str(r[1]) for r in rows}
        rows = self._con.execute(
            "SELECT symbol, strftime(listing_date, '%Y%m%d') FROM meta_symbols "
            "WHERE listing_date IS NOT NULL"
        ).fetchall()
        for r in rows:
            result.setdefault(r[0], str(r[1]))
        return result

    def load_index_meta(self) -> Dict[str, Tuple[int, int, str]]:
        """每个指数的 (first_dt, last_dt, category)"""
        rows = self._con.execute(
            "SELECT symbol, min(dt), max(dt), any_value(category) "
            "FROM v_index_daily GROUP BY symbol"
        ).fetchall()
        return {r[0]: (int(r[1]), int(r[2]), r[3] or "") for r in rows}

    def load_calendar(self) -> List[int]:
        """交易日历（升序 YYYYMMDD）。

        v_calendar 只到 2026-09-28 而日线到 09-30，因此从日线表 DISTINCT 构建。
        """
        rows = self._con.execute(
            "SELECT DISTINCT dt FROM v_daily_unadjusted ORDER BY dt"
        ).fetchall()
        return [int(r[0]) for r in rows]

    # ---------- 日线 ----------

    def load_day_symbol(self, symbol: str) -> List[tuple]:
        """单只股票全部日线（缓存未命中时按需加载，约 1.4s）"""
        return self._con.execute(
            "SELECT dt, open, close, high, low, volume, amount "
            "FROM v_daily_unadjusted WHERE symbol = ? ORDER BY dt", [symbol]
        ).fetchall()

    def load_day_symbols(self, batch_rows: int = 200000) -> Iterator[Tuple[str, List[tuple]]]:
        """一次全表扫描，按 symbol 分组流式产出 (symbol, rows)。

        避免 pandas 全量内存；ORDER BY symbol, dt 保证组内有序。
        使用独立 cursor：扫描期间主连接上的其他查询（如分红表加载）
        不会顶掉这个流式结果集。
        """
        cur = self._con.cursor()
        cur.execute(
            "SELECT {} FROM v_daily_unadjusted ORDER BY symbol, dt".format(DAY_COLS)
        )
        symbol = None
        rows: List[tuple] = []
        try:
            while True:
                batch = cur.fetchmany(batch_rows)
                if not batch:
                    break
                for row in batch:
                    if row[0] != symbol:
                        if symbol is not None:
                            yield symbol, rows
                        symbol = row[0]
                        rows = []
                    rows.append((row[1], row[2], row[3], row[4], row[5], row[6], row[7]))
            if symbol is not None:
                yield symbol, rows
        finally:
            cur.close()

    def load_index_symbol(self, symbol: str) -> List[tuple]:
        return self._con.execute(
            "SELECT dt, open, close, high, low, volume, amount "
            "FROM v_index_daily WHERE symbol = ? ORDER BY dt", [symbol]
        ).fetchall()

    def load_index_symbols(self) -> Iterator[Tuple[str, List[tuple]]]:
        """全部指数日线，按 symbol 分组（总量 11 万行，一次拉回再分组）"""
        rows = self._con.execute(
            "SELECT {} FROM v_index_daily ORDER BY symbol, dt".format(DAY_COLS)
        ).fetchall()
        by_symbol: Dict[str, List[tuple]] = {}
        for r in rows:
            by_symbol.setdefault(r[0], []).append((r[1], r[2], r[3], r[4], r[5], r[6], r[7]))
        for symbol, group in by_symbol.items():
            yield symbol, group

    # ---------- 复权因子 / 分红送配 / ST ----------

    def load_ex_factor_changes(self) -> Dict[str, List[Tuple[int, float]]]:
        """复权因子变化点（symbol -> [(date_int, factor), ...] 升序）。

        v_adjust_factors 每股每天一行（1076 万行），压缩到因子变化日
        （约 74 万行）后与 rqalpha ex_cum_factor 语义一致：
        factor = 后复权收盘 / 不复权收盘，阶梯递增。
        比较加 1e-9 容差：原始数据存在末位 ULP 抖动（如
        1.4971197048293416 vs 1.4971197048293414），不算真实变化。
        """
        rows = self._con.execute(
            "SELECT symbol, strftime(date, '%Y%m%d')::BIGINT, factor FROM ("
            "  SELECT symbol, date, factor,"
            "         lag(factor) OVER (PARTITION BY symbol ORDER BY date) AS prev"
            "  FROM v_adjust_factors"
            ") WHERE prev IS NULL OR abs(factor - prev) > 1e-9 ORDER BY symbol, date"
        ).fetchall()
        result: Dict[str, List[Tuple[int, float]]] = {}
        for r in rows:
            result.setdefault(r[0], []).append((int(r[1]), float(r[2])))
        return result

    def load_dividends(self) -> Dict[str, List[tuple]]:
        """分红送配事件（symbol -> [(date_int, interest, stock_bonus, allotment), ...]）。

        interest/stock_bonus/allotment 均为每 10 股口径；
        date 为除权除息日。全表 5.8 万行，一次加载。
        """
        rows = self._con.execute(
            "SELECT symbol, strftime(date, '%Y%m%d')::BIGINT, interest, stock_bonus, allotment "
            "FROM v_dividend_factors ORDER BY symbol, date"
        ).fetchall()
        result: Dict[str, List[tuple]] = {}
        for r in rows:
            result.setdefault(r[0], []).append(
                (int(r[1]), float(r[2] or 0.0), float(r[3] or 0.0), float(r[4] or 0.0)))
        return result

    def load_st_dates(self) -> Dict[str, List[int]]:
        """历史 ST 日期（symbol -> [date_int, ...] 升序）。

        v_features_daily.is_st 是逐日历史值（2016 年起每年都有），
        只取 is_st=1 的行（约 24 万行）。
        """
        rows = self._con.execute(
            "SELECT symbol, dt FROM v_features_daily WHERE is_st = 1 ORDER BY symbol, dt"
        ).fetchall()
        result: Dict[str, List[int]] = {}
        for r in rows:
            result.setdefault(r[0], []).append(int(r[1]))
        return result

    # ---------- 分钟线 ----------

    def minute_path(self, symbol: str) -> Optional[str]:
        """该股票分钟 parquet 文件的绝对路径（不存在返回 None）"""
        path = self._minute_paths.get(symbol, "")
        if path:
            return path
        row = self._con.execute(
            "SELECT path FROM meta_minute_instruments WHERE symbol = ?", [symbol]
        ).fetchone()
        if not row or not row[0]:
            self._minute_paths[symbol] = ""
            return None
        path = os.path.join(MINUTE_ROOT, row[0])
        self._minute_paths[symbol] = path
        return path

    def _minute_sql(self, symbol: str, day_filter: bool) -> Tuple[str, list]:
        path = self.minute_path(symbol)
        if path is None:
            return "", []
        sql = ("SELECT strftime(timestamp, '%Y%m%d%H%M%S')::BIGINT, open, high, low, close,"
               " volume, turnover FROM read_parquet(?)")
        params = [path]
        if day_filter:
            sql += " WHERE timestamp >= CAST(? AS TIMESTAMP) AND timestamp < CAST(? AS TIMESTAMP) + INTERVAL 1 DAY"
        return sql, params

    @staticmethod
    def _day_bounds(date_int: int) -> Tuple[str, str]:
        s = str(date_int)
        return "{}-{}-{}".format(s[:4], s[4:6], s[6:8]), "{}-{}-{}".format(s[:4], s[4:6], s[6:8])

    def load_minute_day(self, symbol: str, date_int: int) -> List[tuple]:
        """单个交易日的分钟线（含 09:30 竞价 bar），单文件按日过滤，约 30ms"""
        sql, params = self._minute_sql(symbol, day_filter=True)
        if not sql:
            return []
        lo, hi = self._day_bounds(date_int)
        return self._con.execute(sql, params + [lo, hi]).fetchall()

    def load_minute_all(self, symbol: str, start_int: int = MINUTE_DATA_START) -> List[tuple]:
        """该股票全部分钟历史（单文件，约 1s/百万根）。

        start_int 之后的数据才加载（默认 2016-01-04，更早的没有日线/复权因子配套）。
        """
        sql, params = self._minute_sql(symbol, day_filter=False)
        if not sql:
            return []
        if start_int:
            s = str(int(start_int))
            sql += " WHERE timestamp >= TIMESTAMP '{}-{}-{}'".format(s[:4], s[4:6], s[6:8])
        return self._con.execute(sql, params).fetchall()

    # ---------- 通用查询（策略 API 用） ----------

    def query_df(self, sql: str, params: Optional[Sequence] = None):
        """参数化查询，返回 pandas.DataFrame"""
        return self._con.execute(sql, list(params) if params else None).df()

    def execute(self, sql: str, params: Optional[Sequence] = None):
        return self._con.execute(sql, list(params) if params else None).fetchall()
