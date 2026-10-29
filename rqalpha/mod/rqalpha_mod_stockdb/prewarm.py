# -*- coding: utf-8 -*-
# stockdb 缓存预热脚本：在跑回测/并发之前，把行情数据批量灌入本地磁盘缓存。
#
# 用法:
#   python -m rqalpha.mod.rqalpha_mod_stockdb.prewarm --day --minute \
#       --codes 510300,000001 --start 2025-01-01
#   python -m rqalpha.mod.rqalpha_mod_stockdb.prewarm --day --etf     # 全部ETF日K
#   python -m rqalpha.mod.rqalpha_mod_stockdb.prewarm --day --stocks  # 全部A股日K(~2.5GB)
#   python -m rqalpha.mod.rqalpha_mod_stockdb.prewarm --help
#
# 说明:
#   - 日K全市场约 2.5GB（npy, mmap 共享页缓存, 多进程并发不重复占用）
#   - 分钟K按标的约 20MB/只/全历史；全市场分钟约 150GB，请用 --codes 精确指定
#   - 预热后回测冷启动的 stockdb 串行查询瓶颈即消失

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor


def main():
    parser = argparse.ArgumentParser(description="stockdb 数据缓存预热")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=80)
    parser.add_argument("--server-dir", default="")
    parser.add_argument("--cache-dir", default="~/.rqalpha/stockdb_cache")
    parser.add_argument("--day", action="store_true", help="预热日K")
    parser.add_argument("--minute", action="store_true", help="预热分钟K")
    parser.add_argument("--codes", default="", help="逗号分隔代码，如 510300,000001")
    parser.add_argument("--start", default=None, help="分钟K起始日期 YYYY-MM-DD")
    parser.add_argument("--stocks", action="store_true", help="预热全部A股(日K)")
    parser.add_argument("--etf", action="store_true", help="预热全部ETF")
    parser.add_argument("--workers", type=int, default=6)
    args = parser.parse_args()

    if not (args.day or args.minute):
        parser.error("至少指定 --day 或 --minute")

    from .client import StockdbClient
    from .data_source import StockdbDataSource  # noqa: F401 (确保依赖已导入)
    from .stores import StockdbDayBarStore, StockdbMinuteStore
    from .universe import Universe

    client = StockdbClient(host=args.host, port=args.port,
                           server_dir=args.server_dir, auto_restart=bool(args.server_dir))
    universe = Universe(client, args.cache_dir)
    universe.build()
    calendar = universe.load_calendar()

    codes = []
    if args.codes:
        codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    if args.stocks:
        codes += [code for code in universe._meta if not universe._meta[code]["etf"]]
    if args.etf:
        codes += [code for code in universe._meta if universe._meta[code]["etf"]]
    codes = sorted(set(codes))
    if not codes:
        print("没有要预热的代码")
        return

    loader = None
    day_store = None
    minute_store = None
    if args.day:
        loader = __import__("rqalpha.mod.rqalpha_mod_stockdb.stores", fromlist=["CorpActionLoader"]).CorpActionLoader(client)
        day_store = StockdbDayBarStore(client, args.cache_dir, universe, loader, etf=False)

    def warm_one(code):
        t0 = time.time()
        obid = code + ".XSHG" if code.startswith(("5", "6")) else code + ".XSHE"
        if args.day and day_store is not None:
            day_store.get_bars(obid)
        if args.minute and minute_store is not None:
            if args.start:
                # 指定起始日: 逐日预热该日之后的所有交易日
                start_int = int(args.start.replace("-", ""))
                days = [int(d) for d in calendar if d >= start_int]
                for d in days:
                    minute_store.get_day(code, d)
            else:
                minute_store.get_day(code, 0)  # 触发整历史加载
        return code, time.time() - t0

    # 分钟预热要先建好 day store（分钟涨跌停基准依赖日K）
    if args.minute:
        if loader is None:
            loader = __import__("rqalpha.mod.rqalpha_mod_stockdb.stores", fromlist=["CorpActionLoader"]).CorpActionLoader(client)
        day_store = StockdbDayBarStore(client, args.cache_dir, universe, loader, etf=False)
        minute_store = StockdbMinuteStore(client, args.cache_dir, day_store)

    print("预热 {} 只标的 (day={} minute={}) ...".format(len(codes), args.day, args.minute))
    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for code, dt in pool.map(warm_one, codes):
            done += 1
            if done % 200 == 0 or done == len(codes):
                print("  {}/{} 最近: {} ({:.1f}s) 总耗时 {:.0f}s".format(
                    done, len(codes), code, dt, time.time() - t0))
    print("预热完成，共 {:.0f}s，缓存目录: {}".format(time.time() - t0, args.cache_dir))


if __name__ == "__main__":
    main()
