# -*- coding: utf-8 -*-
# quantdb 缓存预热脚本：在跑回测/并发之前，把行情数据批量灌入本地磁盘缓存。
#
# 用法:
#   PYTHONPATH=/root/pcx/rqalpha-release-6.5.0 \
#     python3 -m rqalpha.mod.rqalpha_mod_quantdb.prewarm --day            # 全市场日K(约1GB, 2~3分钟)
#   PYTHONPATH=/root/pcx/rqalpha-release-6.5.0 \
#     python3 -m rqalpha.mod.rqalpha_mod_quantdb.prewarm --minute --codes 600519,000001
#   PYTHONPATH=/root/pcx/rqalpha-release-6.5.0 \
#     python3 -m rqalpha.mod.rqalpha_mod_quantdb.prewarm --day --codes 600519   # 少量标的按需
#
# 说明:
#   - --day 全市场走一次全表扫描流式构建（比逐票查询快一个数量级），
#     同时刷新 universe.json / ex_factors.pkl / st_dates.pkl 与 46 个指数日线
#   - --minute 只按 --codes 预热（每只约 60MB，2016-01-04 起），全市场约 300GB+ 不要全量
#   - 预热后回测冷启动不再触发 DuckDB 全表扫描

import argparse
import os
import time


def main():
    parser = argparse.ArgumentParser(description="quantdb 数据缓存预热")
    parser.add_argument("--catalog-path", default="/root/pcx/quantdb/catalog.duckdb")
    parser.add_argument("--cache-dir", default="~/.rqalpha/quantdb_cache")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--day", action="store_true", help="预热日K（全市场或 --codes 指定标的）")
    parser.add_argument("--minute", action="store_true", help="预热分钟K（需 --codes 指定标的）")
    parser.add_argument("--codes", default="", help="逗号分隔 6 位代码，如 600519,000001")
    args = parser.parse_args()

    if not (args.day or args.minute):
        parser.error("至少指定 --day 或 --minute")
    if args.minute and not args.codes:
        parser.error("--minute 需要 --codes 指定标的（全市场分钟约 300GB+，不要全量预热）")

    from .client import DuckdbClient
    from .stores import (CorpActionLoader, FactorLoader, QuantdbDayBarStore,
                         QuantdbIndexDayBarStore, QuantdbMinuteStore, STLoader,
                         _atomic_save_npy)
    from .universe import Universe

    client = DuckdbClient(args.catalog_path, threads=args.threads)
    universe = Universe(client, args.cache_dir)
    universe.build()
    calendar = universe.load_calendar()
    print("universe: {} 只股票, {} 个指数, 日历 {} ~ {}".format(
        len(universe.codes()), len(universe.index_codes()),
        int(calendar[0]), int(calendar[-1])))

    corp_loader = CorpActionLoader(client)
    st_loader = STLoader(client, args.cache_dir)
    factor_loader = FactorLoader(client, args.cache_dir)
    day_store = QuantdbDayBarStore(client, args.cache_dir, universe, corp_loader, st_loader)
    minute_store = QuantdbMinuteStore(client, args.cache_dir, day_store, universe)

    codes = []
    if args.codes:
        codes = [c.strip() for c in args.codes.split(",") if c.strip()]

    if args.day:
        # 复权因子/ST 的全表扫描也顺便落盘（后续进程直接读缓存）
        factor_loader.get("600519.SH")
        st_loader.get("600519.SH")

        if codes:
            t0 = time.time()
            for i, code in enumerate(codes, 1):
                day_store.get_bars(universe.code_to_order_book_id_safe(code))
                print("  day {}/{} {} ({:.1f}s)".format(i, len(codes), code, time.time() - t0))
        else:
            t0 = time.time()
            done = 0
            cache_dir = os.path.join(os.path.abspath(os.path.expanduser(args.cache_dir)), "day")
            for symbol, rows in client.load_day_symbols():
                code = symbol.split(".")[0]
                arr = day_store._build(code, rows)
                _atomic_save_npy(os.path.join(cache_dir, code + ".npy"), arr)
                done += 1
                if done % 500 == 0:
                    print("  day {}/{} 最近: {} 总耗时 {:.0f}s".format(
                        done, len(universe.codes()), symbol, time.time() - t0))
            print("  日K预热完成: {} 只, {:.0f}s".format(done, time.time() - t0))

        # 指数日线（基准用，46 个一次全量拉取）
        t0 = time.time()
        index_cache = os.path.join(os.path.abspath(os.path.expanduser(args.cache_dir)), "index")
        index_store = QuantdbIndexDayBarStore(client, args.cache_dir, universe)
        for symbol, rows in client.load_index_symbols():
            code = symbol.split(".")[0]
            arr = index_store._build(rows)
            _atomic_save_npy(os.path.join(index_cache, code + ".npy"), arr)
        print("  指数预热完成: {} 个, {:.0f}s".format(len(universe.index_codes()), time.time() - t0))

    if args.minute:
        t0 = time.time()
        for i, code in enumerate(codes, 1):
            # 先确保日K就绪（分钟涨跌停基准依赖日K）
            day_store.get_bars(universe.code_to_order_book_id_safe(code))
            minute_store._load_full_history(code)
            print("  minute {}/{} {} ({:.1f}s)".format(i, len(codes), code, time.time() - t0))

    client.close()
    print("预热完成，缓存目录: {}".format(os.path.abspath(os.path.expanduser(args.cache_dir))))


if __name__ == "__main__":
    main()
