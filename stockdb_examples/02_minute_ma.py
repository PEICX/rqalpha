# -*- coding: utf-8 -*-
"""策略2: 分钟线回测（510300 沪深300ETF, 1m 频率, 双均线）
独立验证: 用 stockdb 分钟数据手算信号对比 trades
"""
import pickle
import sys
from bisect import bisect_right

sys.path.insert(0, "/root/pcx/rqalpha-release-6.5.0")
sys.path.insert(0, "/root/pcx/stockdb/pybao")

from rqalpha import run_code

CODE = '''
def init(context):
    context.s = "510300.XSHG"

def handle_bar(context, bar_dict):
    prices = history_bars(context.s, 32, '1m', 'close')
    if prices is None or len(prices) < 32:
        return
    ma_s = prices[-5:].mean()
    ma_l = prices[-30:].mean()
    pos = context.portfolio.positions.get(context.s)
    held = pos is not None and pos.quantity > 0
    if ma_s > ma_l and not held:
        order_target_percent(context.s, 0.95)
    elif ma_s < ma_l and held:
        order_target_percent(context.s, 0)
'''


def main():
    config = {
        "base": {
            "start_date": "2026-09-07",
            "end_date": "2026-09-11",
            "frequency": "1m",
            "accounts": {"stock": 100000},
        },
        "mod": {
            "stockdb": {
                "enabled": True,
                "lib": "rqalpha.mod.rqalpha_mod_stockdb",
                "server_dir": "/root/pcx/stockdb",
            },
            "sys_analyser": {
                "enabled": True,
                "benchmark": None,
                "output_file": "/tmp/stockdb_test/out_s2.pkl",
            },
        },
    }
    result = run_code(CODE, config)
    summary = result["sys_analyser"]["summary"]
    print("total_returns =", summary["total_returns"])

    # ---- 独立验证 ----
    with open("/tmp/stockdb_test/out_s2.pkl", "rb") as f:
        r = pickle.load(f)
    trades_df = r["trades"]
    print("trades:", len(trades_df))
    from rqalpha.mod.rqalpha_mod_stockdb.client import StockdbClient
    c = StockdbClient("127.0.0.1", 80, timeout=10)

    # 分钟收盘字典（含回测起始日前一交易日，供预热均线）
    # rqalpha skip_suspended=True 过滤 volume==0 的分钟，保持一致
    mcloses = {}
    for d in [20260904, 20260907, 20260908, 20260909, 20260910, 20260911]:
        for row in c.get_minute_day("510300", d):
            if float(row[5]) > 0:
                mcloses[int(row[0])] = float(row[2])
    minutes = sorted(mcloses)

    cash, qty = 100000.0, 0
    buy_day = None  # T+1: 当日买入不可卖
    exp = []
    win_start = 20260907000000  # 只在回测窗口内交易
    for m in minutes:
        if m < win_start:
            continue
        # rqalpha 分钟事件网格为 09:31-11:30 / 13:01-15:00，无 09:30/13:00 事件
        tod = m % 1000000
        if tod in (93000, 130000):
            continue
        # rqalpha include_now=False: 在 m 分钟下单时只看 m 之前的数据
        # 与策略一致：不足32根不产生信号
        hist = [mcloses[x] for x in minutes if x < m]
        if len(hist) < 32:
            continue
        ma5, ma30 = sum(hist[-5:]) / 5, sum(hist[-30:]) / 30
        px = mcloses[m]
        day = m // 1000000
        if ma5 > ma30 and qty == 0:
            n = int(cash * 0.95 / px / 100) * 100
            if n:
                qty += n; cash -= n * px * 1.0001; buy_day = day; exp.append((m, n, px))
        elif ma5 < ma30 and qty > 0 and day > buy_day:
            cash += qty * px * 0.9999; exp.append((m, qty, px)); qty = 0

    exp_m = [e[0] for e in exp]
    bt_m = [int(str(t["datetime"]).replace("-", "").replace(":", "").replace(" ", "")[:14])
            for _, t in trades_df.iterrows()]
    print("分钟信号日期序列一致:", exp_m == bt_m, "exp:", len(exp_m), "bt:", len(bt_m))
    if exp_m != bt_m:
        for e, b in zip(exp_m, bt_m):
            if e != b:
                print("  首个不一致: 期望", e, "实际", b)
                break
    ok_px = all(abs(float(t["last_price"]) - px) < 1e-6 for (_, _, px), (_, t) in zip(exp, trades_df.iterrows()))
    print("成交价一致:", ok_px)
    assert exp_m == bt_m, "minute signal mismatch"
    assert ok_px, "minute price mismatch"
    print("=== 策略2 分钟线验证通过 ===")


if __name__ == "__main__":
    main()
