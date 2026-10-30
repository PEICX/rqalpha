# -*- coding: utf-8 -*-
"""quantdb 示例 1: 日线双均线（000001.XSHE），真实指数基准 000300.XSHG.

运行:
    cd /root/pcx/rqalpha-release-6.5.0
    python3 quantdb_examples/01_daily_dual_ma.py
"""
import os
import pickle
import sys

sys.path.insert(0, "/root/pcx/rqalpha-release-6.5.0")

from rqalpha import run_code

CODE = '''
def init(context):
    context.s = "000001.XSHE"
    context.short = 5
    context.long = 20


def handle_bar(context, bar_dict):
    prices = history_bars(context.s, context.long + 2, '1d', 'close')
    if prices is None or len(prices) < context.long:
        return
    ma_s = prices[-context.short:].mean()
    ma_l = prices[-context.long:].mean()
    cur = context.portfolio.positions.get(context.s)
    held = cur is not None and cur.quantity > 0
    if ma_s > ma_l and not held:
        order_target_percent(context.s, 0.95)
    elif ma_s < ma_l and held:
        order_target_percent(context.s, 0)
'''


def main():
    config = {
        "base": {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "frequency": "1d",
            "accounts": {"stock": 100000},
        },
        "mod": {
            "quantdb": {
                "enabled": True,
                "lib": "rqalpha.mod.rqalpha_mod_quantdb",
            },
            "stockdb": {"enabled": False},
            "sys_analyser": {
                "enabled": True,
                "benchmark": "000300.XSHG",
            },
        },
    }
    result = run_code(CODE, config)["sys_analyser"]

    summary = result["summary"]
    print("total_returns = {}".format(summary["total_returns"]))
    print("benchmark_total_returns = {}".format(summary["benchmark_total_returns"]))
    print("alpha = {}".format(summary["alpha"]))
    print("beta = {}".format(summary["beta"]))
    print("sharpe = {}".format(summary["sharpe"]))
    print("max_drawdown = {}".format(summary["max_drawdown"]))
    print("trades = {}".format(len(result["trades"])))

    out = "/tmp/quantdb_test"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "out_day.pkl"), "wb") as f:
        pickle.dump(result, f)
    print("结果已写入 {}/out_day.pkl".format(out))


if __name__ == "__main__":
    main()
