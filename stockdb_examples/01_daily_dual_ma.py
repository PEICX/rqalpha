# -*- coding: utf-8 -*-
"""策略1: 日线双均线 + 基准(ETF代理指数)"""
import sys
sys.path.insert(0, "/root/pcx/rqalpha-release-6.5.0")

from rqalpha import run_code

CODE = '''
import datetime


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
            "end_date": "2026-06-30",
            "frequency": "1d",
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
                "benchmark": "000300.XSHG",
                "output_file": "/tmp/stockdb_test/out_s1.pkl",
            },
        },
    }
    result = run_code(CODE, config)
    summary = result["sys_analyser"]["summary"]
    for k in ["total_returns", "annualized_returns", "benchmark_total_returns",
              "sharpe", "max_drawdown", "total_trades"]:
        print(k, "=", summary.get(k))


if __name__ == "__main__":
    main()
