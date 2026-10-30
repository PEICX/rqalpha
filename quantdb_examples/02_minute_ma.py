# -*- coding: utf-8 -*-
"""quantdb 示例 2: 分钟双均线（000001.XSHE，1m 频率）.

校验点：
    - 每个交易日最多 240 根 bar（09:31~11:30 + 13:01~15:00）
    - 第一根是 09:31，没有 09:30 竞价 bar
    - history_bars(..., '1m') 的收盘价与数据源一致

运行:
    cd /root/pcx/rqalpha-release-6.5.0
    python3 quantdb_examples/02_minute_ma.py
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
    context.long = 30
    context.day = None
    context.day_count = 0
    context.day_first = None
    context.bars_by_day = {}


def handle_bar(context, bar_dict):
    d = context.now.strftime("%Y-%m-%d")
    if d != context.day:
        if context.day is not None:
            context.bars_by_day[context.day] = (context.day_count, context.day_first)
        context.day = d
        context.day_count = 0
        context.day_first = bar_dict[context.s].datetime.strftime("%H:%M")
    context.day_count += 1

    prices = history_bars(context.s, context.long, '1m', 'close', include_now=True)
    if prices is None or len(prices) < context.long:
        return
    ma_s = prices[-context.short:].mean()
    ma_l = prices.mean()
    cur = context.portfolio.positions.get(context.s)
    held = cur is not None and cur.quantity > 0
    if ma_s > ma_l and not held:
        order_target_percent(context.s, 0.5)
    elif ma_s < ma_l and held:
        order_target_percent(context.s, 0)


def after_trading(context):
    context.bars_by_day[context.day] = (context.day_count, context.day_first)
    print(" {}: {} 根, 首根 {}".format(context.day, context.day_count, context.day_first))
'''


def main():
    config = {
        "base": {
            "start_date": "2026-08-03",
            "end_date": "2026-08-06",
            "frequency": "1m",
            "accounts": {"stock": 100000},
        },
        "mod": {
            "quantdb": {
                "enabled": True,
                "lib": "rqalpha.mod.rqalpha_mod_quantdb",
            },
            "stockdb": {"enabled": False},
            "sys_analyser": {"enabled": True},
        },
    }
    result = run_code(CODE, config)["sys_analyser"]

    summary = result["summary"]
    print("total_returns = {}".format(summary["total_returns"]))
    print("trades = {}".format(len(result["trades"])))
    out = "/tmp/quantdb_test"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "out_min.pkl"), "wb") as f:
        pickle.dump(result, f)
    print("结果已写入 {}/out_min.pkl".format(out))


if __name__ == "__main__":
    main()
