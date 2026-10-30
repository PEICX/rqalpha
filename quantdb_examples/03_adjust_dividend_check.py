# -*- coding: utf-8 -*-
"""quantdb 示例 3: 复权与分红入账校验.

校验点：
    1. 复权：000990.XSHE 2026-07-30 实施 10 送 4。
       history_bars(adjust_type='pre', adjust_orig=回测日) 的隔夜跳变
       应约 +0.2%（不复权约 -28.4%），与 qdata 前复权口径一致。
    2. 分红：600519.XSHG 2026-06-26 除息，每 10 股派 280.24 元（税前）。
       除权日前持有 1000 股，账户现金应增加 280.24 / 10 * 1000 = 28024 元。

运行:
    cd /root/pcx/rqalpha-release-6.5.0
    python3 quantdb_examples/03_adjust_dividend_check.py
"""
import sys

sys.path.insert(0, "/root/pcx/rqalpha-release-6.5.0")

from rqalpha import run_code

CODE = '''
import numpy as np


def init(context):
    context.s = "600519.XSHG"
    context.bought = False
    context.cash_before_dividend = None


def handle_bar(context, bar_dict):
    dt = context.now
    if dt.strftime("%Y-%m-%d") == "2026-06-24" and not context.bought:
        order_shares(context.s, 1000)
        context.bought = True
        return
    if dt.strftime("%Y-%m-%d") == "2026-06-25":
        # 除息日(06-26)前一交易日收盘：记录现金，便于次日核对入账
        context.cash_before_dividend = context.portfolio.cash
        pos = context.portfolio.positions.get(context.s)
        print("持仓: {} 股, 现金: {:.2f}".format(pos.quantity, context.portfolio.cash))
        return
    if dt.strftime("%Y-%m-%d") == "2026-06-26" and context.cash_before_dividend is not None:
        delta = context.portfolio.cash - context.cash_before_dividend
        print("除息日现金变动: {:+.2f} (期望 +28024.00 税前)".format(delta))
        return


def after_trading(context):
    dt = context.now
    if dt.strftime("%Y-%m-%d") in ("2026-07-29", "2026-07-30"):
        s = "000990.XSHE"
        # 策略层 history_bars 的前复权锚点隐式为当前回测时间（adjust_orig=now）
        bars = history_bars(s, 2, "1d", ["datetime", "close"],
                            skip_suspended=False, include_now=True, adjust_type="pre")
        raw = history_bars(s, 2, "1d", "close",
                           skip_suspended=False, include_now=True, adjust_type="none")
        if len(bars) == 2:
            print("{} 前复权: {:.4f} -> {:.4f} ({:+.4%})".format(
                dt.strftime("%m-%d"), bars["close"][0], bars["close"][1],
                bars["close"][1] / bars["close"][0] - 1))
            print("{} 不复权: {:.4f} -> {:.4f} ({:+.4%})".format(
                dt.strftime("%m-%d"), raw[0], raw[1], raw[1] / raw[0] - 1))
'''


def main():
    config = {
        "base": {
            "start_date": "2026-06-22",
            "end_date": "2026-07-30",
            "frequency": "1d",
            "accounts": {"stock": 1500000},
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
    run_code(CODE, config)


if __name__ == "__main__":
    main()
