# -*- coding: utf-8 -*-
"""策略3: 验证 sb_* API 层（估值/资金流/板块/指标）"""
import sys
sys.path.insert(0, "/root/pcx/rqalpha-release-6.5.0")
sys.path.insert(0, "/root/pcx/stockdb/pybao")

import numpy as np
import pandas as pd
from rqalpha import run_code

CODE = '''
import pandas as pd


def init(context):
    context.s = "600633.XSHG"
    # 板块: 股票 -> 申万一级行业
    b = sb_stock_boards(context.s, category=1)
    logger.info("BOARDS\\n" + b.to_string())
    assert len(b) > 0 and b.iloc[0]["name"] == "传媒"
    # 板块 -> 成分股
    m = sb_board_stocks("传媒", category=1)
    assert "600633" in m["symbols"], m
    logger.info("BOARD_STOCKS OK: %d symbols" % len(m["symbols"]))
    # 估值
    v = sb_valuation(context.s, "20260720", "20260727", fields=["pe_ttm", "pb"])
    logger.info("VALUATION\\n" + v.to_string())
    assert len(v) == 6 and v["pe_ttm"].iloc[-1] > 0
    # 资金流
    mf = sb_money_flow(context.s, "20260720", "20260727")
    logger.info("MONEY_FLOW rows=%d main_net@last=%.0f" % (len(mf), mf["main_net"].iloc[-1]))
    assert len(mf) > 0
    # 指标
    ind = sb_indicator("ma,macd", context.s, "20260601", "20260727", n="5,10")
    assert "ma_5" in ind.columns and "dif" in ind.columns
    logger.info("INDICATOR last: ma5=%.3f ma10=%.3f dif=%.4f" % (
        ind["ma_5"].iloc[-1], ind["ma_10"].iloc[-1], ind["dif"].iloc[-1]))
    # ma 数值独立核对
    raw = sb_get_bars(context.s, "20260721", "20260727", fq="qfq")
    assert abs(ind["ma_5"].iloc[-1] - raw["close"].tail(5).mean()) < 1e-9
    logger.info("MA cross-check OK")


def handle_bar(context, bar_dict):
    pass
'''


def main():
    config = {
        "base": {
            "start_date": "2026-07-27",
            "end_date": "2026-07-27",
            "frequency": "1d",
            "accounts": {"stock": 100000},
        },
        "mod": {
            "stockdb": {
                "enabled": True,
                "lib": "rqalpha.mod.rqalpha_mod_stockdb",
                "server_dir": "/root/pcx/stockdb",
            },
            # 用户级配置默认启用了 quantdb，这里显式关闭避免两个数据源互相覆盖
            "quantdb": {"enabled": False},
            "sys_analyser": {"enabled": True, "benchmark": None,
                             "output_file": "/tmp/stockdb_test/out_s3.pkl"},
        },
    }
    run_code(CODE, config)
    print("=== 策略3 API 层验证通过 ===")


if __name__ == "__main__":
    main()
