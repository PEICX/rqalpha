# -*- coding: utf-8 -*-
"""quantdb 示例 4: qd_* 策略 API 演示（因子/财务/估值/板块/指数权重）.

演示在策略里直接使用 quantdb 的扩展数据：
    qd_bars          复权日线（前/后/不复权）
    qd_factors       L1/L2/L1+L2 因子面板
    qd_financial     财务数据（按公告日防未来函数）
    qd_valuation     估值（pe/pb/市值）
    qd_margin        融资融券
    qd_sectors       板块成分
    qd_index_weights 指数权重

运行:
    cd /root/pcx/rqalpha-release-6.5.0
    python3 quantdb_examples/04_api_showcase.py
"""
import sys

sys.path.insert(0, "/root/pcx/rqalpha-release-6.5.0")

from rqalpha import run_code

CODE = '''
def init(context):
    context.s = "600519.XSHG"

    # 1) 复权日线（qfq 锚定数据集最新交易日 2026-09-30）
    bars = qd_bars(context.s, "2026-08-20", "2026-08-31", adj="qfq")
    print("qd_bars qfq 尾部:\\n{}".format(bars.tail(2)))

    # 2) 因子面板（l1 从 2016 年起，l1l2 从 2018 年起）
    fac = qd_factors([context.s, "000001.XSHE"], "2026-08-25", "2026-08-31",
                     level="l1", fields=["turn_20", "mom_ret_20d"])
    print("qd_factors:\\n{}".format(fac.tail(3)))

    # 3) 财务（as_of 传 context.now 防未来函数）
    fin = qd_financial("income", [context.s], as_of=context.now, latest=True,
                       fields=["report_date", "ann_date", "revenue",
                               "net_profit_incl_min_int_inc"])
    print("qd_financial income:\\n{}".format(fin))

    # 4) 估值
    val = qd_valuation([context.s], "2026-08-25", "2026-08-31", fields=["pe_ttm", "pb"])
    print("qd_valuation:\\n{}".format(val.tail(2)))

    # 5) 融资融券
    mg = qd_margin([context.s], "2026-08-25", "2026-08-28",
                   fields=["finance_balance", "slo_volume"])
    print("qd_margin:\\n{}".format(mg.tail(2)))

    # 6) 板块 / 指数权重
    sec = qd_sectors(order_book_id=context.s)
    print("qd_sectors(600519) 前5:\\n{}".format(sec.head(5)))
    w = qd_index_weights("000300.XSHG")
    print("qd_index_weights(000300) 前5:\\n{}".format(w.head(5)))


def handle_bar(context, bar_dict):
    pass
'''


def main():
    config = {
        "base": {
            "start_date": "2026-09-01",
            "end_date": "2026-09-04",
            "frequency": "1d",
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
    run_code(CODE, config)


if __name__ == "__main__":
    main()
