# -*- coding: utf-8 -*-
"""策略4: 分红现金核验（除权日持有过 000001 20260612 分红 0.36元/股）
策略5: 涨跌停拒单行为（在 close==limit_up 的日期买入应被拒）
"""
import pickle
import sys

sys.path.insert(0, "/root/pcx/rqalpha-release-6.5.0")

from rqalpha import run_code

# ---- 策略4: 20260610 收盘买入, 持有到 20260617 (跨过 0612 除权) ----
CODE4 = '''
def init(context):
    context.s = "000001.XSHE"
    context.bought = False


def handle_bar(context, bar_dict):
    if not context.bought:
        order_shares(context.s, 5000)
        context.bought = True
'''


def run_s4():
    config = {
        "base": {"start_date": "2026-06-10", "end_date": "2026-06-17",
                 "frequency": "1d", "accounts": {"stock": 100000}},
        "mod": {"stockdb": {"enabled": True, "lib": "rqalpha.mod.rqalpha_mod_stockdb",
                            "server_dir": "/root/pcx/stockdb"},
                "sys_analyser": {"enabled": True, "benchmark": None,
                                 "output_file": "/tmp/stockdb_test/out_s4.pkl"}},
    }
    result = run_code(CODE4, config)
    r = result["sys_analyser"]
    # 找除权日现金变化
    pc = list(r["portfolio"]["cash"])
    dates = [str(d)[:10] for d in r["portfolio"]["total_value"].index]
    print("现金序列:")
    for d, c in zip(dates, pc):
        print("  ", d, round(c, 2))
    # 20260612 当天现金应增加 10000 * 0.36 = 3600 (除权日现金分红入账)
    i612 = dates.index("2026-06-12")
    delta = pc[i612] - pc[i612 - 1] if i612 > 0 else None
    print("0612 现金增量:", round(delta, 2) if delta is not None else None)
    assert delta is not None and abs(delta - 1800.0) < 1.0, \
        "dividend cash delta expected 1800, got {}".format(delta)
    print("=== 策略4 分红现金核验通过 ===")


# ---- 策略5: 涨跌停拒单 ----
# 找一个 close == limit_up 的股票日
def find_limit_up_day():
    from rqalpha.mod.rqalpha_mod_stockdb.client import StockdbClient
    c = StockdbClient("127.0.0.1", 80, timeout=15)
    # 扫描某日全市场 6 开头, close 接近涨停的
    import msgpack, urllib.request, urllib.parse
    for d in ["20260930", "20260925", "20241008", "20240930"]:
        with urllib.request.urlopen(
            "http://127.0.0.1:80/?cmd=vals&t=" + urllib.parse.quote("日k") +
            "&k1=qz:6&k2=key:" + d, timeout=30) as resp:
            rows = msgpack.unpackb(resp.read(), raw=False)
        for r in rows:
            code, pct = str(r.get("code")), r.get("pct_chg")
            if code.startswith("6") and pct is not None and float(pct) >= 9.9:
                return code, d
    return None, None


CODE5 = '''
def init(context):
    context.s = context.s5


def handle_bar(context, bar_dict):
    order_shares(context.s, 1000)
'''


def run_s5():
    code, day = find_limit_up_day()
    print("涨停样本:", code, day)
    assert code, "no limit-up sample found"
    start = "{}-{}-{}".format(day[:4], day[4:6], day[6:8])
    config = {
        "base": {"start_date": start, "end_date": start,
                 "frequency": "1d", "accounts": {"stock": 100000}},
        "mod": {"stockdb": {"enabled": True, "lib": "rqalpha.mod.rqalpha_mod_stockdb",
                            "server_dir": "/root/pcx/stockdb"},
                "sys_analyser": {"enabled": True, "benchmark": None,
                                 "output_file": "/tmp/stockdb_test/out_s5.pkl"}},
    }
    code_src = CODE5.replace("context.s5", repr(code + "." + ("XSHG" if code.startswith("6") else "XSHE")))
    result = run_code(code_src, config)
    r = pickle.load(open("/tmp/stockdb_test/out_s5.pkl", "rb"))
    trades = r["trades"]
    print("涨停日买入成交笔数:", len(trades))
    # 涨停价买入应被拒（无成交）
    assert len(trades) == 0, "order at limit-up should be rejected"
    print("=== 策略5 涨跌停拒单验证通过 ===")


if __name__ == "__main__":
    run_s4()
    run_s5()
