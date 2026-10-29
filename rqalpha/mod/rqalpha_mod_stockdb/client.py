# -*- coding: utf-8 -*-
# stockdb 本地数据库 HTTP 客户端（纯 Python 实现，不依赖 stockdb 原生模块）
#
# 协议：GET /?cmd=get|vals|keys&t=<table>&k1=<expr>&k2=<expr>&num=N&ap=get.f1,f2
# 响应：application/x-msgpack，msgpack.unpackb(raw=False) 解码
#
# 可靠性：请求超时可能意味着本地 stockdb 服务卡死（实测全表扫描会导致服务
# 停止响应），此时自动重启本地服务并重试（需配置 server_dir）。

import os
import socket
import subprocess
import threading
import time

import msgpack
import requests

from rqalpha.utils.logger import system_log

DAY_ROW_AP = "date,open,close,high,low,volume,amount,is_st"
MINUTE_ROW_AP = "date,open,close,high,low,volume,amount"


class StockdbRequestError(Exception):
    pass


class StockdbClient(object):
    def __init__(self, host="127.0.0.1", port=80, timeout=30, server_dir="",
                 auto_restart=None, restart_cooldown=30):
        self._base_url = "http://{}:{}/".format(host, port)
        self._host = host
        self._port = port
        self._timeout = timeout
        self._server_dir = os.path.abspath(os.path.expanduser(server_dir)) if server_dir else ""
        # 默认：配置了 server_dir 才允许自动重启
        self._auto_restart = auto_restart if auto_restart is not None else bool(self._server_dir)
        self._restart_cooldown = restart_cooldown
        self._last_restart_ts = 0.0
        self._lock = threading.Lock()
        self._local = threading.local()

    @property
    def _session(self):
        # requests.Session 非严格线程安全，按线程各自持有
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            self._local.session = session
        return session

    def query(self, cmd, table, k1=None, k2=None, ap=None, num=None, timeout_extra=0):
        """执行一次查询，返回 msgpack 解码结果（dict 或 list）。

        cmd: get / vals / keys
        table: 表名，如 "日k"；k1/k2 为键表达式（"key:600633" / "qz:6" / "fwd:a,b"）
        ap: 字段投影，如 "date,close"
        num: 服务端切片，正数取最新 N 条（降序），负数取最旧 N 条（升序）
        timeout_extra: 大结果集查询的额外超时秒数
        """
        params = {"cmd": cmd, "t": table}
        if k1 is not None:
            params["k1"] = k1
        if k2 is not None:
            params["k2"] = k2
        if ap:
            params["ap"] = "get." + ap
        if num is not None:
            params["num"] = num

        last_err = None
        for attempt in range(3):
            try:
                resp = self._session.get(self._base_url, params=params,
                                         timeout=self._timeout + timeout_extra)
                resp.raise_for_status()
                if not resp.content:
                    return None
                return msgpack.unpackb(resp.content, raw=False)
            except (requests.exceptions.RequestException, socket.error, msgpack.exceptions.ExtraData) as e:
                last_err = e
                system_log.warning("stockdb query failed (attempt {}): {}".format(attempt + 1, e))
                if attempt < 2:
                    self._handle_failure()
        raise StockdbRequestError("stockdb query failed after retries: {} ({})".format(params, last_err))

    def _handle_failure(self):
        if self._auto_restart and self._server_dir:
            with self._lock:
                if time.time() - self._last_restart_ts < self._restart_cooldown:
                    time.sleep(1)
                    return
                self._last_restart_ts = time.time()
                self._restart_server()
        else:
            time.sleep(0.5)

    def _restart_server(self):
        system_log.warning("stockdb not responding, restarting local server in {} ...".format(self._server_dir))
        try:
            subprocess.run(["pkill", "-9", "-x", "stockdb"], check=False,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(0.5)
            pidfile = os.path.join(self._server_dir, "lgdb.pid")
            if os.path.exists(pidfile):
                os.remove(pidfile)
            subprocess.Popen(["./stockdb", "-d", "stockdb.conf"], cwd=self._server_dir,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                             start_new_session=True)
        except OSError as e:
            system_log.error("failed to restart stockdb server: {}".format(e))
            return
        for _ in range(60):
            try:
                socket.create_connection((self._host, self._port), timeout=1).close()
                system_log.info("stockdb server restarted")
                time.sleep(0.5)
                return
            except OSError:
                time.sleep(0.5)
        system_log.error("stockdb server did not come back within 30s")

    # ---- 业务封装 ----
    # 线协议键表达式（服务端只认以下形式，'<'/'>' 语法是原生客户端的翻译产物）：
    #   all:      全量，升序
    #   fwz:a,b   区间 [a,b]，升序
    #   fwd:a,b   区间 [a,b]，降序（新→旧）
    #   qz:pre    前缀匹配，升序
    #   num=+N    从返回序列头部取 N 条；num=-N 从尾部取 N 条

    def get_day_rows(self, code, ap=DAY_ROW_AP):
        """某标的全部日K（升序返回）。停牌日无记录。"""
        rows = self.query("vals", "日k", k1="key:{}".format(code), k2="all:", ap=ap)
        return rows if isinstance(rows, list) else []

    def get_day_rows_between(self, code, start_int, end_int):
        rows = self.query("vals", "日k", k1="key:{}".format(code),
                          k2="fwz:{},{}".format(start_int, end_int), ap=DAY_ROW_AP)
        return rows if isinstance(rows, list) else []

    def get_minute_day(self, code, date_int):
        """某标的某交易日全部分钟K（升序返回）。date_int 为 8 位 YYYYMMDD。"""
        rows = self.query("vals", "分钟k", k1="key:{}".format(code),
                          k2="qz:{}".format(date_int), ap=MINUTE_ROW_AP)
        return rows if isinstance(rows, list) else []

    def get_minute_all(self, code):
        """某标的全部分钟K历史（升序，单次请求）。

        一次约 200+ 交易日 × 242 行，比逐日请求少 200 倍服务端查询次数，
        用于冷缓存预热（用一次大请求换掉串行服务的逐日排队）。
        """
        rows = self.query("vals", "分钟k", k1="key:{}".format(code),
                          k2="all:", ap=MINUTE_ROW_AP, timeout_extra=60)
        return rows if isinstance(rows, list) else []

    def get_code_groups(self):
        """股票代码全集：{前缀: [code, ...]}"""
        result = self.query("get", "股票代码")
        return result if isinstance(result, dict) else {}

    def get_delisted_codes(self):
        result = self.query("vals", "退市")
        return set(result) if isinstance(result, list) else set()

    def get_first_last_info(self, code):
        """返回 (首日int, 最新日int, 最新名称)。两次小请求。"""
        newest = self.query("vals", "日k", k1="key:{}".format(code),
                            k2="fwd:19900101,N", num=1)
        oldest = self.query("keys", "日k", k1="key:{}".format(code), k2="all:", num=1)
        newest = newest[0] if isinstance(newest, list) and newest and isinstance(newest[0], dict) else None
        oldest = oldest[0] if isinstance(oldest, list) and oldest else None
        if newest is None or oldest is None:
            return None
        # oldest 形如 "日k:600633:20000104"
        first_date = int(oldest.split(":")[-1])
        return first_date, int(newest["date"]), str(newest.get("name", ""))

    def get_corp_actions(self, code):
        """某标的全部除权除息事件（升序）。

        复权表的日期仅在键中（值字段: div/give/trans/mult/cum），
        因此用 cmd=get 取 [key, value] 对并解析键中日期。
        返回 [(date_int, {div,give,trans,mult,cum}), ...]
        """
        rows = self.query("get", "复权", k1="key:{}".format(code), k2="all:")
        events = []
        if isinstance(rows, list):
            for item in rows:
                if not (isinstance(item, (list, tuple)) and len(item) == 2):
                    continue
                key, value = item
                try:
                    date_int = int(str(key).split(":")[-1])
                except ValueError:
                    continue
                if isinstance(value, dict):
                    events.append((date_int, value))
        events.sort(key=lambda e: e[0])
        return events

    def get_index_dates(self, code):
        """某标的全部交易日（升序 int64 YYYYMMDD，仅 date 字段）。"""
        rows = self.query("vals", "日k", k1="key:{}".format(code), k2="all:", ap="date")
        if isinstance(rows, list) and rows and isinstance(rows[0], list):
            dates = [int(r[0]) for r in rows]
        else:
            dates = [int(r) for r in rows] if isinstance(rows, list) else []
        return sorted(dates)
