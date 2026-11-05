#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股右侧交易罗盘 · 复刻版 本地服务端

设计原则
--------
1. 只用 Python 标准库，零第三方依赖，双击即跑。
2. 前端 index.html 与原站保持一致（不裁剪模块），差异全部收敛在后端：
   已实现的接口给真实公开行情数据；未实现的接口返回结构完整的中文 stub，
   页面显示「暂无数据 / 数据引擎未接入」，而不是白屏或一堆报错。
3. 数据源全部是公开免费接口：
   - 腾讯 qt.gtimg.cn        实时快照 / 五档（被东财 push2 拦时的主要来源）
   - 新浪 money.finance      日K线、全市场快照（涨跌家数）
   - 东财 searchapi          股票搜索
   - 东财 push2ex            涨停/跌停/炸板池（含行业、连板数、封单额）
4. 所有外网请求带内存缓存 + 失败降级，单点挂掉不影响整页。

启动
----
    python server.py            # 默认 http://127.0.0.1:8092
    python server.py 9000       # 指定端口
"""

import json
import os
import re
import socket
import ssl
import sys
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

import engine
import gold
import ind15
import knowledge
import review
import tactics
import liveradar
import report_docx
import edge
import short

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 可写数据目录见 engine.py 同名注释：桌面便携版把 state/ 钉在 exe 旁边，
# 否则每次启动都解压到新的临时目录，榜单缓存全部作废。
STORE_DIR = os.environ.get("COMPASS_STORE") or BASE_DIR
STATE_DIR = os.path.join(STORE_DIR, "state")
DEFAULT_PORT = 8092
RUNTIME_PORT = [DEFAULT_PORT]   # main() 启动时回填，/api/health 会带出去

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

# ----------------------------------------------------------------------------
# 基础设施：HTTP 抓取 + TTL 缓存
# ----------------------------------------------------------------------------

_cache = {}
_cache_lock = threading.Lock()
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


def cached(key, ttl, producer):
    """进程内 TTL 缓存。producer 抛异常时不写缓存。"""
    now = time.time()
    with _cache_lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
    val = producer()
    with _cache_lock:
        _cache[key] = (now, val)
    return val


def fetch(url, referer=None, timeout=20, encoding="utf-8"):
    """GET 一个 URL，返回解码后的字符串。"""
    headers = {
        "User-Agent": UA,
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }
    if referer:
        headers["Referer"] = referer
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CTX) as r:
        raw = r.read()
    return raw.decode(encoding, "ignore")


def fetch_json(url, referer=None, timeout=20, encoding="utf-8"):
    txt = fetch(url, referer=referer, timeout=timeout, encoding=encoding)
    txt = txt.strip()
    # 新浪部分接口返回 JSONP 风格/非法 JSON，做些清洗
    if txt and txt[0] not in "[{":
        idx = min([i for i in (txt.find("["), txt.find("{")) if i >= 0], default=-1)
        if idx >= 0:
            txt = txt[idx:]
    return json.loads(txt)


# ----------------------------------------------------------------------------
# 代码 / secid 转换
# ----------------------------------------------------------------------------

def secid_to_sym(secid):
    """东财 secid -> 腾讯/新浪 symbol。1.600519 -> sh600519, 0.000001 -> sz000001,
    0.899050(北证50) -> bj899050"""
    try:
        market, code = str(secid).split(".", 1)
    except ValueError:
        code = str(secid)
        market = "1" if code[:1] == "6" else "0"
    code = code.strip()
    if market == "1":
        return "sh" + code
    if code[:1] in ("4", "8", "9"):
        return "bj" + code
    return "sz" + code


def sym_to_secid(sym):
    """sh600519 -> 1.600519 / sz000001 -> 0.000001 / bj899050 -> 0.899050"""
    s = sym.lower()
    if s.startswith("sh"):
        return "1." + s[2:]
    return "0." + s[2:]


def sym_of_code(code):
    code = str(code).strip()
    if code[:1] == "6":
        return "sh" + code
    if code[:1] in ("4", "8", "9"):
        return "bj" + code
    return "sz" + code


def secid_of_code(code):
    return sym_to_secid(sym_of_code(code))


# ----------------------------------------------------------------------------
# 腾讯行情
# ----------------------------------------------------------------------------
_tx_quote_cache = {}


def tx_raw(syms):
    """批量取腾讯行情原始字段（list）。空数据自动跳过，稳。"""
    out = {}
    syms = [s for s in dict.fromkeys(syms) if s]
    for i in range(0, len(syms), 60):
        batch = syms[i:i + 60]
        url = "https://qt.gtimg.cn/q=" + ",".join(batch)
        try:
            txt = fetch(url, referer="https://gu.qq.com/", timeout=15, encoding="gbk")
        except Exception:
            continue
        for line in txt.split(";"):
            line = line.strip()
            if not line.startswith("v_") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            sym = key[2:].strip()
            val = val.strip().strip('"')
            if not val:
                continue
            out[sym] = val.split("~")
    return out


def _f(arr, idx, default=0.0):
    try:
        v = float(arr[idx])
        return v
    except (IndexError, TypeError, ValueError):
        return default


def tx_to_em(sym, arr):
    """腾讯字段 -> 东财 push2 字段名（前端按东财 f 字段取值，必须对齐）。"""
    code = arr[2] if len(arr) > 2 else sym[2:]
    name = arr[1] if len(arr) > 1 else ""
    price = _f(arr, 3)
    prevclose = _f(arr, 4)
    openp = _f(arr, 5)
    vol_hand = _f(arr, 6)          # 成交量（手）
    change = _f(arr, 31)
    pct = _f(arr, 32)
    high = _f(arr, 33)
    low = _f(arr, 34)
    amount_wan = _f(arr, 37)       # 成交额（万元）
    turnover = _f(arr, 38)
    pe = _f(arr, 39)
    circ_mv_yi = _f(arr, 44)
    total_mv_yi = _f(arr, 45)
    pb = _f(arr, 46)
    volratio = _f(arr, 49)
    if not change and prevclose:
        change = round(price - prevclose, 3)
    if not pct and prevclose:
        pct = round((price - prevclose) / prevclose * 100, 2)
    market = 1 if sym.startswith("sh") else 0
    return {
        "f2": price, "f3": pct, "f4": change,
        "f5": vol_hand, "f6": round(amount_wan * 10000, 2),
        "f8": turnover, "f9": pe, "f10": volratio,
        "f12": code, "f13": market, "f14": name,
        "f15": high, "f16": low, "f17": openp, "f18": prevclose,
        "f20": round(total_mv_yi * 1e8, 2), "f21": round(circ_mv_yi * 1e8, 2),
        "f23": pb, "f114": pe, "f115": pe,
    }


def api_quotes(params):
    raw_ids = (params.get("secids") or params.get("codes") or "").strip()
    if not raw_ids:
        return {"source": "tx", "data": {"diff": []}}
    ids = [x for x in re.split(r"[,\s]+", raw_ids) if x]
    syms = [secid_to_sym(x) if "." in x else sym_of_code(x) for x in ids]
    raw = tx_raw(syms)
    diff = [tx_to_em(s, raw[s]) for s in syms if s in raw]
    return {"source": "tx", "data": {"diff": diff}}


# ----------------------------------------------------------------------------
# K 线（新浪日线，前复权口径由 nofq 标记说明）
# ----------------------------------------------------------------------------

def api_kline(params):
    secid = params.get("secid") or ""
    code = params.get("code") or (secid.split(".")[-1] if "." in secid else secid)
    sym = secid_to_sym(secid) if "." in secid else sym_of_code(code)
    days = int(params.get("days") or 300)
    days = max(20, min(days, 1200))
    url = ("https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
           "CN_MarketData.getKLineData?symbol=%s&scale=240&ma=no&datalen=%d" % (sym, days))
    rows = cached("kline:%s:%d" % (sym, days), 120, lambda: fetch_json(url, timeout=20))
    klines = []
    for r in rows or []:
        try:
            klines.append("%s,%s,%s,%s,%s,%s,0" % (
                r["day"], r["open"], r["close"], r["high"], r["low"], r["volume"]))
        except (KeyError, TypeError):
            continue
    name = ""
    try:
        q = tx_raw([sym]).get(sym)
        if q:
            name = q[1]
    except Exception:
        pass
    return {"source": "sina", "nofq": True,
            "data": {"code": code, "name": name, "klines": klines}}


# ----------------------------------------------------------------------------
# 搜索（东财 suggest）
# ----------------------------------------------------------------------------
_SUGGEST_TOKEN = "D43BF722C8E33BDC906FB84D85E326E8"


def api_search(params):
    q = (params.get("q") or "").strip()
    if not q:
        return {"list": []}
    url = ("https://searchapi.eastmoney.com/api/suggest/get?input=%s"
           "&type=14&token=%s&count=20" % (urllib.parse.quote(q), _SUGGEST_TOKEN))
    try:
        d = fetch_json(url, referer="https://quote.eastmoney.com/", timeout=12)
    except Exception:
        return {"list": []}
    rows = ((d.get("QuotationCodeTable") or {}).get("Data")) or []
    out = []
    for r in rows:
        if (r.get("Classify") or "") not in ("AStock", "UniversalIndex", "Index"):
            continue
        secid = r.get("QuoteID") or ""
        if not secid:
            continue
        out.append({
            "code": r.get("Code"),
            "name": r.get("Name"),
            "market": int((secid.split(".")[0] or "0")),
            "secid": secid,
        })
    return {"list": out}


# ----------------------------------------------------------------------------
# 涨跌停池（东财 push2ex）
# ----------------------------------------------------------------------------
_POOL_UT = "7eea3edcaed734bea9cbfc24409ed989"


def _pool_url(kind, date=None, pagesize=300):
    api = {"zt": "getTopicZTPool", "dt": "getTopicDTPool", "zb": "getTopicZBPool"}[kind]
    sort = {"zt": "fbt:asc", "dt": "fund:asc", "zb": "fbt:asc"}[kind]
    url = ("https://push2ex.eastmoney.com/%s?ut=%s&dpt=wz.ztzt&Pageindex=0"
           "&pagesize=%d&sort=%s" % (api, _POOL_UT, pagesize, sort))
    if date:
        url += "&date=" + date
    return url


def _fetch_pool(kind, date=None):
    def load():
        try:
            d = fetch_json(_pool_url(kind, date), referer="https://quote.eastmoney.com/",
                           timeout=20)
        except Exception:
            return {"tc": 0, "qdate": None, "pool": []}
        data = d.get("data") or {}
        return {"tc": data.get("tc") or 0, "qdate": data.get("qdate"),
                "pool": data.get("pool") or []}
    return cached("pool:%s:%s" % (kind, date or "latest"), 120, load)


def _hhmm(v):
    try:
        s = "%06d" % int(v)
        return "%s:%s" % (s[0:2], s[2:4])
    except (TypeError, ValueError):
        return "--"


def _pool_row(r, kind):
    price = (r.get("p") or 0) / 1000.0
    zttj = r.get("zttj") or {}
    return {
        "code": r.get("c"),
        "name": r.get("n"),
        "price": price,
        "pct": round(r.get("zdp") or 0, 2),
        "industry": r.get("hybk") or "--",
        "streak": r.get("lbc") or 1,
        "turnover": round(r.get("hs") or 0, 2),
        "amount_yi": round((r.get("amount") or 0) / 1e8, 2),
        "seal_wan": round((r.get("fund") or 0) / 1e4, 1),
        "open_times": r.get("zbc") or 0,
        "first_seal": _hhmm(r.get("fbt")),
        "days": zttj.get("days"),
        "ct": zttj.get("ct"),
        "last_seal": _hhmm(r.get("lbt")) if kind != "zb" else "--",
    }


def _trading_days_back(start_ymd, n):
    """从 YYYYMMDD 往前推 n 个工作日（简化：只跳周末）。"""
    import datetime
    y, m, d = int(start_ymd[0:4]), int(start_ymd[4:6]), int(start_ymd[6:8])
    cur = datetime.date(y, m, d)
    out = []
    while len(out) < n:
        cur -= datetime.timedelta(days=1)
        if cur.weekday() < 5:
            out.append(cur.strftime("%Y%m%d"))
    return out


def _breadth():
    """全市场涨跌家数 + 成交额。新浪快照分页，57 页左右，并行抓 + 缓存 90 秒。"""

    def load():
        base = ("https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
                "Market_Center.getHQNodeData?page=%d&num=100&sort=symbol&asc=1&node=%s")
        rows = []

        def one(p):
            try:
                return fetch_json(base % (p, "hs_a"), referer="https://finance.sina.com.cn",
                                  timeout=20) or []
            except Exception:
                return []

        with ThreadPoolExecutor(max_workers=10) as ex:
            for chunk in ex.map(one, range(1, 63)):
                rows.extend(chunk)
                # 新浪返回不足 100 条即最后一页
                if len(chunk) < 100:
                    break
        up = down = flat = amount = 0.0
        total = 0
        for r in rows:
            try:
                trade = float(r.get("trade") or 0)
            except (TypeError, ValueError):
                continue
            if trade <= 0:      # 停牌
                continue
            total += 1
            try:
                pct = float(r.get("changepercent") or 0)
            except (TypeError, ValueError):
                pct = 0.0
            if pct > 0:
                up += 1
            elif pct < 0:
                down += 1
            else:
                flat += 1
            try:
                amount += float(r.get("amount") or 0)
            except (TypeError, ValueError):
                pass
        return {"up": int(up), "down": int(down), "flat": int(flat), "total": total,
                "amount_yi": round(amount / 1e8, 1),
                "scanned": len(rows)}

    return cached("breadth", 90, load)


def _promote_rate(trade_date):
    """昨日涨停今日晋级率：昨日涨停池里今天涨幅仍 ≥9.5% 的占比。"""
    if not trade_date:
        return None
    trade_date = str(trade_date).replace("-", "")
    prev_days = _trading_days_back(trade_date, 1)
    if not prev_days:
        return None

    def load():
        prev = _fetch_pool("zt", prev_days[0])
        codes = [r.get("c") for r in prev.get("pool") or [] if r.get("c")]
        if not codes:
            return None
        syms = [sym_of_code(c) for c in codes]
        raw = tx_raw(syms)
        keep = 0
        for s in syms:
            arr = raw.get(s)
            if not arr:
                continue
            if _f(arr, 32) >= 9.5:
                keep += 1
        return round(keep / len(codes) * 100, 1)

    return cached("promote:%s" % trade_date, 300, load)


def latest_trade_date():
    """最近一个交易日（YYYYMMDD）。用上证指数日线最后一条判定，最可靠。"""

    def load():
        url = ("https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
               "CN_MarketData.getKLineData?symbol=sh000001&scale=240&ma=no&datalen=5")
        try:
            rows = fetch_json(url, timeout=15) or []
            if rows:
                return str(rows[-1]["day"]).replace("-", "")
        except Exception:
            pass
        return time.strftime("%Y%m%d")

    return cached("trade_date", 300, load)


def api_sentiment(params):
    tdate = latest_trade_date()
    zt = _fetch_pool("zt", tdate)
    dt = _fetch_pool("dt", tdate)
    zb = _fetch_pool("zb", tdate)
    trade_date = "%s-%s-%s" % (tdate[0:4], tdate[4:6], tdate[6:8])
    breadth = _breadth()

    idx_meta = [
        ("1.000001", "sh000001", "上证指数"),
        ("0.399001", "sz399001", "深证成指"),
        ("0.399006", "sz399006", "创业板指"),
        ("1.000300", "sh000300", "沪深300"),
        ("1.000688", "sh000688", "科创50"),
        ("0.899050", "bj899050", "北证50"),
    ]
    idx_raw = tx_raw([m[1] for m in idx_meta])
    indices = []
    for secid, sym, nm in idx_meta:
        arr = idx_raw.get(sym)
        if not arr:
            continue
        indices.append({
            "secid": secid, "name": nm,
            "price": _f(arr, 3), "pct": round(_f(arr, 32), 2),
            "amount_yi": round(_f(arr, 37) / 1e4, 2),
        })

    zt_n = int(zt.get("tc") or len(zt.get("pool") or []))
    dt_n = int(dt.get("tc") or len(dt.get("pool") or []))
    zb_n = int(zb.get("tc") or len(zb.get("pool") or []))
    seal_rate = round(zt_n / (zt_n + zb_n) * 100, 1) if (zt_n + zb_n) else None

    zt_rows = [_pool_row(r, "zt") for r in (zt.get("pool") or [])]
    dt_rows = [_pool_row(r, "dt") for r in (dt.get("pool") or [])]
    zb_rows = [_pool_row(r, "zb") for r in (zb.get("pool") or [])]
    max_streak = max([r["streak"] for r in zt_rows] or [0])

    ladder = {}
    for r in zt_rows:
        if r["streak"] and r["streak"] >= 2:
            ladder[r["streak"]] = ladder.get(r["streak"], 0) + 1
    ladder_rows = [{"streak": k, "count": v} for k, v in sorted(ladder.items(), reverse=True)]

    hot = {}
    for r in zt_rows:
        ind = r["industry"] or "--"
        hot[ind] = hot.get(ind, 0) + 1
    hot_rows = [{"name": k, "count": v} for k, v in
                sorted(hot.items(), key=lambda kv: -kv[1])[:8]]

    # 情绪温度：涨跌比 30 + 涨停数 20 + 封板率 20 + 连板高度 15 + 指数 15
    up, dn = breadth.get("up", 0), breadth.get("down", 0)
    ratio = (up / (up + dn)) if (up + dn) else 0.5
    s_ratio = ratio * 30
    s_zt = min(zt_n / 80.0, 1.0) * 20
    s_seal = ((seal_rate if seal_rate is not None else 60) / 100.0) * 20
    s_streak = min(max_streak / 6.0, 1.0) * 15
    idx_avg = sum(i["pct"] for i in indices) / len(indices) if indices else 0
    s_idx = max(0.0, min(1.0, (idx_avg + 2) / 4.0)) * 15
    score = round(s_ratio + s_zt + s_seal + s_streak + s_idx, 1)
    label = ("过热" if score >= 80 else "偏热" if score >= 60 else
             "中性" if score >= 40 else "偏冷" if score >= 20 else "冰点")

    return {
        "updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "trade_date": trade_date or time.strftime("%Y-%m-%d"),
        "source": "腾讯行情 · 新浪全市场快照 · 东财涨跌停池(push2ex)",
        "indices": indices,
        "index_source": "tx",
        "breadth": {"up": up, "down": dn, "flat": breadth.get("flat", 0),
                    "total": breadth.get("total", 0)},
        "limit": {"zt": zt_n, "dt": dt_n, "zb": zb_n, "seal_rate": seal_rate,
                  "max_streak": max_streak, "ladder": ladder_rows,
                  "promote_rate": _promote_rate(trade_date)},
        "hot_industries": hot_rows,
        "amount": {"total_yi": breadth.get("amount_yi", 0)},
        "temperature": {"score": score, "label": label},
        "pools": {"zt": zt_rows, "dt": dt_rows, "zb": zb_rows},
        "warnings": [],
    }


# ----------------------------------------------------------------------------
# 盘口（腾讯五档 + 规则化打分）
# ----------------------------------------------------------------------------

def api_pankou(params):
    secid = params.get("secid") or ""
    code = params.get("code") or (secid.split(".")[-1] if "." in secid else "")
    sym = secid_to_sym(secid) if secid else sym_of_code(code)
    raw = tx_raw([sym]).get(sym)
    if not raw:
        return {"ok": False, "error": "行情快照不可达"}
    em = tx_to_em(sym, raw)
    bid = []
    ask = []
    for i in range(5):
        bid.append({"price": _f(raw, 9 + i * 2), "vol": _f(raw, 10 + i * 2)})
        ask.append({"price": _f(raw, 19 + i * 2), "vol": _f(raw, 20 + i * 2)})
    bid_sum = sum(b["vol"] for b in bid)
    ask_sum = sum(a["vol"] for a in ask)
    net = bid_sum - ask_sum
    ratio = round(bid_sum / ask_sum, 3) if ask_sum else None
    waipan, neipan = _f(raw, 7), _f(raw, 8)
    wp_ratio = round(waipan / (waipan + neipan), 2) if (waipan + neipan) else None
    weibi = round(net / (bid_sum + ask_sum) * 100, 1) if (bid_sum + ask_sum) else 0.0
    price = em["f2"]
    prevclose = em["f18"]
    zt = round(prevclose * 1.1, 2) if prevclose else 0.0
    dtp = round(prevclose * 0.9, 2) if prevclose else 0.0
    pct = em["f3"]
    if price >= zt - 0.001:
        state = "涨停"
    elif price <= dtp + 0.001:
        state = "跌停"
    else:
        state = "普通"

    signals = []
    if weibi <= -40:
        signals.append({"name": "委比弱势", "type": "bear",
                        "desc": "委比=%.1f，五档卖盘压制明显。" % weibi})
    elif weibi >= 40:
        signals.append({"name": "委比强势", "type": "bull",
                        "desc": "委比=%.1f，五档买盘承接积极。" % weibi})
    volratio = em["f10"]
    if volratio >= 1.8:
        signals.append({"name": "显著放量", "type": "bull",
                        "desc": "量比=%.2f，交投明显放大。" % volratio})
    if em["f8"] >= 10:
        signals.append({"name": "换手过热", "type": "bear",
                        "desc": "换手率=%.2f%%，短线筹码交换剧烈。" % em["f8"]})

    score = 50
    score += 8 if (ratio or 1) >= 1 else -8
    score += 6 if (wp_ratio or 0.5) >= 0.5 else -6
    score += 6 if volratio >= 1.2 else -4
    score += 8 if pct > 0 else -8
    score = max(0, min(100, score))
    grade = "强势" if score >= 70 else "中性" if score >= 45 else "警惕"

    return {
        "ok": True, "code": em["f12"], "name": em["f14"], "sym": sym,
        "secid": secid or secid_of_code(code),
        "price": price, "prevclose": prevclose, "open": em["f17"],
        "zt": zt, "dt": dtp, "chg_pct": pct,
        "sanity": {"waipan": waipan, "neipan": neipan, "wp_ratio": wp_ratio,
                   "weibi": weibi, "weicha": net, "volratio": volratio,
                   "turnover": em["f8"]},
        "five": {"bid": bid, "ask": ask, "bid_sum": bid_sum, "ask_sum": ask_sum,
                 "net": net, "ratio": ratio,
                 "support": bid[0]["price"], "pressure": ask[0]["price"]},
        "limit": {"state": state, "is_limit": state != "普通",
                  "is_yizi": False, "is_t": False, "is_fanbao": False},
        "minute": {"has": False, "avg_price": None, "above_avg": None, "slope": None,
                   "day_chg_pct": pct, "shape": "—", "shape_desc": "复刻版暂未接入分时形态识别。",
                   "warn": False, "points": 0},
        "signals": signals,
        "score": score, "score_max": 100, "grade": grade,
        "grade_text": "盘口为规则化推算，不构成投资建议。",
        "note": "盘口数据为实时快照，所有信号为规则化推算，不构成投资建议。",
        "ksource": "tx",
    }


# ----------------------------------------------------------------------------
# 自选 / 归档（本地 JSON 持久化）
# ----------------------------------------------------------------------------

def _state_file(name):
    os.makedirs(STATE_DIR, exist_ok=True)
    return os.path.join(STATE_DIR, name)


def _read_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def api_watchlist_get(params):
    """自选股列表。

    ★ 磁盘上只存 {"code": "600519"}（POST 原样写入前端传的数组），但前端在用渲染
      研究中心那行「自选股快捷诊断：<a>名称</a>」时读的是 s.name / s.secid ——
      直接回原文就会渲染出 `undefined`（2026-11-04 实测）。所以读取时补齐
      name/secid，顺带带上快照价，写盘格式保持不变（老文件不用迁移）。
    """
    lst = _read_json(_state_file("watchlist.json"), []) or []
    if not isinstance(lst, list):
        return {"list": []}
    try:
        snap = engine.snap_index()
    except Exception:
        snap = {}
    out = []
    for it in lst:
        if not isinstance(it, dict):
            continue
        code = str(it.get("code") or it.get("secid") or "").split(".")[-1].strip()
        if len(code) != 6:
            continue
        row = snap.get(code) or {}
        item = dict(it)
        item["code"] = code
        item["secid"] = it.get("secid") or engine.secid_of_code(code)
        item["name"] = it.get("name") or row.get("name") or code
        if row:
            item["price"] = row.get("price")
            item["pct"] = row.get("pct")
        out.append(item)
    return {"list": out}


def api_watchlist_post(body):
    lst = body.get("list")
    if isinstance(lst, list):
        _write_json(_state_file("watchlist.json"), lst)
    return {"ok": True, "count": len(lst or [])}


def api_save_report(body):
    if not isinstance(body, dict):
        return {"ok": False, "error": "请求体不是 JSON 对象"}
    code = str(body.get("code") or "unknown")
    stamp = time.strftime("%Y%m%d-%H%M%S")
    path = _state_file(os.path.join("reports", "%s-%s.json" % (code, stamp)))
    _write_json(path, body)
    return {"ok": True, "id": "%s-%s" % (code, stamp),
            "saved_at": time.strftime("%Y-%m-%d %H:%M:%S")}


def api_archive(params):
    rep_dir = _state_file("reports")
    items = []
    if os.path.isdir(rep_dir):
        for fn in sorted(os.listdir(rep_dir), reverse=True)[:50]:
            if not fn.endswith(".json"):
                continue
            d = _read_json(os.path.join(rep_dir, fn), {})
            items.append({"id": fn[:-5], "code": d.get("code"),
                          "name": d.get("name"), "title": d.get("title") or fn[:-5],
                          "saved_at": d.get("saved_at") or fn})
    return {"list": items, "count": len(items)}


# ----------------------------------------------------------------------------
# 未接入模块的 stub
# ----------------------------------------------------------------------------

NOT_IMPLEMENTED = {
    "strong": "强势股扫描（需全市场 5000+ 标的日线复算）",
    "rightside": "右侧三位置扫描（六池全市场实算）",
    "compass": "右侧罗盘打分",
    "compass_live": "右侧罗盘实时刷新",
    "checkup": "个股十二维体检",
    "checkup_top": "体检榜单",
    "chan": "缠论结构识别",
    "chanlist": "缠论候选列表",
    "chanscore": "缠论评分",
    "volprice": "量价分析",
    "volcheck": "量价校验",
    "mainwave": "主升浪判定",
    "buildup": "主力建仓雷达",
    "buildupcheck": "主力建仓复核",
    "taofei": "腾飞点识别",
    "taofeicheck": "腾飞点复核",
    "short": "短线作战池",
    "chaoduan": "超短池",
    "chaoduanlist": "超短池列表",
    "sentiment_pool": "情绪池明细",
    "scores": "综合评分",
    "tactics": "战法库",
    "tacticscheck": "战法校验",
    "fib4check": "斐波那契四线校验",
    "ind15": "十五五产业映射",
    "gold": "黄金专区（沪金十二维体检）",
    "gold_kb": "黄金体系溯源",
    "knowledge": "知识库",
    "dragon": "龙虎榜",
    "dragonlist": "龙虎榜明细",
    "fund": "资金流向",
    "liveradar": "实时雷达",
    "liveradarstart": "实时雷达启动",
    "watch_sell": "卖出侧信号",
    "position_health": "持仓体检",
    "review": "复盘",
    "edge": "优势因子",
    "screen": "条件选股器",
    "reports": "券商研报",
    "notice": "公告",
    "news": "资讯",
    "report_docx": "Word 报告导出",
    "rescan": "全市场重扫",
}


def _truthy(v):
    return str(v).lower() in ("1", "true", "yes", "y")


def api_strong(params):
    """强势股榜：全市场日K复算的四条件严格筛选（结果按交易日落盘）。"""
    td = engine.latest_trade_date()
    d = engine.load_strong()
    if _truthy(params.get("force")) or not d or d.get("trade_date") != td:
        engine.start_scan("strong", "strong-api")
    if d and d.get("list"):
        if d.get("trade_date") != td:
            d = dict(d)
            d["scanning"] = engine.scan_running()
            d["note"] = "数据为上一交易日（%s），已在后台补扫今日" % d.get("trade_date")
        return d
    st = engine.scan_state()
    return {
        "ok": True, "empty": True, "scanning": st.get("running"),
        "message": "正在扫描全市场（首次约 2 分钟，之后按交易日缓存秒开）",
        "updated": None, "trade_date": td, "count": 0, "list": [],
        "total_scanned": 0, "conditions": engine.STRONG_COND,
        "source": engine.STRONG_SOURCE,
        "progress": {"phase": st.get("phase"), "done": st.get("done"), "total": st.get("total")},
    }


def api_rightside(params):
    """右侧交易：六池 + 总榜 + 风险名单 + 市场环境（同一份全市场扫描产出）。"""
    td = engine.latest_trade_date()
    d = engine.load_rightside()
    if _truthy(params.get("force")) or not d or d.get("trade_date") != td:
        engine.start_scan("rightside", "rightside-api")
    if d:
        if d.get("trade_date") != td:
            d = dict(d)
            d["scanning"] = engine.scan_running()
            d["stale"] = True
        return d
    st = engine.scan_state()
    return {"ok": True, "empty": True, "scanning": st.get("running"),
            "message": "正在做全市场右侧扫描…", "pools": [], "rank": [], "risk_list": [],
            "stats": {}, "trade_date": td,
            "progress": {"phase": st.get("phase"), "done": st.get("done"), "total": st.get("total")}}


def api_checkup(params):
    """个股十二维体检：单票（code/secid）或批量（codes=a,b,c & slim=1）。"""
    codes = params.get("codes")
    if codes:
        want = [c.strip() for c in str(codes).split(",") if c.strip()][:40]
        slim = _truthy(params.get("slim"))
        items = {}
        for c in want:
            try:
                r = engine.checkup_one(c, slim=slim)
                if r.get("ok"):
                    items[c] = r
            except Exception:
                continue
        return {"ok": True, "items": items, "count": len(items),
                "trade_date": engine.latest_trade_date()}
    d = engine.checkup_one(params.get("code"), params.get("secid"),
                           slim=_truthy(params.get("slim")))
    if not d.get("ok"):
        d["verdict"] = "—"
        d["strategy_score"] = None
        d["checkup_score"] = None
    return d


def api_chanscore(params):
    """批量缠论体检分（强势股/总榜用来做 6:4 综合分重排）。"""
    codes = [c.strip() for c in str(params.get("codes") or "").split(",") if c.strip()][:80]
    try:
        mn = int(params.get("min") or 0)
    except (TypeError, ValueError):
        mn = 0
    items = engine.chanscore_many(codes, mn)
    return {"ok": True, "items": items, "count": len(items)}


# ----------------------------------------------------------------------------
# 单票量价 / 缠论 / 缠论候选榜
#
# 适配原则：engine 侧 volprice()/chan() 给的是「引擎内部口径」，与前端渲染层
# 期望的字段并非一一对应（例如 advice 引擎给字符串、前端要 {level,text}；
# stage 引擎给字符串、前端要 {name,fit}；signals 引擎分四个数组、前端要一个
# 带 level 的扁平数组）。这些差异全部收敛在本节的适配函数里 —— 引擎不动，
# index.html 不动，坏一处不影响其他模块。
# ----------------------------------------------------------------------------

def _stock_ctx(code, secid=None):
    """取单票上下文（复刻 checkup_one 的取数口径）：返回 (code, snap, rows, feats, err)。"""
    code = str(code or "").strip()
    if not code and secid:
        code = str(secid).split(".")[-1]
    if not code:
        return "", {}, [], None, "缺少 code/secid"
    td = engine.latest_trade_date()
    rows = engine.kline_get(code)
    if not rows or (engine.read_json(engine._kl_path(code)) or {}).get("td") != td:
        engine.ensure_klines([code], td=td, workers=1)
        rows = engine.kline_get(code)
    if not rows:
        return code, {}, [], None, "未取到该股日 K（代码 %s 可能已退市或停牌）" % code
    s = engine.snap_index().get(code) or {}
    f = engine.features(code, rows, s)
    if not f:
        return code, s, rows, None, "日 K 不足 60 根，无法计算"
    if s.get("industry") is None:
        s["industry"] = engine.industry_map().get(code) or "--"
    f["industry"] = s.get("industry") or "--"
    return code, s, rows, f, None


def api_volcheck(params):
    """量价体检（单票）：八态关系 + 三分区闸门 + 警示/确认信号。"""
    code, s, rows, f, err = _stock_ctx(params.get("code"), params.get("secid"))
    if err:
        return {"ok": False, "error": err}

    vp = engine.volprice(f)
    n = len(rows)
    closes = [r[2] for r in rows]
    highs = [r[3] for r in rows]
    lows = [r[4] for r in rows]
    vols = [r[5] for r in rows]

    hi120, lo120 = max(highs[-120:]), min(lows[-120:])
    chg5 = ((closes[-1] / closes[-6] - 1) * 100.0) if n > 6 and closes[-6] else None
    v5 = sum(vols[-5:]) / 5.0 if n >= 5 else 0.0
    p5 = sum(vols[-10:-5]) / 5.0 if n >= 10 else v5
    vol_trend = (v5 / p5) if p5 else 1.0

    # 引擎分四个数组，前端消费一个扁平数组（level 决定配色与图标）
    signals = []
    for lvl, arr in (("confirm", vp["confirm"]), ("warn", vp["warn"]),
                     ("mid", vp["mid"]), ("info", vp["info"])):
        for x in arr:
            signals.append({"level": lvl, "name": x.get("name"),
                            "desc": x.get("desc"), "evidence": x.get("evidence")})

    gate = vp["gate"]
    adv_level = {"pass": "积极", "block": "减仓/回避"}.get(gate, "观望")
    adv_text = {
        "pass": "量价确认成立，可按右侧纪律建仓，仓位由止损距离倒推",
        "block": "量价出现警示信号，按纪律不建仓",
        "watch": "量价中性、无明确方向，持有或等待",
    }[gate if gate in ("pass", "block") else "watch"]

    vp8 = dict(vp["vp8"])
    vp8["chg5"] = round(chg5, 2) if chg5 is not None else None
    vp8["vol_trend"] = round(vol_trend, 2)

    return {
        "ok": True, "code": code, "name": s.get("name") or "",
        "secid": engine.secid_of_code(code),
        "price": round(f["close"], 2), "pct": round(f.get("pct") or 0, 2),
        "trade_date": f["date"], "ksource": "sina",
        "metrics": {
            "vol_ratio_today": vp.get("vol_ratio_today"),
            "pos120": vp.get("pos120"),
            "lo120": round(lo120, 2), "hi120": round(hi120, 2),
            "chg20": round(f["chg20"], 2) if f.get("chg20") is not None else None,
        },
        "vp8": vp8,
        "counts": vp["counts"],
        "signals": signals,
        "gate": gate,
        "advice": {"level": adv_level, "text": adv_text},
        # 引擎的 stage 是字符串（拉升期/回调期/整理期），前端要 {name, fit}；
        # fit（与主力阶段吻合度）需要更长周期的资金流数据，公开源拿不到，留空不编。
        "stage": {"name": vp["stage"], "fit": None},
        "disclaimer": "量价关系在高位与低位含义相反，以上为规则化算法推算，不构成投资建议。",
    }


def api_chan(params):
    """缠论体检（单票）：结构分 + 走势 + 背驰 + 中枢 + 笔段 + K 线（画图用）。"""
    code, s, rows, f, err = _stock_ctx(params.get("code"), params.get("secid"))
    if err:
        return {"ok": False, "error": err}

    ch = engine.chan(rows)
    if not ch:
        return {"ok": False, "error": "缠论结构计算失败（K 线不足 60 根）"}

    bs = ch.get("bs") or ""
    out = dict(ch)
    out.update({
        "ok": True, "code": code, "name": s.get("name") or "",
        "secid": engine.secid_of_code(code),
        "px": round(f["close"], 2), "chg_pct": round(f.get("pct") or 0, 2),
        "trade_date": f["date"],
        # 硬性否决：三卖 or 顶背驰（与 checkup 的 veto 口径一致）
        "veto": ("三卖" in bs) or (ch.get("divergence") == "顶背驰"),
        # 斐波那契四度（魏强斌框架）需要分形位与筹码分布，公开源不具备，留空由前端降级
        "four": {},
        "plan": engine.make_plan(f, engine.level_of(f.get("dev20") or 0)),
        "rows": [{"date": r[0], "open": r[1], "close": r[2], "high": r[3],
                  "low": r[4], "vol": r[5]} for r in rows[-120:]],
    })
    return out


_CHANLIST_CACHE = {"ts": 0, "data": None}


def api_chanlist(params):
    """缠论候选榜。

    候选集不另做全市场扫描，直接取「强势榜 + 右侧六池」的并集 —— 这批票的日 K
    在 scan 阶段已全部落盘，逐只算结构是纯内存运算，几百只约 1~2 秒，不会因为
    一个榜单把全市场重扫一遍。结果缓存 60 秒（结构在盘中不会因几笔成交而改变）。
    """
    try:
        mn = int(params.get("min") or 0)
    except (TypeError, ValueError):
        mn = 0
    force = _truthy(params.get("force"))
    now = time.time()
    if not force and _CHANLIST_CACHE["data"] and now - _CHANLIST_CACHE["ts"] < 60 \
            and _CHANLIST_CACHE["data"].get("min") == mn:
        return _CHANLIST_CACHE["data"]

    rs = engine.load_rightside() or {}
    st = engine.load_strong() or {}
    seen, cands = set(), []
    for r in (st.get("list") or []):
        c = r.get("code")
        if c and c not in seen:
            seen.add(c)
            cands.append(r)
    for p in (rs.get("pools") or []):
        for r in (p.get("stocks") or []):
            c = r.get("code")
            if c and c not in seen:
                seen.add(c)
                cands.append(r)

    if not cands:
        engine.ensure_scan_async("all", "chanlist-empty")

    lst = []
    for r in cands[:400]:
        code = r.get("code")
        rows = engine.kline_get(code)
        if not rows:
            continue
        ch = engine.chan(rows)
        if not ch or ch["score"] < mn:
            continue
        cents = ch.get("centers") or []
        cen = cents[-1] if cents else {}
        lst.append({
            "code": code,
            "secid": r.get("secid") or engine.secid_of_code(code),
            "name": r.get("name") or "",
            "industry": r.get("industry") or "--",
            "price": r.get("price"),
            "score": ch["score"], "score_max": ch.get("score_max", 100),
            "grade": ch["grade"], "grade_text": ch.get("grade_text"),
            "walk": ch["walk"], "divergence": ch["divergence"],
            "bs": ch.get("bs") or "",
            "center": {"zg": cen.get("zg"), "zd": cen.get("zd")} if cen else {},
        })
    lst.sort(key=lambda x: -x["score"])

    out = {
        "ok": True, "trade_date": rs.get("trade_date") or st.get("trade_date")
        or engine.latest_trade_date(),
        "updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(lst), "total_scanned": len(cands), "list": lst,
        "min": mn,
        "conditions": "包含处理 → 分型 → 笔 → 中枢 → 背驰 → 三类买卖点；"
                      "按结构分排序（≥80 结构健康 · 60~79 中性 · <60 走坏）",
        "method": "候选取强势榜与右侧六池的并集（日 K 已在扫描阶段落盘，不重复请求外网）。"
                  "结构分以走势（上涨/下跌/盘整）+ 最近中枢位置 + 背驰 + 买卖点合成，"
                  "均线仍多头时对中枢下移做相干性修正，避免强势股回踩被一票打死。",
        "disclaimer": "缠论结构识别对分型/笔的参数敏感，同一段行情换参数结论可能不同，仅供研究参考。",
    }
    _CHANLIST_CACHE["ts"] = now
    _CHANLIST_CACHE["data"] = out
    return out


# ----------------------------------------------------------------------------
# 每日右侧安全 Top10（compass）
#
# 与 /api/rightside 的区别：rightside 是「六池命中清单」（按池归类，回答"哪些形态成立"），
# compass 是「一张榜单」（按三引擎加权分排序，回答"今天最该看哪 10 只"）。
# 数据同源（复用扫描产物，不重复拉外网），只是重排 + 过否决：
#   综合分 = 成熟度×0.45 + 缠论×0.35 + 量价×0.20
# 说明：量价维在此端点现场复算（engine.volprice），因为扫描产物里只落了
# vp 的确认/警示计数，没落维度分；日 K 已在磁盘，几十只纯内存运算可忽略。
# ----------------------------------------------------------------------------

_CMP_CACHE = {"ts": 0.0, "td": None, "data": None}

def _maform(f):
    """均线形态：四线多头 / 三线多头 / 空头排列 / 纠缠。"""
    a, b, c, d = f.get("ma5"), f.get("ma10"), f.get("ma20"), f.get("ma60")
    if a and b and c:
        if a > b > c and d and c > d:
            return "四线多头"
        if a > b > c:
            return "三线多头"
        if a < b < c:
            return "空头排列"
    return "均线纠缠"


def api_compass(params):
    """每日右侧安全 Top10 + 观察席（第二档）。"""
    td = engine.latest_trade_date()
    force = _truthy(params.get("force"))
    now = time.time()
    if not force and _CMP_CACHE["data"] and _CMP_CACHE["td"] == td \
            and now - _CMP_CACHE["ts"] < 120:
        return _CMP_CACHE["data"]

    rs = engine.load_rightside() or {}
    if (not rs or rs.get("trade_date") != td) and not engine.scan_running():
        engine.start_scan("rightside", "compass-api")

    if not rs or not rs.get("pools"):
        st = engine.scan_state()
        return {
            "ok": True, "empty": True, "scanning": st.get("running"),
            "trade_date": td, "top10": [], "watchlist10": [],
            "stats": {}, "market": {},
            "note": "全市场扫描尚未完成，完成后自动出榜。",
            "progress": {"phase": st.get("phase"), "done": st.get("done"), "total": st.get("total")},
        }

    snap_map = engine.snap_index()
    temp, tdet = engine.market_temp()
    rows_cache = {}
    items, seen = [], set()
    veto = {}

    def _veto(k):
        veto[k] = veto.get(k, 0) + 1

    for p in (rs.get("pools") or []):
        for r0 in (p.get("stocks") or []):
            code = r0.get("code")
            if not code or code in seen:
                continue
            seen.add(code)
            rows = rows_cache.get(code) or engine.kline_get(code)
            if not rows:
                continue
            rows_cache[code] = rows
            s = snap_map.get(code) or {}
            f = engine.features(code, rows, s)
            if not f:
                continue
            vp = engine.volprice(f)
            vps, _vplv, _ev, _adv = engine._dim_volprice(f, vp)

            lk = r0.get("level_key") or "pos2"
            chan_grade = r0.get("chan_grade")
            gate = vp.get("gate")
            liq = f.get("amount_today") or 0

            # 硬性否决（7 道）：命中任一即不进榜，但计入否决分布给用户看
            bad = None
            if lk == "pos1":
                bad = "追高位（偏离 MA20 超 15%）"
            elif chan_grade == "risk":
                bad = "缠论结构走坏"
            elif gate == "block":
                bad = "量价警示（放量下跌/天量滞涨/长上影）"
            elif liq < 5e7:
                bad = "流动性不足（成交额 < 5000 万）"
            if bad:
                _veto(bad)
                continue

            mat_score = r0.get("score") or 0
            chan_score = r0.get("chan_score") or 60
            comp = round(mat_score * 0.45 + chan_score * 0.35 + vps * 0.20, 1)
            items.append({
                "code": code, "secid": r0.get("secid") or engine.secid_of_code(code),
                "name": r0.get("name") or "", "board": r0.get("board"),
                "industry": r0.get("industry") or "--",
                "price": r0.get("price"), "pct": r0.get("pct"),
                "level": r0.get("level"), "level_key": lk,
                "score": mat_score, "chan_score": chan_score,
                "chan_grade": chan_grade, "vp_score": vps, "gate": gate,
                "composite": comp, "dev": r0.get("dev20"),
                "amount": round(liq, 0), "maform": _maform(f),
                "tactics": (r0.get("tags") or [""])[0],
                "plan": r0.get("plan") or {},
            })

    items.sort(key=lambda x: -x["composite"])
    pos3 = [x for x in items if x["level_key"] == "pos3"]
    pos2 = [x for x in items if x["level_key"] == "pos2"]
    top10, watch10 = pos3[:10], pos2[:10]

    avg = round(sum(x["composite"] for x in items) / len(items), 1) if items else None

    # 涨停池只用来给页面加一点环境温度，取不到就让前端显示 "--"，不影响榜单
    zt = dt_ = streak = None
    # 注意：engine.latest_trade_date() 给的是 YYYY-MM-DD，而东财涨停池接口吃 YYYYMMDD，
    # 直接传会静默返回空池（zt=0），所以这里必须去横线。
    ymd = str(td).replace("-", "")
    try:
        d = _fetch_pool("zt", ymd)
        arr = d.get("pool") or []
        zt = d.get("tc") or len(arr)
        streak = max([int(x.get("lbc") or 1) for x in arr] or [1]) if arr else None
        d = _fetch_pool("dt", ymd)
        dt_ = d.get("tc") or len(d.get("pool") or [])
    except Exception:
        pass

    out = {
        "ok": True, "trade_date": rs.get("trade_date") or td,
        "generated_at": rs.get("updated") or time.strftime("%Y-%m-%d %H:%M:%S"),
        "source": "本机全市场实算（新浪快照 + 日 K 复算）",
        "stale": rs.get("trade_date") != td,
        "top10": top10, "watchlist10": watch10,
        "stats": {
            "universe": len(snap_map),
            "scanned": (rs.get("stats") or {}).get("candidates") or len(seen),
            "stage3": len(pos3),
            "passed": len(items),
            "top10_shown": len(top10),
            "watch_shown": len(watch10),
            "avg_composite": avg,
            "elapsed_s": rs.get("elapsed"),
            "veto_breakdown": veto,
        },
        "market": {
            "temp": round(temp, 1),
            "temp_label": ("过热" if temp >= 70 else ("偏暖" if temp >= 55
                          else ("中性" if temp >= 40 else "偏冷"))),
            "limit": {"zt": zt, "dt": dt_, "max_streak": streak},
            "amount_yi": round(tdet.get("amount_yi") or 0, 0),
            "source": (rs.get("market") or {}).get("phase") or "--",
        },
        "note": (rs.get("market") or {}).get("verdict") or "",
        "disclaimer": rs.get("disclaimer") or "",
    }
    _CMP_CACHE["ts"] = now
    _CMP_CACHE["td"] = td
    _CMP_CACHE["data"] = out
    return out


# ----------------------------------------------------------------------------
# 主力建仓雷达（buildup）
#
# 候选口径：与「右侧六池」相反 —— 建仓雷达找的是还在低位的票，所以不能用
# 右侧候选集（那是已经走出来的强势股，天然不含吸筹阶段）。这里直接走全市场
# 快照，按当日成交额降序取前 N 只（成交额太小的票既没有主力也没有流动性，
# 算了也是噪声），先用 52 周位置做粗筛（pos52 > 45 直接跳过，省掉筹码计算），
# 再算六维确认。日 K 全部来自扫描阶段已落盘的磁盘缓存，不额外请求外网。
# ----------------------------------------------------------------------------

_BUILDUP_CACHE = {"ts": 0.0, "td": None, "data": None}
BUILDUP_UNIVERSE = 1500     # 按成交额取前 N 只；全市场 5500 只里长尾部分无主力可言


def api_buildup(params):
    """主力建仓雷达：低位 + 温和放量 + 筹码集中，按建仓分排序。"""
    td = engine.latest_trade_date()
    force = _truthy(params.get("force"))
    now = time.time()
    if not force and _BUILDUP_CACHE["data"] and _BUILDUP_CACHE["td"] == td \
            and now - _BUILDUP_CACHE["ts"] < 300:
        return _BUILDUP_CACHE["data"]

    snap = engine.snapshot()
    rows_all = list(snap.get("rows") or [])
    rows_all.sort(key=lambda r: -(r.get("amount") or 0))
    cands = rows_all[:BUILDUP_UNIVERSE]

    emap = engine.industry_map()
    lst, manner_cnt, chip_cnt = [], {}, {"ok": 0, "mid": 0, "no": 0}
    scanned = 0
    for r in cands:
        code = r.get("code")
        if not code:
            continue
        rows = engine.kline_get(code)
        if not rows:
            continue
        s = dict(r)
        if s.get("industry") is None:
            s["industry"] = emap.get(code) or "--"
        f = engine.features(code, rows, s)
        if not f:
            continue
        # 粗筛：位置还在 52 周下半区的才可能处于吸筹阶段
        if f.get("pos52") is None or f["pos52"] > 45:
            continue
        scanned += 1
        b = engine.buildup_one(f, rows)
        if b["score"] < 55:
            continue
        m = b.get("manner") or {}
        if m.get("key"):
            manner_cnt[m["key"]] = manner_cnt.get(m["key"], 0) + 1
        ch = b.get("chips") or {}
        if ch.get("cls") in chip_cnt:
            chip_cnt[ch["cls"]] += 1

        lv = engine.level_of(f.get("dev20"))
        plan = engine.make_plan(f, lv)
        lst.append({
            "code": code, "secid": engine.secid_of_code(code),
            "name": r.get("name") or "", "industry": s.get("industry") or "--",
            "price": round(f["close"], 2), "pct": round(f.get("pct") or 0, 2),
            "score": b["score"], "ok": b["ok"], "ok_cnt": b["ok_cnt"],
            "stage": b["stage"], "hits": b["hits"],
            "manner": m or None, "chips": ch or None,
            "pos250": round(f["pos52"], 1) if f.get("pos52") is not None else None,
            "vol_ratio": round(f.get("vol_ratio") or 0, 2) if f.get("vol_ratio") else None,
            "chg20": round(f.get("chg20"), 1) if f.get("chg20") is not None else None,
            "stop": plan.get("stop_loss"),
            "vp": {"gate": engine.volprice(f).get("gate")},
            "checkup": None,     # 12 维体检成本高，展开诊断时前端另行按需拉 /api/checkup
        })

    lst.sort(key=lambda x: (-x["score"], x["pos250"] if x["pos250"] is not None else 100))
    lst = lst[:100]

    out = {
        "ok": True, "empty": not lst,
        "trade_date": td.replace("-", "-"),
        "updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(lst), "total_scanned": scanned,
        "manner_count": manner_cnt, "chip_count": chip_cnt,
        "list": lst,
        "conditions": "建仓六维：① 52 周区间下半区 ② 量比 1.0~1.8 温和放大 "
                      "③ 站上 MA20 且偏离<8% ④ MA5 上穿 MA10 ⑤ 90% 筹码宽度≤35% "
                      "⑥ 20 日均额≥5000 万；评分≥55 入选，最多展示 100",
        "method": "候选取当日成交额前 %d 只（长尾无主力可言），先用 52 周位置粗筛，"
                  "再按六维确认计入证据条数。筹码分布为近 250 日成交量按价格摊布的"
                  "近似值（公开数据拿不到真实持仓成本），只看相对集中度，不可当精确数字。"
                  "建仓手法四型：抢筹拉升 / 打压吸筹 / 缓升吸筹 / 横盘吸筹。" % BUILDUP_UNIVERSE,
        "disclaimer": "主力行为只能推断不能确认，本表为规则化算法推算，仅供研究参考，不构成投资建议。",
    }
    _BUILDUP_CACHE["ts"] = now
    _BUILDUP_CACHE["td"] = td
    _BUILDUP_CACHE["data"] = out
    return out


# ----------------------------------------------------------------------------
# 主升浪三类买点（mainwave）
#
# 候选走全市场成交额前 N 只。这里没有用强势榜/六池做候选，是因为 A 类买点
# 判的是「今日突破」，而强势榜的四条件含 MA 多头与 MACD 红柱 —— 突破当天
# 未必立刻满足全部条件，用强势榜会系统性漏掉刚启动的那一批。
# ----------------------------------------------------------------------------

_MW_CACHE = {"ts": 0.0, "td": None, "data": None}
MW_UNIVERSE = 2000


def api_mainwave(params):
    """主升浪买点三类榜（A 突破当日 / B 回踩 5~10 日线 / C 回踩 20 日线）。"""
    td = engine.latest_trade_date()
    force = _truthy(params.get("force"))
    now = time.time()
    if not force and _MW_CACHE["data"] and _MW_CACHE["td"] == td \
            and now - _MW_CACHE["ts"] < 300:
        return _MW_CACHE["data"]

    snap = engine.snapshot()
    rows_all = list(snap.get("rows") or [])
    rows_all.sort(key=lambda r: -(r.get("amount") or 0))
    cands = rows_all[:MW_UNIVERSE]

    emap = engine.industry_map()
    A, B, C = [], [], []
    for r in cands:
        code = r.get("code")
        if not code:
            continue
        rows = engine.kline_get(code)
        if not rows:
            continue
        s = dict(r)
        if s.get("industry") is None:
            s["industry"] = emap.get(code) or "--"
        f = engine.features(code, rows, s)
        if not f:
            continue
        try:
            typ, payload = engine.mainwave_one(f, rows)
        except Exception:
            continue
        if not typ:
            continue
        {"A": A, "B": B, "C": C}[typ].append(payload)

    for lst in (A, B, C):
        lst.sort(key=lambda x: -x["score"])

    out = {
        "ok": True, "empty": not (A or B or C),
        "trade_date": td, "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(A) + len(B) + len(C),
        "buyA": A[:40], "buyB": B[:40], "buyC": C[:40],
        "scanned": len(cands),
        "method": "先定「最近一次收盘创 60 日新高」的位置，再按距今天数分档："
                  "A 类=突破当日（放量≥1.5 + 涨幅≥3% + 收阳 + 上影≤40%）；"
                  "B 类=突破后 1~10 日缩量回踩 MA5/MA10（±2%）；"
                  "C 类=突破后 1~20 日回踩 MA20（±2.5%）。"
                  "形态四型（挖坑/蓄势/助跑/井喷）按突破前 25 根的振幅、涨幅与是否跌破 MA60 判定。",
        "discipline": "买点只回答「什么时候上车」，不回答「值不值得买」——"
                      "必须与十二维体检的结论叠加：体检为回避时，买点再标准也放弃或降至最小仓。"
                      "A 类止损最近（跌破突破日最低价），C 类容错最低，仓位应逐档递减。",
        "disclaimer": "形态识别对参数敏感，同一段行情换阈值结论会变，仅供研究参考，不构成投资建议。",
    }
    _MW_CACHE["ts"] = now
    _MW_CACHE["td"] = td
    _MW_CACHE["data"] = out
    return out


# ----------------------------------------------------------------------------
# 单票建仓复核（buildupcheck） / 榜单实时刷新（compass_live）
# ----------------------------------------------------------------------------

def api_buildupcheck(params):
    """单票建仓复核：证据条数与打分口径与 /api/buildup 完全一致（同一个 buildup_one）。"""
    code, s, rows, f, err = _stock_ctx(params.get("code"), params.get("secid"))
    if err:
        return {"ok": False, "error": err}
    b = engine.buildup_one(f, rows)
    lv = engine.level_of(f.get("dev20"))
    plan = engine.make_plan(f, lv)
    return {
        "ok": b["ok"], "code": code, "name": s.get("name") or "",
        "secid": engine.secid_of_code(code),
        "stage": b["stage"], "ok_cnt": b["ok_cnt"], "score": b["score"],
        "price": round(f["close"], 2), "pct": round(f.get("pct") or 0, 2),
        "pos250": round(f["pos52"], 1) if f.get("pos52") is not None else None,
        "vol_ratio": round(f.get("vol_ratio") or 0, 2) if f.get("vol_ratio") else None,
        "chg20": round(f.get("chg20"), 1) if f.get("chg20") is not None else None,
        "date": f["date"], "ksource": "sina",
        "stop": plan.get("stop_loss"),
        "manner": b.get("manner"), "chips": b.get("chips"),
        "hits": b["hits"], "evidence": b["evidence"],
        "level_text": b["level_text"],
        "disclaimer": "主力行为只能推断不能确认，仅供研究参考，不构成投资建议。",
    }


_DR_CACHE = {"ts": 0.0, "td": None, "data": None}
_CD_CACHE = {"ts": 0.0, "td": None, "data": None}
DR_UNIVERSE = 1500


def api_dragon(params):
    """龙头气质榜：四维打分 + 退潮判定。候选走成交额前 N（龙头必然活跃）。"""
    td = engine.latest_trade_date()
    force = _truthy(params.get("force"))
    now = time.time()
    if not force and _DR_CACHE["data"] and _DR_CACHE["td"] == td \
            and now - _DR_CACHE["ts"] < 300:
        return _DR_CACHE["data"]

    snap = engine.snapshot()
    rows_all = list(snap.get("rows") or [])
    rows_all.sort(key=lambda r: -(r.get("amount") or 0))
    cands = rows_all[:DR_UNIVERSE]
    emap = engine.industry_map()

    lst, gcount, rcount = [], {}, {}
    for r in cands:
        code = r.get("code")
        if not code:
            continue
        rows = engine.kline_get(code)
        if not rows:
            continue
        s = dict(r)
        if s.get("industry") is None:
            s["industry"] = emap.get(code) or "--"
        f = engine.features(code, rows, s)
        if not f:
            continue
        try:
            it = engine.dragon_one(f, rows)
        except Exception:
            continue
        if it["score"] < 50:
            continue
        gcount[it["grade"]] = gcount.get(it["grade"], 0) + 1
        rcount[it["retreat_key"]] = rcount.get(it["retreat_key"], 0) + 1
        lst.append(it)

    lst.sort(key=lambda x: -x["score"])
    lst = lst[:100]
    out = {
        "ok": True, "empty": not lst,
        "trade_date": td, "updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(lst), "total_scanned": len(cands), "list": lst,
        "grade_count": gcount, "retreat_count": rcount,
        "conditions": "四维：带动性（阶段涨幅+流动性）/ 封板质量（涨停强度+上影）/ "
                      "形态位置（均线多头+52周位置，过高反而扣分）/ 量能资金（放量推动）；"
                      "各 0-100 等权合成，≥50 入选",
        "method": "分档：≥80 真龙 · 65~79 准龙 · 50~64 候选 · <50 伪龙。"
                  "退潮以 MA10 为生命线：跌破 MA20 为冰点、跌破 MA10 为退潮、"
                  "偏离 MA20 超 10% 且上涨为加速、其余为上升。",
        "note": "龙头榜的「12 维体检」列与买入侧口径存在结构性冲突（龙头天然高偏离、"
                "常被判回避），该列仅供参考，不作为否决信号。",
        "disclaimer": "龙头判定基于量价与涨幅的规则化推断，无席位数据与板块联动数据，仅供研究参考。",
    }
    _DR_CACHE.update({"ts": now, "td": td, "data": out})
    return out


def api_dragonlist(params):
    """龙头气质榜（前端「龙头」页签走的就是这个端点）。

    ★ 2026-11-03 修正：index.html 的 loadDragon() 调的是 api("dragonlist")，
    不是 api("dragon")。最初把四维榜挂在 dragon 上、dragonlist 返回空态，
    结果页面上什么都看不到。这里直接复用 api_dragon 的数据。
    龙虎榜（营业部席位）是另一回事，暂无数据源，见 api_dragon_seats。
    """
    return api_dragon(params)


def api_dragon_seats(params):
    """龙虎榜席位明细：暂不接入（需要营业部买卖前五的席位数据）。

    已实测 datacenter-web 的 RPT_DAILYBILLBOARD_DETAILSNEW 可拿到当日上榜
    个股与净买额，但没有拆分到营业部席位；若要完整龙虎榜，需再接
    RPT_BILLBOARD_DAILYDETAILSBUY / SELL 两张表，属增量工作。
    """
    return {
        "ok": True, "empty": True, "list": [], "count": 0,
        "trade_date": engine.latest_trade_date(),
        "note": "席位数据未接入。龙头气质榜请看「龙头」页签。",
    }


def api_chaoduan(params):
    """超短池：涨停/大阳/倍量/短多头/近新高/收强/够流动，命中≥4 条入选。"""
    td = engine.latest_trade_date()
    force = _truthy(params.get("force"))
    now = time.time()
    if not force and _CD_CACHE["data"] and _CD_CACHE["td"] == td \
            and now - _CD_CACHE["ts"] < 180:
        return _CD_CACHE["data"]

    snap = engine.snapshot()
    rows_all = list(snap.get("rows") or [])
    rows_all.sort(key=lambda r: -(r.get("amount") or 0))
    cands = rows_all[:DR_UNIVERSE]
    emap = engine.industry_map()

    lst = []
    for r in cands:
        code = r.get("code")
        if not code:
            continue
        rows = engine.kline_get(code)
        if not rows:
            continue
        s = dict(r)
        if s.get("industry") is None:
            s["industry"] = emap.get(code) or "--"
        f = engine.features(code, rows, s)
        if not f:
            continue
        try:
            it = engine.chaoduan_one(f, rows)
        except Exception:
            continue
        if it:
            lst.append(it)

    lst.sort(key=lambda x: -x["score"])
    lst = lst[:100]
    out = {
        "ok": True, "empty": not lst,
        "trade_date": td, "updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(lst), "total_scanned": len(cands), "list": lst,
        "conditions": "超短池七条：涨停/大阳(≥5%)、倍量(量比≥2)、MA5>MA10、"
                      "贴近 60 日高点(≥98%)、收强(上影≤25%)、成交额≥1 亿；命中≥4 条入选",
        "method": "超短周期只看当日强度与流动性，不看过往基本面。"
                  "A 档 ≥85 / B 档 70~84 / C 档 <70。持仓周期通常 1~3 日，破前一日低点即离场。",
        "disclaimer": "超短交易对执行力与滑点极敏感，本表仅为规则筛出的候选，不构成投资建议。",
    }
    _CD_CACHE.update({"ts": now, "td": td, "data": out})
    return out


def api_chaoduanlist(params):
    """超短池明细：与 /api/chaoduan 同源，前端按池/档分组时复用。"""
    d = api_chaoduan(params)
    return d


def api_notice(params):
    """个股公告（东财 np-anotice）。

    返回结构必须保持东财原样：前端 renderNotice 取 nt.data.list，且每条要
    codes[0].stock_code + art_code 拼详情链接 —— 所以缺 codes 的条目要过滤掉，
    否则前端 a.codes[0] 会直接抛异常，整个诊断卡报错。
    """
    code = str(params.get("code") or "").split(".")[-1].strip()
    if len(code) != 6:
        return {"ok": False, "error": "缺少合法 code", "data": {"list": []}}
    try:
        size = int(params.get("size") or 20)
    except (TypeError, ValueError):
        size = 20
    size = max(1, min(size, 50))
    url = ("https://np-anotice-stock.eastmoney.com/api/security/ann?"
           "page_size=%d&page_index=1&ann_type=A&client_source=web&stock_list=%s"
           % (size, code))
    d = cached("notice:%s:%d" % (code, size), 300,
               lambda: fetch_json(url, referer="https://data.eastmoney.com/", timeout=15))
    if not d:
        return {"ok": False, "error": "公告接口不可达", "data": {"list": []}}
    lst = ((d.get("data") or {}).get("list") or [])
    lst = [x for x in lst if x.get("codes") and x.get("art_code")]
    return {"ok": True, "code": code,
            "data": {"list": lst, "total": len(lst)},
            "source": "东财公告"}


def api_news(params):
    """个股资讯：暂无按股票过滤的免费接口，返回空态而非全市场快讯冒充。"""
    return {"ok": True, "empty": True, "data": {"list": []},
            "note": "按股票过滤的资讯源尚未接入（东财快讯接口只支持板块/栏目级，"
                    "不支持个股精确过滤，硬塞全市场快讯会误导）。公告已可用。"}


def _r(v, n=2):
    """安全四舍五入：空串 / None / 非法值 -> None（东财研报的 EPS 字段常是空串）"""
    try:
        if v is None or str(v).strip() == "":
            return None
        return round(float(v), n)
    except (TypeError, ValueError):
        return None


def api_reports(params):
    """券商研报（东财 reportapi）。

    前端两种用法（index.html:13298 / 11219 / 13272）：
      * 列表流：api("reports", {page, size})  -> 全市场最新研报，可翻页
      * 单票：  api("reports", {code, size})  -> 该股近期研报
    返回体固定 {data: [...]}，字段名必须照抄东财原始名：
      renderReportList 用 emRatingName 判买入/增持、infoCode 拼详情页链接、
      predictThisYearEps 显示 EPS 预测 —— 少一个字段不报错，但那列会静默空白。

    ★ beginTime 是必填参数，缺了直接 400（实测 2026-11-04）；不传 cb 会返回
      裸 JSON，但带 cb 时是 JSONP，必须自己剥壳。
    """
    code = str(params.get("code") or "").split(".")[-1].strip()
    try:
        size = int(params.get("size") or 25)
    except (TypeError, ValueError):
        size = 25
    size = max(1, min(size, 50))
    try:
        page = int(params.get("page") or 1)
    except (TypeError, ValueError):
        page = 1
    page = max(1, min(page, 100))

    today = time.strftime("%Y-%m-%d")
    begin = time.strftime("%Y-%m-%d", time.localtime(time.time() - 180 * 86400))
    url = ("https://reportapi.eastmoney.com/report/list?cb=cb&pageSize=%d&pageNo=%d&qType=0"
           "&beginTime=%s&endTime=%s" % (size, page, begin, today))
    if len(code) == 6:
        url += "&code=" + code

    def load():
        txt = fetch(url, referer="https://data.eastmoney.com/", timeout=20)
        i, j = txt.find("("), txt.rfind(")")
        if i >= 0 and j > i:
            txt = txt[i + 1:j]
        return json.loads(txt)

    try:
        raw = cached("reports:%s:%d:%d" % (code or "all", page, size), 300, load)
    except Exception as e:
        return {"ok": False, "error": "研报接口不可达（%s）" % type(e).__name__,
                "data": [], "source": "东财 reportapi"}

    rows = []
    for it in (raw.get("data") or []):
        if not it.get("infoCode"):
            continue
        title = it.get("title") or ""
        sn = it.get("stockName") or ""
        # 全市场列表里前端只渲染「标题 + 机构」，不显示股票名 —— 而东财标题多数
        # 是「2026年三季报点评：…」这种写法，一屏看下来分不清是哪只票。
        # 这里在数据层补一个【股票名】前缀（不改前端），个股模式不加，避免重复。
        if not code and sn and sn not in title[:14]:
            title = "【%s】%s" % (sn, title)
        rows.append({
            "title": title,
            "publishDate": (it.get("publishDate") or "")[:10],
            "orgSName": it.get("orgSName") or it.get("orgName") or "",
            "emRatingName": it.get("emRatingName") or "",
            "infoCode": it.get("infoCode"),
            "stockCode": it.get("stockCode") or "",
            "stockName": sn,
            "predictThisYearEps": _r(it.get("predictThisYearEps"), 2),
            "predictNextYearEps": _r(it.get("predictNextYearEps"), 2),
            "predictThisYearPe": _r(it.get("predictThisYearPe"), 1),
        })
    return {"ok": True, "data": rows, "hits": raw.get("hits"),
            "page": page, "size": size,
            "source": "东财 reportapi（近 180 天）",
            "updated": time.strftime("%Y-%m-%d %H:%M:%S"),
            "note": "研报观点是券商单方面判断，不代表本系统结论。"}


def api_ind15(params):
    """十五五产业映射：政策口径三层（支柱/未来/基础盘）+ 代表标的。"""
    try:
        return ind15.build()
    except Exception as e:
        return {"ok": False, "error": "%s: %s" % (type(e).__name__, e),
                "empty": True, "list": []}


_SCREEN_FS = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"
_SCREEN_FIDS = {"f3", "f2", "f6", "f8", "f9", "f20", "f23", "f115", "f114", "f5"}


def api_screen(params):
    """条件选股器：只负责「按某指标排序 + 翻 N 页」把原始行拉回来。

    ★ 分工必须看清：真正的筛选（市值/PE/PB/涨幅/换手区间、ST、科创、关键词）
      全部在**前端**做（index.html:13396-13448），后端不重复实现 ——
      否则同一条件两处判断，用户改区间时两边口径会打架。
    后端只保证两件事：字段名与东财一致（f12/f13/f14/f2/f3/f115/f23/f20/f8/f6/f100），
    以及 total / fetched 两个计数真实（前端拿它们提示「全市场 X 只 · 抓取前 Y 只」）。
    """
    fid = str(params.get("fid") or "f3")
    if fid not in _SCREEN_FIDS:
        fid = "f3"
    po = "0" if str(params.get("po") or "") == "0" else "1"
    try:
        pages = int(params.get("pages") or 2)
    except (TypeError, ValueError):
        pages = 2
    pages = max(1, min(pages, 10))
    size = 100

    base = ("https://push2.eastmoney.com/api/qt/clist/get?po=%s&np=1&fltt=2&invt=2&fid=%s"
            "&fs=%s&fields=f2,f3,f6,f8,f12,f13,f14,f20,f23,f100,f115" % (po, fid, _SCREEN_FS))

    def one(pn):
        try:
            return fetch_json(base + "&pn=%d&pz=%d" % (pn, size),
                              referer="https://quote.eastmoney.com/", timeout=20) or {}
        except Exception:
            return {}

    total, rows = 0, []
    with ThreadPoolExecutor(max_workers=min(4, pages)) as ex:
        for d in ex.map(one, range(1, pages + 1)):
            data = (d or {}).get("data") or {}
            total = total or int(data.get("total") or 0)
            diff = data.get("diff") or []
            if isinstance(diff, dict):
                diff = [diff[k] for k in sorted(diff, key=lambda x: int(x))]
            rows.extend(diff)

    if not rows:
        return {"ok": False, "error": "选股数据源不可达", "diff": [], "total": 0,
                "fetched": 0, "source": "东财 push2 clist"}

    return {
        "ok": True, "diff": rows, "total": total, "fetched": len(rows),
        "page_size": size, "pages": pages, "fid": fid, "po": po,
        "source": "东财 push2 clist（沪深京 A 股实时快照）",
        "updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "note": "本端点只做排序与翻页；市值/PE/PB/涨幅/换手/ST/科创等条件在前端过滤。",
    }


# ----------------------------------------------------------------------------
# 财务指标（条件选股器的业绩闸门）
# ----------------------------------------------------------------------------

_FUND_URL = ("https://datacenter-web.eastmoney.com/api/data/v1/get?reportName=RPT_LICO_FN_CPD"
             "&columns=SECURITY_CODE,SECURITY_NAME_ABBR,WEIGHTAVG_ROE,YSTZ,SJLTZ,XSMLL,"
             "BASIC_EPS,MGJYXJJE,UPDATE_DATE&pageSize=500&sortColumns=SECURITY_CODE&sortTypes=1")

# ★ 前端字段名是硬编码的：roe_y2025 / roe_y2024 / roe_y2023（index.html:13376）。
#   这三个年份写死在这里，2027 年披露 2026 年报后需要同步改前端与这里。
FUND_ROE_YEARS = [2025, 2024, 2023]


def _fund_periods():
    """最近四个可能的报告期（从新到旧），用于探测「已披露的最新期」"""
    lt = time.localtime()
    y, m = lt.tm_year, lt.tm_mon
    if m <= 3:
        base = (y - 1, 12, 31)
    elif m <= 6:
        base = (y, 3, 31)
    elif m <= 9:
        base = (y, 6, 30)
    else:
        base = (y, 9, 30)
    out, yy, mm, dd = [], base[0], base[1], base[2]
    for _ in range(4):
        out.append("%04d-%02d-%02d" % (yy, mm, dd))
        mm -= 3
        if mm <= 0:
            mm += 12
            yy -= 1
        dd = 31 if mm in (3, 12) else 30
    return out


def _fund_pull(report_date, workers=12):
    """拉一个报告期的全市场业绩报表 -> ({code: row}, pages)

    同一报告期同一只票可能有两条（原始 + 更正），按 UPDATE_DATE 取最新那条。
    """
    def one(pn):
        try:
            return fetch_json(_FUND_URL + "&filter=(REPORTDATE%3D%27" + report_date +
                              "%27)&pageNumber=" + str(pn),
                              referer="https://data.eastmoney.com/", timeout=25) or {}
        except Exception:
            return {}

    first = one(1)
    res = (first or {}).get("result") or {}
    pages = int(res.get("pages") or 0)
    if not pages:
        return {}, 0
    rows = list(res.get("data") or [])
    if pages > 1:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for d in ex.map(one, range(2, pages + 1)):
                rows.extend(((d or {}).get("result") or {}).get("data") or [])

    out = {}
    for r in rows:
        code = str(r.get("SECURITY_CODE") or "")
        if len(code) != 6:
            continue
        old = out.get(code)
        if old is None or str(r.get("UPDATE_DATE") or "") > str(old.get("UPDATE_DATE") or ""):
            out[code] = r
    return out, pages


def _build_fund():
    periods = _fund_periods()
    latest, latest_map, pages = None, {}, 0
    for p in periods:      # 探测最近一个真的有全市场数据的报告期（当季刚过时前一格还是空的）
        d, pg = _fund_pull(p)
        if len(d) >= 3000:
            latest, latest_map, pages = p, d, pg
            break
    if not latest:
        return {"ok": False, "error": "业绩数据源不可达", "list": [],
                "latest_period": None, "year_ends": [], "source": "东财 业绩报表"}

    annual = {}
    for y in FUND_ROE_YEARS:
        annual[y], _ = _fund_pull("%d-12-31" % y)

    lst = []
    for code, r in latest_map.items():
        eps = _r(r.get("BASIC_EPS"), 3)
        cfps = _r(r.get("MGJYXJJE"), 3)
        # 现金流比：每股经营现金流 / 每股收益（近似 经营现金流净额/净利润）
        cf_ratio = _r(cfps / eps, 2) if (eps and cfps is not None and eps != 0) else None
        item = {
            "code": code,
            "name": r.get("SECURITY_NAME_ABBR") or "",
            "period": latest,
            "roe": _r(r.get("WEIGHTAVG_ROE"), 2),
            "ystz": _r(r.get("YSTZ"), 2),
            "sjltz": _r(r.get("SJLTZ"), 2),
            "xsml": _r(r.get("XSMLL"), 2),
            "eps": eps, "cfps": cfps, "cf_ratio": cf_ratio,
        }
        for y in FUND_ROE_YEARS:
            a = (annual.get(y) or {}).get(code) or {}
            item["roe_y%d" % y] = _r(a.get("WEIGHTAVG_ROE"), 2)
        lst.append(item)

    return {
        "ok": True, "list": lst, "count": len(lst),
        "latest_period": latest, "year_ends": FUND_ROE_YEARS, "pages": pages,
        "source": "东财 业绩报表 RPT_LICO_FN_CPD（最新报告期 + 三年年报）",
        "updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "note": "ROE 为加权口径；cf_ratio = 每股经营现金流 / 每股收益（近似经营现金流净额/净利润）；"
                "金融股销售毛利率为空（无意义），按缺失处理，不做替代值填充。",
    }


def api_fund(params):
    """全市场财务指标（条件选股器的业绩闸门）。

    首次构建要拉 4 个报告期 × 约 24 页，实测 10-20 秒，之后缓存 6 小时。
    前端 pollFund 每 15 秒轮询一次、最多 10 次，所以这个耗时在预算内。
    """
    force = _truthy(params.get("refresh"))
    return cached("fund:all", 0 if force else 6 * 3600, _build_fund)


_PH_CACHE = {"ts": 0.0, "key": None, "data": None}


def api_position_health(params):
    """持仓体检（卖出侧）：达瓦斯箱体 + 江恩时间止损 + 50% 回撤法则。

    对象默认是自选股（与 index.html 的注释一致：服务端 position_health 无参时
    读的就是 watchlist.json）。也支持 codes= 临时指定，便于自选盯盘复用同一套口径。
    本端点不参与任何打分，只回答「已持有的怎么走」。
    """
    raw = str(params.get("codes") or "").strip()
    if raw:
        codes = [c.strip() for c in re.split(r"[,\s]+", raw) if c.strip()][:40]
    else:
        arr = _read_json(_state_file("watchlist.json"), []) or []
        codes = []
        seen = set()
        for x in arr:
            c = (x or {}).get("code") if isinstance(x, dict) else None
            if c and c not in seen:
                seen.add(c)
                codes.append(c)

    force = _truthy(params.get("refresh")) or _truthy(params.get("force"))
    key = ",".join(codes)
    now = time.time()
    if not force and _PH_CACHE["data"] and _PH_CACHE["key"] == key \
            and now - _PH_CACHE["ts"] < 30:
        d = dict(_PH_CACHE["data"])
        d["cached"] = True
        return d

    items, td = [], engine.latest_trade_date()
    for c in codes:
        try:
            code, s, rows, f, err = _stock_ctx(c)
            if err or not f:
                continue
            it = engine.position_health_one(f, rows, (s or {}).get("name") or "")
            it["checkup"] = None
            items.append(it)
            td = f.get("date") or td
        except Exception:
            continue

    out = {
        "ok": True, "items": items, "count": len(items),
        "cached": False,
        "computed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "trade_date": td,
        "note": "达瓦斯箱体（近 3 箱下沿最高者为移动止损线，只上移不下移）· "
                "江恩时间止损（距摆动低点 %d 个交易日为预算）· "
                "50%% 回撤法则（回撤 >%.1f%% 视为结构受损）。"
                % (engine.PH_TIME_BUDGET, engine.PH_WEAK_RETRACE),
        "disclaimer": "本页数据不参与任何打分，也不改变任何既有结论，仅供研究参考，不构成投资建议。",
    }
    _PH_CACHE["ts"] = now
    _PH_CACHE["key"] = key
    _PH_CACHE["data"] = dict(out)
    return out


def api_fib4check(params):
    """斐波那契四度体检（单票）：位 / 态 / 量 / 动 四维 + 陷阱 + 介入计划。"""
    code, s, rows, f, err = _stock_ctx(params.get("code"), params.get("secid"))
    if err:
        return {"ok": False, "error": err}
    return engine.fib4_one(f, rows)


def api_watch_sell(params):
    """自选盯盘·卖出侧纪律：与 /api/position_health 完全同一套算法（position_health_one）。

    前端要的是 {items: {code: item}} 这种按代码索引的字典（items[code] 直接取），
    不是数组 —— 与 position_health 的 items 数组是两种形状，不要混用。
    """
    codes = [c.strip() for c in re.split(r"[,\s]+", str(params.get("codes") or "")) if c.strip()][:60]
    items = {}
    for c in codes:
        try:
            code, s, rows, f, err = _stock_ctx(c)
            if err or not f:
                continue
            it = engine.position_health_one(f, rows, (s or {}).get("name") or "")
            items[code] = {
                "code": code, "name": it["name"], "price": it["price"],
                "box": it["box"], "time": it["time"], "gann": it["gann"],
                "advice": it["advice"], "advice_level": it["advice_level"],
            }
        except Exception:
            continue
    return {"ok": True, "items": items, "count": len(items),
            "updated": time.strftime("%H:%M:%S")}


# ----------------------------------------------------------------------------
# 腾飞点（taofei 榜单 / taofeicheck 单票复核）
# ----------------------------------------------------------------------------

_TF_CACHE = {"ts": 0.0, "td": None, "data": None}
TF_UNIVERSE = 2000


def api_taofei(params):
    """腾飞点榜单：七条命中 ≥5 且评分 ≥55，按最终分排序。"""
    td = engine.latest_trade_date()
    force = _truthy(params.get("force"))
    now = time.time()
    if not force and _TF_CACHE["data"] and _TF_CACHE["td"] == td \
            and now - _TF_CACHE["ts"] < 300:
        return _TF_CACHE["data"]

    snap = engine.snapshot()
    rows_all = list(snap.get("rows") or [])
    rows_all.sort(key=lambda r: -(r.get("amount") or 0))
    cands = rows_all[:TF_UNIVERSE]

    emap = engine.industry_map()
    # gate 的取值是 pass/block/watch，而前端 vp_counts 的口径是 confirm/warn/mid ——
    # 两套键名都给出，避免统计恒为 0（首版就是拿 pass 去比 confirm 的键，全 0）。
    vp_counts = {"pass": 0, "block": 0, "watch": 0, "confirm": 0, "warn": 0, "mid": 0}
    _gate2vp = {"pass": "confirm", "block": "warn", "watch": "mid"}
    lst = []
    for r in cands:
        code = r.get("code")
        if not code:
            continue
        rows = engine.kline_get(code)
        if not rows:
            continue
        s = dict(r)
        if s.get("industry") is None:
            s["industry"] = emap.get(code) or "--"
        f = engine.features(code, rows, s)
        if not f:
            continue
        try:
            t = engine.taofei_one(f, rows)
        except Exception:
            continue
        if not t:
            continue
        g = t.get("gate")
        if g in vp_counts:
            vp_counts[g] += 1
            vp_counts[_gate2vp[g]] += 1
        lst.append(t)

    lst.sort(key=lambda x: (-x["score_final"], -x["score"]))
    lst = lst[:100]

    out = {
        "ok": True, "empty": not lst,
        "trade_date": td, "updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(lst), "total_scanned": len(cands), "list": lst,
        "scanning": False,
        "vp_counts": vp_counts,
        "conditions": "腾飞七条：① MA30>MA60 ② 收盘站上双线 ③ 涨幅≥5% ④ 量比≥2 "
                      "⑤ 上影≤30% ⑥ 收盘贴近 60 日高点(≥98%) ⑦ DIF>0 且 DIF>DEA；"
                      "命中≥5 条且评分≥55 入选，最多 100 只",
        "vp_note": "评分 score 为七条原始分，score_final 为经量价三分区闸门加减后的最终分"
                   "（警示 → 扣 15 分，确认 → 加 3 分）。MA10 为操盘线："
                   "收盘打穿 3 日不收复应减仓。",
        "disclaimer": "腾飞点只保证形态成立，不保证后续空间；须与十二维体检结论叠加使用，仅供研究参考。",
    }
    _TF_CACHE["ts"] = now
    _TF_CACHE["td"] = td
    _TF_CACHE["data"] = out
    return out


def api_taofeicheck(params):
    """单票腾飞点复核：无论是否入选都回七条明细，便于看清哪一条没过。"""
    code, s, rows, f, err = _stock_ctx(params.get("code"), params.get("secid"))
    if err:
        return {"ok": False, "error": err}
    t = engine.taofei_one(f, rows, strict=False)
    if not t:
        return {"ok": False, "error": "日 K 数据不足，无法判定七条"}
    t["ok"] = t["hits"] >= 5 and t["score"] >= engine.TF_MIN_SCORE
    t["pass_note"] = ("通过腾飞七条（命中 %d/7，评分 %d）" % (t["hits"], t["score"])) \
        if t["ok"] else ("未通过：命中 %d/7 条，评分 %d（门槛 5 条 / 55 分）"
                         % (t["hits"], t["score"]))
    return t


def api_compass_live(params):
    """罗盘/主升浪榜单的实时价格刷新：只回 code/price/pct，前端就地改文本。"""
    codes = [c.strip() for c in re.split(r"[,\s]+", str(params.get("codes") or "")) if c.strip()][:150]
    if not codes:
        return {"ok": True, "list": [], "updated": time.strftime("%H:%M:%S")}
    syms = [sym_of_code(c) for c in codes]
    try:
        raw = tx_raw(syms)
    except Exception as e:
        return {"ok": False, "error": "行情源不可达：%s" % e, "list": []}
    lst = []
    for c, sym in zip(codes, syms):
        a = raw.get(sym)
        if not a:
            continue
        try:
            price = _f(a, 3)
            pct = round(_f(a, 32), 2)
        except Exception:
            continue
        if not price:
            continue
        lst.append({"code": c, "price": price, "pct": pct})
    return {"ok": True, "list": lst, "count": len(lst),
            "updated": time.strftime("%H:%M:%S")}


def api_checkup_top(params):
    """体检页「全市场策略适配分排行」：复用右侧扫描产出的体检结论。"""
    if _truthy(params.get("status")):
        st = engine.scan_state()
        return {"ok": True, "running": st.get("running"), "phase": st.get("phase"),
                "done": st.get("done"), "total": st.get("total")}
    if _truthy(params.get("refresh")):
        engine.start_scan("checkup", "checkup_top")
        return {"ok": True, "started": True, "running": True}
    d = engine.checkup_top()
    if not d.get("items"):
        engine.ensure_scan_async("checkup", "checkup_top-empty")
        d["scanning"] = engine.scan_running()
    return d


# --- 量价扫描榜（量价分析页） -------------------------------------------------
# 收盘后按成交额取前 N 只算八态量价 + 警示/确认标签，分三档出榜。
# 不进 engine 的全局扫描（那套是右侧六池 / 体检用的），这里是一次轻量即算，
# 结果按「交易日 + 5 分钟」缓存，rm 页点「重新扫描」才强制重算。
_VP_CACHE = {"key": "", "at": 0.0, "data": None}
_VP_LOCK = threading.Lock()
VP_POOL = 800
VP_KEEP = {"warn": 40, "confirm": 40, "mid": 20}


def api_volprice(params):
    td = engine.latest_trade_date()
    key = "%s|%s" % (td, time.strftime("%Y-%m-%dT%H"))
    now = time.time()
    with _VP_LOCK:
        c = _VP_CACHE
        if (not _truthy(params.get("refresh")) and c["data"]
                and c["key"] == key and now - c["at"] < 300):
            return c["data"]

    snap = engine.snap_index()
    rows = [r for r in snap.values()
            if engine.is_a_share(r["code"]) and not engine.is_st(r.get("name") or "")
            and (r.get("price") or 0) > 0]
    rows.sort(key=lambda r: -((r.get("amount") or 0) or (r.get("nmc_yi") or 0) * 1e8))
    rows = rows[:VP_POOL]

    board = {"warn": [], "confirm": [], "mid": []}
    ok = failed = 0
    for sr in rows:
        code = sr["code"]
        kl = engine.kline_get(code)
        if not kl or len(kl) < 70:
            failed += 1
            continue
        f = engine.features(code, kl, sr)
        if not f:
            failed += 1
            continue
        ok += 1
        vp = engine.volprice(f)
        sigs = ([{"name": s["name"], "level": "neg"} for s in vp["warn"]]
                + [{"name": s["name"], "level": "pos"} for s in vp["confirm"]]
                + [{"name": s["name"], "level": "neu"} for s in vp["mid"]])
        if not sigs:
            continue          # 八态里没命中任何标签的票不上榜，榜单只放「有话说」的
        g = vp["gate"]
        bucket = "warn" if g == "block" else ("confirm" if g == "pass" else "mid")
        board[bucket].append({
            "code": code, "name": sr.get("name") or code,
            "secid": engine.secid_of_code(code),
            "src": "成交额前 %d" % VP_POOL,
            "price": _r(f.get("close")), "pct": _r(f.get("pct")),
            "vp8": "%s（%s%s）" % (vp["vp8"]["name"], vp["vp8"]["price_dir"],
                                  vp["vp8"]["vol_dir"]),
            "stage": vp["stage"], "advice": vp["advice"],
            "signals": sigs,
            "gate": g,
            "vol_ratio": _r(f.get("vol_ratio")),
            "dev20": _r(f.get("dev20")),
        })

    for k in board:
        board[k].sort(key=lambda x: (-len(x["signals"]), -abs(x["pct"] or 0)))
        board[k] = board[k][:VP_KEEP[k]]

    total = sum(len(board[k]) for k in board)
    out = {
        "ok": ok, "failed": failed, "updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "trade_date": td, "board": board, "count": total,
        "note": ("" if total else
                 "扫描了成交额前 %d 只，量价标签均未触发（八态都在中性区），暂无榜单。"
                 % VP_POOL),
    }
    with _VP_LOCK:
        _VP_CACHE.update({"key": key, "at": now, "data": out})
    return out


# --- 桌面客户端：优雅退出 -----------------------------------------------------
# desktop.py 关窗时调 /api/shutdown，而不是 terminate 子进程 —— 强杀可能正好落在
# write_json 中间，留下半个 JSON（快照/榜单），下次启动读到坏文件还得重抓一遍。
_SRV = [None]


def api_shutdown(params=None):
    """关闭服务（只接受 POST，且服务本身只绑 127.0.0.1）。"""
    srv = _SRV[0]
    if srv is None:
        return {"ok": False, "error": "服务尚未就绪"}
    # shutdown() 会等 serve_forever 的循环退出，必须在另一个线程里调，
    # 否则当前这个请求线程会和它互相等待。
    threading.Thread(target=srv.shutdown, daemon=True).start()
    return {"ok": True, "message": "服务正在退出"}


def api_rescan(params):
    """全市场重扫：不带 status 触发、带 status=1 查进度（与前端轮询协议一致）。

    ★ 2026-11-05：状态分支必须回 trade_date —— 前端每个页签标题旁的「⟳ 重新扫描」
    工具条靠它渲染「数据 <b>2026-11-04</b>」，之前不回这个字段，全站十几个页签
    一致显示「数据 --」，看起来像数据缺失，实际只是协议缺一个键。
    战法 / 复盘是独立模块（自带构建线程与进度），target 命中时走它们自己的入口，
    不塞进 engine 的全市场扫描状态里，否则两个进度会互相覆盖。
    """
    target = params.get("target") or "all"
    # lhb = 短线页的龙虎榜刷新（约 3~5 秒），走 short 自己的进度状态，
    # 不塞进 engine 的全市场扫描状态，否则会和其它页签的进度互相覆盖。
    local = {"tactics": tactics, "review": review, "lhb": short}.get(target)
    if _truthy(params.get("status")):
        st = engine.scan_state()
        lst = local.status() if local else {}
        running = bool(lst.get("building")) or bool(st.get("running"))
        return {"ok": True, "running": running,
                "phase": lst.get("phase") or st.get("phase"),
                "done": lst.get("done") or st.get("done"),
                "total": lst.get("total") or st.get("total"),
                "error": lst.get("error") or st.get("error"),
                "msg": lst.get("phase") or st.get("phase") or "",
                "target": target,
                "trade_date": engine.latest_trade_date(),
                "started_at": st.get("started_at"), "finished_at": st.get("finished_at"),
                "last": st.get("last")}
    if local:
        if local.status().get("building"):
            return {"ok": True, "started": False, "running": True, "target": target,
                    "message": "重扫已在进行中"}
        threading.Thread(target=local._build, kwargs={"force": True}, daemon=True).start()
        return {"ok": True, "started": True, "running": True, "target": target,
                "message": "已启动重扫（约 3~30 秒）"}
    started = engine.start_scan(target, "ui")
    st = engine.scan_state()
    return {"ok": True, "started": started, "running": st.get("running"),
            "phase": st.get("phase"), "target": target,
            "trade_date": engine.latest_trade_date(),
            "message": ("已启动全市场扫描（约 2 分钟）" if started else "扫描已在进行中")}


def api_scan_status(params):
    st = engine.scan_state()
    return {"ok": True, "state": st,
            "strong_count": (engine.load_strong() or {}).get("count"),
            "rightside_updated": (engine.load_rightside() or {}).get("updated")}


def api_scores(params):
    """综合评分 Top20：复用同日强势股扫描结果（成熟度 6 : 缠论 4）。"""
    try:
        n = int(params.get("n") or 20)
    except (TypeError, ValueError):
        n = 20
    d = engine.scores_top(n)
    if not d.get("list"):
        engine.start_scan("scores", "scores-api")
    return d


def api_stub(name, params):
    note = NOT_IMPLEMENTED.get(name) or "该模块"
    return {
        "ok": True, "stub": True, "updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "trade_date": time.strftime("%Y-%m-%d"),
        "note": "复刻版未接入「%s」的数据引擎，需要在此实现。" % note,
        "list": [], "items": [], "rows": [], "data": [], "cards": [],
        "count": 0, "total": 0, "pools": {}, "warnings": [],
    }


# ----------------------------------------------------------------------------
# 路由
# ----------------------------------------------------------------------------

GET_ROUTES = {    "health": lambda p: {"ok": True, "service": "rightside-compass-replica",
                         "name": "A股右侧交易罗盘 · 复刻版", "port": RUNTIME_PORT[0],
                         "scan": engine.scan_state()},
    "quotes": api_quotes,
    "kline": api_kline,
    "search": api_search,
    "sentiment": api_sentiment,
    "pankou": api_pankou,
    "watchlist": api_watchlist_get,
    "archive": api_archive,
    "strong": api_strong,
    "rightside": api_rightside,
    "checkup": api_checkup,
    "chanscore": api_chanscore,
    "volcheck": api_volcheck,
    "chan": api_chan,
    "chanlist": api_chanlist,
    "compass": api_compass,
    "buildup": api_buildup,
    "mainwave": api_mainwave,
    "buildupcheck": api_buildupcheck,
    "compass_live": api_compass_live,
    "fib4check": api_fib4check,
    "position_health": api_position_health,
    "watch_sell": api_watch_sell,
    "taofei": api_taofei,
    "taofeicheck": api_taofeicheck,
    "dragon": api_dragon,
    "dragonlist": api_dragonlist,
    "dragon_seats": api_dragon_seats,
    "notice": api_notice,
    "news": api_news,
    "reports": api_reports,
    "ind15": api_ind15,
    "screen": api_screen,
    "fund": api_fund,
    "gold": gold.api_gold,
    "gold_kb": gold.api_gold_kb,
    "knowledge": knowledge.api_knowledge,
    "review": review.api_review,
    "edge": edge.api_edge,
    "short": short.api_short,
    "tactics": tactics.api_tactics,
    "tacticscheck": tactics.api_tacticscheck,
    "liveradar": liveradar.api_liveradar,
    "liveradarstart": liveradar.api_liveradarstart,
    "volprice": api_volprice,
    "chaoduan": api_chaoduan,
    "chaoduanlist": api_chaoduanlist,
    "checkup_top": api_checkup_top,
    "rescan": api_rescan,
    "scan_status": api_scan_status,
    "scores": api_scores,
}

POST_ROUTES = {
    "watchlist": api_watchlist_post,
    "save_report": api_save_report,
    # ★ 桌面端关窗时靠它优雅停服务。api_shutdown 写好了却漏登记在这里，
    # 结果 POST 落到「未实现该写入接口」的兜底分支 —— 桌面端只能硬杀 Python，
    # 强杀可能正好落在 write_json 中途，留下半截 JSON。
    "shutdown": api_shutdown,
}


class Handler(SimpleHTTPRequestHandler):
    server_version = "RightsideCompassReplica/1.0"

    def __init__(self, *a, **kw):
        super().__init__(*a, directory=BASE_DIR, **kw)

    # --- 工具 ---
    # 客户端先断开（关页面 / 切页签 / 强刷 / 浏览器预连接取消）时，往 socket 写会抛这三个。
    # 以前不接：_json 抛 → 兜底再写 500 又抛 → 异常冒到 socketserver，满屏 traceback，
    # 严重时整个后端进程直接退出。
    _BROKEN = (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)

    def _json(self, obj, code=200):
        raw = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(raw)
        except self._BROKEN:
            self.close_connection = True

    def _file(self, blob, filename, mime):
        """二进制下载：前端会先校验 Content-Type 再 a[download]，
        所以这里必须给对 wordprocessingml，并且文件名走 RFC 5987 的 filename*="""
        from urllib.parse import quote
        try:
            self.send_response(200)
            self.send_header("Content-Type", mime)
            self.send_header("Content-Length", str(len(blob)))
            self.send_header("Content-Disposition",
                             "attachment; filename=\"report.docx\"; filename*=UTF-8''%s"
                             % quote(filename))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(blob)
        except self._BROKEN:
            self.close_connection = True

    def _body(self):
        try:
            n = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            return {}
        if n <= 0:
            return {}
        try:
            return json.loads(self.rfile.read(n).decode("utf-8"))
        except Exception:
            return {}

    def _dispatch(self, path, params, is_post):
        if not path.startswith("/api/"):
            return False
        name = path[5:].strip("/")
        try:
            if is_post:
                # Word 导出是唯一需要返回二进制的写接口：前端「⬇ 下载 Word」两处都打这里
                if name == "report_docx":
                    try:
                        blob, fname = report_docx.api_report_docx(self._body())
                    except ValueError as e:
                        self._json({"ok": False, "error": str(e)}, 400)
                        return True
                    self._file(blob, fname,
                               "application/vnd.openxmlformats-officedocument."
                               "wordprocessingml.document")
                    return True
                fn = POST_ROUTES.get(name)
                if fn:
                    self._json(fn(self._body()))
                    return True
                self._json({"ok": False, "error": "复刻版未实现该写入接口：%s" % name}, 404)
                return True
            fn = GET_ROUTES.get(name)
            if fn:
                self._json(fn(params))
            else:
                self._json(api_stub(name, params))
        except Exception as e:
            # 兜底的 500 本身也可能写不出去（客户端已断），别让它再抛一次
            try:
                self._json({"ok": False, "error": "%s: %s" % (type(e).__name__, e)}, 500)
            except Exception:
                pass
        return True

    # --- 请求入口 ---
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = {k: v[0] for k, v in urllib.parse.parse_qs(parsed.query).items()}
        if self._dispatch(parsed.path, params, False):
            return
        # 静态文件：默认无缓存，方便改前端立即可见
        self.path = parsed.path
        try:
            super().do_GET()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        params = {k: v[0] for k, v in urllib.parse.parse_qs(parsed.query).items()}
        if not self._dispatch(parsed.path, params, True):
            self._json({"ok": False, "error": "仅支持 /api/* 的 POST"}, 405)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def end_headers(self):
        # 前端改动频繁：HTML/JS/CSS 强制走协商缓存
        if self.path.endswith((".html", "/", ".js", ".css")):
            self.send_header("Cache-Control", "no-cache, must-revalidate")
        super().end_headers()

    def log_message(self, fmt, *args):
        msg = fmt % args
        if "/api/" in msg or " 5" in msg or " 4" in msg:
            sys.stderr.write("[%s] %s\n" % (time.strftime("%H:%M:%S"), msg))


def main():
    port = DEFAULT_PORT
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            pass
    port = int(os.environ.get("PORT", port))
    if not os.path.exists(os.path.join(BASE_DIR, "index.html")):
        print("找不到 index.html，请确认 server.py 与 index.html 在同一目录。")
        sys.exit(1)
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    srv.daemon_threads = True
    RUNTIME_PORT[0] = port
    # ★ 交给 api_shutdown 用。之前只写了 api_shutdown 却忘了在这里登记实例，
    # 它永远走「服务尚未就绪」分支 —— 桌面端关窗时拿不到优雅退出，只能硬杀。
    _SRV[0] = srv
    print("A股右侧交易罗盘 · 复刻版")
    print("  本地地址: http://127.0.0.1:%d" % port)
    print("  数据源  : 腾讯行情 / 新浪日线与快照 / 东财搜索与涨跌停池")
    print("  引擎    : 强势股扫描 · 右侧六池 · 十二维体检（首次扫描约 2 分钟，之后按交易日缓存）")
    print("  Ctrl+C 退出")

    # 启动即补扫：缓存不是当前交易日就在后台跑一遍，页面一打开就有数
    def _auto_update():
        """常驻自动更新：把「启动扫一次」升级成「一直跟着交易日走」。

        原站的行为是「收盘后 + 数据日期不是今天」才补扫。复刻版之前只在服务启动时
        扫一次，进程长开跨天后就再也不更新 —— 第二天打开页面看到的还是前天的榜单。

        这里每 60 秒做一件很轻的事：调 `ensure_scan_async`，它内部只比较
        strong/rightside 的 trade_date 与 latest_trade_date()，相等就什么都不做。
        所以盘中（td 还没变）不会打扰任何人；一旦收盘后日 K 更新、td 推进，
        下一次检查就会自动起一轮全市场扫描。
        复盘 / 战法两个模块各自按交易日缓存，td 变了它们会自己重建，无需在这里管。
        """
        time.sleep(2.0)
        last_td = None
        last_msg = 0.0
        while True:
            try:
                td = engine.latest_trade_date()
                if td != last_td:
                    print("  [引擎] 交易日 → %s" % td)
                    last_td = td
                if engine.ensure_scan_async("all", "auto"):
                    print("  [引擎] 数据落后于交易日 %s，已自动启动全市场扫描 …" % td)
                else:
                    now = time.time()
                    if now - last_msg > 3600:      # 每小时报一次平安，别刷屏
                        s = engine.load_strong() or {}
                        print("  [引擎] 自动更新：%s，榜单已是 %s 的数据，无需重扫"
                              % (time.strftime("%H:%M"), s.get("trade_date") or "—"))
                        last_msg = now
            except Exception as e:
                print("  [引擎] 自动更新异常：", e)
            time.sleep(60)

    threading.Thread(target=_auto_update, name="autoupdate", daemon=True).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n已退出")
        srv.shutdown()


if __name__ == "__main__":
    main()
