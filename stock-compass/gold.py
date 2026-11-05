#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
黄金专区（金石为开）数据引擎

三个页签共用本模块：
  1. 沪金十二维体检   -> /api/gold       （checkup / quote / live_bar / oi_structure / term_structure）
  2. 黄金标的池       -> /api/gold 同源   （pool / signal_pool / pool_summary）
  3. 体系溯源         -> /api/gold_kb    （books / new_dims / pitfalls / not_found）

口径红线（前端 index.html:19353-19391 有明确要求，改这里必须先读那段注释）
--------------------------------------------------------------------------
* 实时行情条 = 盘中实时；十二维体检 = **只用已收盘日K**。两者必须分开显示两个时间，
  否则用户会以为体检结论用的是盘中未收盘价。
* 涨跌幅按期货惯例以**昨结算价**为基准（不是昨收盘），基准名随数据一起返回。
* 夜盘 21:00-02:30 的成交按上期所规则归**次日**交易日。
* 持仓量（OI）是期货独有维度，股票侧不可复现 —— 所以标的池只回答「能不能做放大器」，
  不回答「该不该买」。

数据源（全部公开免费，2026-11-03 实测可用）
-------------------------------------------
  新浪 hq.sinajs.cn        nf_AU0 / nf_AU2612 沪金连续与各月合约（GBK）
                           hf_GC / hf_XAU / hf_XAG 纽约金 / 伦敦金 / 伦敦银（美元/盎司）
                           fx_susdcny 在岸人民币 · sh518880 黄金ETF华安
  新浪 InnerFuturesNewService.getDailyKLine   沪金日K（含持仓量 p、结算价 s）
  新浪 GlobalFuturesService.getGlobalFuturesDailyKLine 外盘日线（算金银比）
  东财 push2his kline       美元指数 UDI 日线

只依赖标准库 + engine（复用本地 A 股日K缓存做标的池相关性）。
"""

import json
import math
import re
import ssl
import threading
import time
import urllib.request

import engine

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

_SSL = ssl.create_default_context()
_SSL.check_hostname = False
_SSL.verify_mode = ssl.CERT_NONE

OZ_TO_GRAM = 31.1035        # 1 金衡盎司 = 31.1035 克
AU_LOT_GRAM = 1000          # 上期所黄金 1000 克/手
MIN_PCORR = 0.40            # 标的池准入线：core 层且偏相关 ≥ 0.40 才可作交易腿

# ----------------------------------------------------------------------------
# 基础设施
# ----------------------------------------------------------------------------

_cache = {}
_cache_lock = threading.Lock()


def _cached(key, ttl, producer):
    now = time.time()
    with _cache_lock:
        hit = _cache.get(key)
        if hit and ttl > 0 and now - hit[0] < ttl:
            return hit[1]
    val = producer()
    with _cache_lock:
        _cache[key] = (now, val)
    return val


def _get(url, encoding="utf-8", referer="https://finance.sina.com.cn", timeout=15):
    req = urllib.request.Request(url, headers={
        "User-Agent": UA, "Referer": referer, "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
    })
    with urllib.request.urlopen(req, timeout=timeout, context=_SSL) as r:
        return r.read().decode(encoding, "ignore")


def _f(v, default=None):
    """安全转 float：空串/None/非法值 -> default"""
    try:
        if v is None:
            return default
        s = str(v).strip()
        if s == "" or s == "-":
            return default
        return float(s)
    except (TypeError, ValueError):
        return default


def _r(v, n=2):
    if v is None:
        return None
    try:
        return round(float(v), n)
    except (TypeError, ValueError):
        return None


def _clamp(v, lo=0.0, hi=100.0):
    return max(lo, min(hi, v))


# ----------------------------------------------------------------------------
# 统计工具
# ----------------------------------------------------------------------------

def _mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def _sd(xs):
    if len(xs) < 2:
        return 0.0
    m = _mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


def _corr(xs, ys):
    n = min(len(xs), len(ys))
    if n < 5:
        return None
    xs, ys = xs[-n:], ys[-n:]
    mx, my = _mean(xs), _mean(ys)
    sxy = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    sxx = sum((v - mx) ** 2 for v in xs)
    syy = sum((v - my) ** 2 for v in ys)
    if sxx <= 0 or syy <= 0:
        return None
    return sxy / math.sqrt(sxx * syy)


def _pct_rank(xs, v):
    """v 在 xs 中的百分位（0-100）"""
    if not xs:
        return None
    n = len([x for x in xs if x <= v])
    return n / len(xs) * 100.0


def _returns(closes):
    return [closes[i] / closes[i - 1] - 1 for i in range(1, len(closes)) if closes[i - 1]]


def _rsi(closes, n=14):
    if len(closes) < n + 1:
        return None
    seg = closes[-(n + 1):]
    up = dn = 0.0
    for i in range(1, len(seg)):
        d = seg[i] - seg[i - 1]
        if d > 0:
            up += d
        else:
            dn -= d
    if up + dn == 0:
        return 50.0
    rs = up / dn if dn else 999.0
    return 100.0 - 100.0 / (1.0 + rs)


def _ma(vals, n):
    if len(vals) < n:
        return None
    return sum(vals[-n:]) / n


# ----------------------------------------------------------------------------
# 行情解析
# ----------------------------------------------------------------------------

_NF_FIELDS = ("name time open high low close bid ask last settle prev_settle "
              "bidvol askvol oi volume exch product date").split()


def _nf_parse(line):
    """新浪内盘期货 hq_str_nf_XXX 一行 -> dict

    实测字段（2026-11-03，nf_AU2612）：
      0 名称 1 时间HHMMSS 2 开 3 高 4 低 5 收盘/最新 6 买 7 卖 8 最新
      9 今结算 10 昨结算 11 买量 12 卖量 13 持仓量 14 成交量 15 交易所 16 品种 17 日期
    校验依据：10(昨结算 839.180) 与日K上一根 s 值一致；8(841.800)/14(271886)/13(160467)
    与当日日K的 c/v/p 完全一致。
    """
    m = re.search(r'="([^"]*)"', line or "")
    if not m or not m.group(1).strip():
        return None
    f = m.group(1).split(",")
    if len(f) < 18:
        return None
    code = ""
    mm = re.search(r'hq_str_nf_([A-Za-z0-9]+)=', line)
    if mm:
        code = mm.group(1)
    last = _f(f[8])
    t = (f[1] or "").strip()
    if len(t) == 6 and t.isdigit():
        t = "%s:%s:%s" % (t[:2], t[2:4], t[4:])
    q = {
        "code": code, "name": f[0].strip(), "time": t, "date": (f[17] or "").strip(),
        "open": _f(f[2]), "high": _f(f[3]), "low": _f(f[4]),
        "last": last if last else None,
        "bid": _f(f[6]), "ask": _f(f[7]),
        "settle": _f(f[9]), "prev_settle": _f(f[10]),
        "oi": int(_f(f[13], 0) or 0), "volume": int(_f(f[14], 0) or 0),
        "exchange": f[15].strip() if len(f) > 15 else "",
    }
    q["night_session"] = _is_night(t)
    return q


def _is_night(t):
    """上期所黄金夜盘 21:00-02:30

    ★ 必须按数值比较：新浪的时间字段可能出现 "9:11:34"（无前导零），
    字符串比大小会把 "9:11" >= "20:55" 判成 True，导致日盘被误标成夜盘。
    """
    m = re.match(r"^(\d{1,2}):(\d{2})", (t or "").strip())
    if not m:
        return False
    mins = int(m.group(1)) * 60 + int(m.group(2))
    return mins >= 20 * 60 + 55 or mins <= 2 * 60 + 35


def _mins(t):
    """'09:11:34' / '9:11:34' -> 551；无法解析 -> None"""
    m = re.match(r"^(\d{1,2}):(\d{2})", (t or "").strip())
    if not m:
        return None
    return int(m.group(1)) * 60 + int(m.group(2))


def _nf_batch(symbols):
    """批量取内盘期货行情（一次请求多个合约，省往返）"""
    if not symbols:
        return {}
    url = "https://hq.sinajs.cn/list=" + ",".join("nf_" + s for s in symbols)
    txt = _get(url, encoding="gbk")
    out = {}
    for line in txt.split(";"):
        if "hq_str_nf_" not in line:
            continue
        q = _nf_parse(line)
        if q and q.get("code"):
            out[q["code"]] = q
    return out


def _hf_parse(line):
    """新浪外盘 hf_XXX（美元/盎司）

    实测 hf_GC：0 最新 1 昨结(可空) 2 买 3 卖 4 高 5 低 6 时间 7 昨收 8 开 ... 12 日期 13 名称
    """
    m = re.search(r'="([^"]*)"', line or "")
    if not m or not m.group(1).strip():
        return None
    f = m.group(1).split(",")
    if len(f) < 13:
        return None
    return {
        "last": _f(f[0]), "bid": _f(f[2]), "ask": _f(f[3]),
        "high": _f(f[4]), "low": _f(f[5]), "time": (f[6] or "").strip(),
        "prev_close": _f(f[7]), "open": _f(f[8]),
        "date": (f[12] or "").strip(), "name": f[13].strip() if len(f) > 13 else "",
    }


def _fx_cny():
    """在岸人民币 USDCNY

    实测 fx_susdcny：0 时间 1 买 2 卖 3 昨收 5 开 6 高 7 低 8 最新 9 名称 10 涨跌幅%
    """
    try:
        txt = _get("https://hq.sinajs.cn/list=fx_susdcny", encoding="gbk")
    except Exception:
        return None
    m = re.search(r'="([^"]*)"', txt)
    if not m or not m.group(1).strip():
        return None
    f = m.group(1).split(",")
    if len(f) < 9:
        return None
    last = _f(f[8])
    prev = _f(f[3])
    return {
        "last": last, "prev_close": prev, "open": _f(f[5]),
        "high": _f(f[6]), "low": _f(f[7]),
        "chg_pct": _r((last / prev - 1) * 100, 3) if last and prev else _r(_f(f[10]), 3),
        "time": (f[0] or "").strip(),
    }


def _stock_quote(sym):
    """新浪 A 股/ETF 快照：0 名称 1 开 2 昨收 3 现价 4 高 5 低 8 量 9 额"""
    try:
        txt = _get("https://hq.sinajs.cn/list=" + sym, encoding="gbk")
    except Exception:
        return None
    m = re.search(r'="([^"]*)"', txt)
    if not m or not m.group(1).strip():
        return None
    f = m.group(1).split(",")
    if len(f) < 10:
        return None
    return {
        "name": f[0].strip(), "open": _f(f[1]), "prev_close": _f(f[2]),
        "last": _f(f[3]), "high": _f(f[4]), "low": _f(f[5]),
        "volume": _f(f[8], 0), "amount": _f(f[9], 0),
        "date": (f[30] or "").strip() if len(f) > 30 else "",
        "time": (f[31] or "").strip() if len(f) > 31 else "",
    }


# ----------------------------------------------------------------------------
# 日线
# ----------------------------------------------------------------------------

def _fut_daily(symbol="AU0"):
    """沪金日K -> [{d,o,h,l,c,v,oi,s}]（v=成交量 手, oi=持仓量 手, s=结算价）"""
    def load():
        # 注意：回调前缀里的 %20 是 URL 编码的空格，不能用 % 格式化拼接（%20_ 会被当成转换符）
        url = ("https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var%20_" + symbol +
               "=/InnerFuturesNewService.getDailyKLine?symbol=" + symbol)
        txt = _get(url)
        i, j = txt.find("["), txt.rfind("]")
        if i < 0 or j < 0:
            return []
        try:
            raw = json.loads(txt[i:j + 1])
        except ValueError:
            return []
        out = []
        for it in raw:
            try:
                out.append({
                    "d": it["d"], "o": float(it["o"]), "h": float(it["h"]),
                    "l": float(it["l"]), "c": float(it["c"]),
                    "v": float(it.get("v") or 0), "oi": float(it.get("p") or 0),
                    "s": float(it.get("s") or 0),
                })
            except (KeyError, TypeError, ValueError):
                continue
        return out
    return _cached("gold:dm:%s" % symbol, 300, load)


def _global_daily(symbol):
    """外盘日线（XAU / XAG）-> [{d,c}]"""
    def load():
        url = ("https://stock.finance.sina.com.cn/futures/api/jsonp.php/var%20t=/"
               "GlobalFuturesService.getGlobalFuturesDailyKLine?symbol=" + symbol)
        txt = _get(url)
        i, j = txt.find("["), txt.rfind("]")
        if i < 0 or j < 0:
            return []
        try:
            raw = json.loads(txt[i:j + 1])
        except ValueError:
            return []
        out = []
        for it in raw:
            c = _f(it.get("close"))
            d = it.get("date")
            if c and d:
                out.append({"d": d, "c": c})
        return out
    return _cached("gold:gm:%s" % symbol, 600, load)


def _dxy_daily():
    """美元指数日线（东财 100.UDI）-> [{d,c}]"""
    def load():
        url = ("https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=100.UDI"
               "&fields1=f1,f2,f3&fields2=f51,f53&klt=101&fqt=0&lmt=260&end=20500101")
        try:
            txt = _get(url, referer="https://quote.eastmoney.com/")
            d = json.loads(txt)
        except Exception:
            return []
        ks = ((d or {}).get("data") or {}).get("klines") or []
        out = []
        for s in ks:
            p = s.split(",")
            if len(p) >= 2:
                c = _f(p[1])
                if c:
                    out.append({"d": p[0], "c": c})
        return out
    return _cached("gold:dxy", 600, load)


# ----------------------------------------------------------------------------
# 主力合约识别 / 期限结构
# ----------------------------------------------------------------------------

def _month_codes(n=14):
    """从当月起，未来 n 个月的合约后缀 YYMM"""
    lt = time.localtime()
    y, m = lt.tm_year, lt.tm_mon
    out = []
    for i in range(n):
        yy = y + (m - 1 + i) // 12
        mm = (m - 1 + i) % 12 + 1
        out.append("%02d%02d" % (yy % 100, mm))
    return out


_LAST_MAIN = {"info": None}


def _resolve_main(notes):
    """用持仓量识别主力合约（写死代码会在换月后静默失真，必须实时判定）"""
    codes = _month_codes(14)
    try:
        quotes = _nf_batch(["AU" + c for c in codes])
    except Exception as e:
        notes.append("各月合约行情取数失败（%s），退回沪金连续 AU0" % type(e).__name__)
        quotes = {}

    live = [q for q in quotes.values() if q.get("oi")]

    # 竞价/夜盘切换时点，个别合约会短暂返回持仓 0；缩到近 6 个月再试一次
    if not live:
        try:
            quotes2 = _nf_batch(["AU" + c for c in codes[:6]])
            live = [q for q in quotes2.values() if q.get("oi")]
            if live:
                notes.append("各月全表持仓为空，改用近 6 个月合约重新识别")
        except Exception:
            pass

    if not live:
        # 仍然为空：沿用上次识别结果（换月不影响，因为每天会重新判定）
        prev = _LAST_MAIN.get("info")
        q0 = None
        try:
            q0 = _nf_batch(["AU0"]).get("AU0")
        except Exception:
            pass
        if prev:
            notes.append("各月合约持仓暂为空（竞价/夜盘切换时点），沿用上次识别的主力 %s" % prev["code"])
            return q0, dict(prev, reason=prev.get("reason", "") + "（沿用上次识别结果）"), quotes
        if q0:
            info = {"name": q0.get("name") or q0["code"], "code": q0["code"],
                    "volume": q0.get("volume") or 0, "oi": q0.get("oi") or 0,
                    "reason": "各月合约持仓暂为空，按沪金连续快照展示", "ranking": []}
            notes.append("各月合约持仓暂为空，主力退回沪金连续 AU0")
            return q0, info, quotes
        notes.append("全部合约行情缺失")
        return None, None, quotes

    live.sort(key=lambda x: -x["oi"])
    main = live[0]
    second = live[1] if len(live) > 1 else None
    gap = ""
    if second and second["oi"]:
        gap = "，较次主力 %s 高 %.0f%%" % (second["code"], (main["oi"] / second["oi"] - 1) * 100)
    ranking = [{"code": q["code"], "oi": q["oi"], "volume": q["volume"]}
               for q in live[:4]]
    info = {
        "name": main.get("name") or main["code"],
        "code": main["code"],
        "volume": main["volume"], "oi": main["oi"],
        "reason": "按各月合约持仓量比较，%s 持仓 %s 手最大%s" % (
            main["code"], format(main["oi"], ","), gap),
        "ranking": ranking,
    }
    _LAST_MAIN["info"] = info
    return main, info, quotes


def _term_structure(quotes, main, notes):
    """期限结构：主力月 vs 下一个有量的远月。只展示不打分（改权重等于改口径）。"""
    if not main or not quotes:
        return {"ok": False, "reason": "合约行情缺失"}
    rows = [q for q in quotes.values() if q.get("last") and q.get("oi")]
    if len(rows) < 2:
        return {"ok": False, "reason": "有效合约不足两个"}
    rows.sort(key=lambda x: x["code"])
    codes = [q["code"] for q in rows]
    if main["code"] not in codes:
        return {"ok": False, "reason": "主力合约不在有效合约列表内"}
    i = codes.index(main["code"])
    far = None
    for q in rows[i + 1:]:
        if q["oi"] >= max(main["oi"] * 0.03, 500):
            far = q
            break
    if far is None:
        return {"ok": False, "reason": "主力之后无有效远月（合约尾部）"}
    spread = (far["last"] / main["last"] - 1) * 100
    if spread > 0.02:
        state = "远月升水（contango）"
        text = "远月 %s 比近月 %s 高 %.2f%%，属正常持有成本（仓储+资金）。" % (
            far["code"], main["code"], spread)
    elif spread < -0.02:
        state = "近月升水（backwardation）"
        text = "近月 %s 比远月 %s 贵 %.2f%%，现货紧张结构，黄金上罕见。" % (
            main["code"], far["code"], -spread)
    else:
        state = "平水"
        text = "近远月价差 %.2f%%，基本平水。" % spread
    return {
        "ok": True, "state": state, "state_text": text,
        "near_code": main["code"], "near_price": _r(main["last"], 2),
        "far_code": far["code"], "far_price": _r(far["last"], 2),
        "spread_pct": _r(spread, 2),
        "basis": "实时报价（盘中口径），仅展示",
    }


# ----------------------------------------------------------------------------
# 已收盘 / 进行中 拆分
# ----------------------------------------------------------------------------

def _mins(t):
    """'09:11:34' / '9:11:34' -> 551；无法解析 -> None"""
    m = re.match(r"^(\d{1,2}):(\d{2})", (t or "").strip())
    if not m:
        return None
    return int(m.group(1)) * 60 + int(m.group(2))


def _split_live(bars, q):
    """把「进行中」的那根从已收盘序列里摘出来。

    返回 (live_bar, settled_bars)。判定：
      * 行情时间在夜盘（21:00-02:30）或日盘 15:00 之前 -> 当日那根未收盘；
      * 行情日期 > 最后一根日K日期 -> 新交易日尚无日K，用快照造一根；
      * 日盘 15:00 之后且非夜盘 -> 当日已收盘，无 live。
    """
    if not bars or not q or not q.get("last"):
        return None, bars
    qd = q.get("date") or ""
    t = q.get("time") or ""
    night = bool(q.get("night_session"))
    mins = _mins(t)
    settled_now = (not night) and (mins is None or mins >= 15 * 60)
    last = bars[-1]

    if qd and last["d"] == qd and not settled_now:
        live = {"d": qd, "o": q.get("open"), "h": q.get("high"), "l": q.get("low"),
                "c": q["last"], "v": q.get("volume") or 0, "oi": q.get("oi") or 0,
                "live": 1}
        return live, bars[:-1]

    if qd and qd > last["d"]:
        live = {"d": qd, "o": q.get("open"), "h": q.get("high"), "l": q.get("low"),
                "c": q["last"], "v": q.get("volume") or 0, "oi": q.get("oi") or 0,
                "live": 1}
        return live, bars

    return None, bars


# ----------------------------------------------------------------------------
# 十二维体检
# ----------------------------------------------------------------------------

W = {   # 权重合计 1.00（黄金版口径，与个股十二维体检不同，不可横向比较）
    "trend": 0.12, "oi": 0.12, "term": 0.07, "momentum": 0.10,
    "volume": 0.08, "position": 0.10, "vola": 0.06, "gs": 0.07,
    "premium": 0.08, "dxy": 0.07, "etf": 0.06, "liq": 0.07,
}


def _dim(name, weight, score, level, evidence=None, advice=""):
    return {
        "name": name, "weight": weight,
        "score": None if score is None else int(round(_clamp(score))),
        "level": level, "evidence": evidence or [], "advice": advice,
    }


def _miss(name, weight, why):
    return _dim(name, weight, None, "数据缺失", [why], "")


def _dim_trend(bars):
    closes = [b["c"] for b in bars]
    if len(closes) < 65:
        return _miss("趋势结构", W["trend"], "日K不足 60 根")
    c = closes[-1]
    ma5, ma10, ma20, ma60 = (_ma(closes, n) for n in (5, 10, 20, 60))
    slope20 = (ma20 / _ma(closes[:-5], 20) - 1) * 100 if _ma(closes[:-5], 20) else 0
    sc = 50.0
    if c > ma20:
        sc += 12
    else:
        sc -= 12
    if ma20 > ma60:
        sc += 12
    else:
        sc -= 12
    if ma5 > ma10 > ma20:
        sc += 10
    if ma10 < ma20 < ma60:
        sc -= 15
    if slope20 > 0.5:
        sc += 8
    elif slope20 < -0.5:
        sc -= 8
    if ma5 > ma10 > ma20 > ma60:
        level = "多头排列"
    elif c > ma20 and ma20 > ma60:
        level = "上升趋势"
    elif c < ma20 and ma20 < ma60:
        level = "空头趋势"
    else:
        level = "震荡"
    ev = ["收盘 %.2f" % c, "MA20 %.2f" % ma20, "MA60 %.2f" % ma60,
          "MA20 五日斜率 %+.2f%%" % slope20]
    advice = {
        "多头排列": "均线全多，回踩 MA10/MA20 是右侧加仓位；跌破 MA20 减仓。",
        "上升趋势": "中期趋势向上，等回踩 MA20 不破再介入，不在偏离过大时追。",
        "震荡": "均线纠缠无方向，等站稳 MA20 且 MA20 上翘再谈右侧。",
        "空头趋势": "中期空头，右侧信号未成立，反弹到 MA20/MA60 是减压位。",
    }[level]
    return _dim("趋势结构", W["trend"], sc, level, ev, advice)


def _oi_pattern(pchg20, oi_chg20):
    if pchg20 > 0.5 and oi_chg20 > 0:
        return "价涨仓增", "新资金进场做多，趋势有增量资金背书（墨菲四形态中最健康）", 85
    if pchg20 > 0.5 and oi_chg20 <= 0:
        return "价涨仓减", "空头回补推动，涨势缺乏新增多头，持续性弱", 60
    if pchg20 < -0.5 and oi_chg20 > 0:
        return "价跌仓增", "新资金进场做空，下跌有增量资金推动，最需警惕", 25
    if pchg20 < -0.5 and oi_chg20 <= 0:
        return "价跌仓减", "多头清算离场，抛压来自平仓而非新空，跌势可能衰竭", 40
    return "价仓双平", "价格与持仓都无明确方向，资金观望", 55


def _dim_oi(bars):
    if len(bars) < 25:
        return _miss("持仓结构", W["oi"], "日K不足 25 根")
    c = bars[-1]["c"]
    pc = bars[-21]["c"]
    pchg20 = (c / pc - 1) * 100 if pc else 0
    oi_now = bars[-1]["oi"]
    oi5 = bars[-6]["oi"] or 0
    oi20 = bars[-21]["oi"] or 0
    if not oi_now or not oi20:
        return _miss("持仓结构", W["oi"], "持仓量序列缺失")
    oi_chg5 = (oi_now / oi5 - 1) * 100 if oi5 else 0
    oi_chg20 = (oi_now / oi20 - 1) * 100
    pattern, desc, sc = _oi_pattern(pchg20, oi_chg20)
    if pattern == "价涨仓增" and oi_chg5 > 2:
        sc += 7
    if pattern == "价跌仓增" and oi_chg5 > 2:
        sc -= 5
    vol5 = _mean([b["v"] for b in bars[-5:]])
    vol20 = _mean([b["v"] for b in bars[-20:]])
    vr = vol5 / vol20 if vol20 else None
    ev = ["持仓 %s 手" % format(int(oi_now), ","),
          "5 日 %+.1f%% / 20 日 %+.1f%%" % (oi_chg5, oi_chg20),
          "价 20 日 %+.2f%%" % pchg20,
          "量比(5/20) %s" % ("%.2f" % vr if vr else "--")]
    advice = {
        "价涨仓增": "增仓上行可跟随，持仓开始回落时警惕趋势末端。",
        "价涨仓减": "减仓上行不追高，等持仓重新回升确认。",
        "价跌仓增": "增仓下跌不接，等持仓回落或价格站回 MA20。",
        "价跌仓减": "杀跌动能衰减，但右侧需等价格先企稳。",
        "价仓双平": "资金观望，等价格与持仓同向放大再说。",
    }[pattern]
    return _dim("持仓结构", W["oi"], sc, pattern, ev, advice)


def _dim_term(term):
    if not term or not term.get("ok"):
        return _miss("期限结构", W["term"], (term or {}).get("reason") or "合约行情缺失")
    sp = term["spread_pct"]
    if sp is None:
        return _miss("期限结构", W["term"], "价差缺数")
    if sp < -0.02:
        sc, level = 70, "近月升水"
        advice = "现货紧张结构，历史上易出现逼仓，注意近月挤仓风险。"
    elif sp <= 2.5:
        sc, level = 68 - abs(sp) * 3, "远月温和升水"
        advice = "正常持有成本区间，对方向判断中性。"
    elif sp <= 5:
        sc, level = 52, "远月明显升水"
        advice = "远月升水偏大，说明现货不紧，做多需依赖宏观而非供需。"
    else:
        sc, level = 36, "远月大幅升水"
        advice = "升水过大往往是现货疲弱信号，谨慎追多。"
    ev = ["近月 %s %.2f" % (term["near_code"], term["near_price"]),
          "远月 %s %.2f" % (term["far_code"], term["far_price"]),
          "价差 %+.2f%%" % sp]
    return _dim("期限结构", W["term"], sc, level, ev, advice)


def _dim_momentum(bars):
    closes = [b["c"] for b in bars]
    if len(closes) < 25:
        return _miss("动量强度", W["momentum"], "日K不足 25 根")
    c = closes[-1]
    chg20 = (c / closes[-21] - 1) * 100
    chg5 = (c / closes[-6] - 1) * 100
    rsi = _rsi(closes, 14)
    sc = 50 + max(-30, min(30, chg20 * 2.2))
    if rsi is not None:
        if rsi > 78:
            sc -= 14
        elif rsi >= 55:
            sc += 8
        elif rsi < 30:
            sc += 6
        elif rsi < 40:
            sc -= 6
    if chg5 > 0 and chg20 > 0:
        sc += 4
    ev = ["20 日 %+.2f%%" % chg20, "5 日 %+.2f%%" % chg5,
          "RSI14 %s" % ("%.1f" % rsi if rsi is not None else "--")]
    if rsi is not None and rsi > 78:
        level, advice = "超买", "动量强但 RSI 超买，右侧追高赔率差，等回踩再看。"
    elif chg20 > 6:
        level, advice = "强势", "动量向上，顺势但用移动止损守住利润。"
    elif chg20 > 0:
        level, advice = "温和向上", "动量温和，需量能配合才能确认。"
    elif chg20 > -6:
        level, advice = "走弱", "动量为负，右侧不成立。"
    else:
        level, advice = "弱势", "动量明显走坏，反弹以减仓为主。"
    return _dim("动量强度", W["momentum"], sc, level, ev, advice)


def _dim_volume(bars):
    if len(bars) < 25:
        return _miss("量能配合", W["volume"], "日K不足 25 根")
    v5 = _mean([b["v"] for b in bars[-5:]])
    v20 = _mean([b["v"] for b in bars[-20:]])
    if not v20:
        return _miss("量能配合", W["volume"], "成交量缺失")
    vr = v5 / v20
    today = bars[-1]
    vtoday = today["v"] / v5 if v5 else None
    up = today["c"] >= today["o"]
    sc = 50 + max(-25, min(25, (vr - 1) * 45))
    if vr > 1.2 and up:
        sc += 10
    if vr > 1.3 and not up:
        sc -= 12
    if vr < 0.7:
        sc -= 6
    level = "放量" if vr >= 1.25 else ("缩量" if vr <= 0.8 else "量能平稳")
    ev = ["5/20 日均量 %.2f" % vr,
          "今日量 / 5 日均 %s" % ("%.2f 倍" % vtoday if vtoday else "--")]
    advice = {
        "放量": "放量方向即短期方向，放量滞涨要减。",
        "缩量": "缩量说明分歧小，突破需要量能确认。",
        "量能平稳": "量能无异常，结论以趋势与持仓为主。",
    }[level]
    return _dim("量能配合", W["volume"], sc, level, ev, advice)


def _dim_position(bars):
    if len(bars) < 60:
        return _miss("位置与回撤", W["position"], "日K不足 60 根")
    seg = bars[-250:] if len(bars) >= 250 else bars
    hi = max(b["h"] for b in seg)
    lo = min(b["l"] for b in seg)
    c = bars[-1]["c"]
    pos = (c - lo) / (hi - lo) * 100 if hi > lo else 50
    dd = (c / hi - 1) * 100
    if pos >= 95:
        sc, level = 55, "接近区间顶"
        advice = "位置极高，右侧只剩追高，等回踩 MA10 或突破确认后再加。"
    elif pos >= 78:
        sc, level = 72, "高位强势"
        advice = "高位强势但未过热，趋势票可持有，止损上移。"
    elif pos >= 45:
        sc, level = 78, "中位偏强"
        advice = "区间中上位置，向上空间与风险均衡，是右侧介入的舒适区。"
    elif pos >= 22:
        sc, level = 55, "中位偏弱"
        advice = "区间中下位置，需等结构转强再介入。"
    else:
        sc, level = 32, "接近区间底"
        advice = "区间底部，左侧接刀风险大，等右侧信号。"
    if dd < -15:
        sc -= 12
    ev = ["250 日分位 %.0f%%" % pos, "距 250 日高点 %+.1f%%" % dd,
          "区间 %.2f ~ %.2f" % (lo, hi)]
    return _dim("位置与回撤", W["position"], sc, level, ev, advice)


def _dim_vola(bars):
    if len(bars) < 65:
        return _miss("波动率状态", W["vola"], "日K不足 65 根")
    trs = []
    for i in range(-20, 0):
        b, p = bars[i], bars[i - 1]
        trs.append(max(b["h"] - b["l"], abs(b["h"] - p["c"]), abs(b["l"] - p["c"])))
    atr20 = _mean(trs)
    atr_pct = atr20 / bars[-1]["c"] * 100
    sd20 = _sd(_returns([b["c"] for b in bars[-21:]])) * 100
    sd120 = _sd(_returns([b["c"] for b in bars[-121:]])) * 100
    ratio = sd20 / sd120 if sd120 else 1.0
    if ratio > 2:
        sc, level, advice = 38, "极端放大", "波动是常态的两倍以上，止损易被扫，降杠杆等收敛。"
    elif ratio > 1.4:
        sc, level, advice = 52, "波动扩张", "波动扩张期，仓位减半、止损放宽。"
    elif ratio >= 0.8:
        sc, level, advice = 72, "波动正常", "波动处于常态，技术位可靠性较高。"
    elif ratio >= 0.55:
        sc, level, advice = 62, "波动收敛", "波动收敛，往往在酝酿方向选择。"
    else:
        sc, level, advice = 50, "异常平静", "波动极低，突破后容易一步到位，注意假突破。"
    ev = ["ATR20 %.2f%%" % atr_pct, "20/120 日波动比 %.2f" % ratio]
    return _dim("波动率状态", W["vola"], sc, level, ev, advice)


def _dim_gs(ratio_now, ratio_hist):
    """金银比：黄金/白银的比价，衡量贵金属内部风险偏好"""
    if not ratio_now:
        return _miss("金银比", W["gs"], "伦敦金/银报价缺失")
    pct = _pct_rank(ratio_hist, ratio_now) if len(ratio_hist) > 60 else None
    if pct is None:
        sc, level = 55, "金银比 %.1f" % ratio_now
        advice = "缺少历史序列，仅作参考。"
    elif pct >= 90:
        sc, level = 42, "极值高位"
        advice = "金银比极高，白银极度弱势，通常对应避险/衰退交易；黄金相对强但比价易回落。"
    elif pct >= 70:
        sc, level = 56, "偏高"
        advice = "金银比偏高，黄金强于白银，做多黄金胜于做多白银。"
    elif pct >= 25:
        sc, level = 70, "中性区间"
        advice = "金银比中性，贵金属内部无极端错配。"
    else:
        sc, level = 48, "极值低位"
        advice = "金银比极低，白银过热，贵金属整体情绪偏投机。"
    ev = ["金银比 %.1f" % ratio_now]
    if pct is not None:
        ev.append("250 日分位 %.0f%%" % pct)
    return _dim("金银比", W["gs"], sc, level, ev, advice)


def _dim_premium(au_px, gc_usd, cny):
    if not au_px or not gc_usd or not cny:
        return _miss("内外盘价差", W["premium"], "COMEX 或汇率缺数")
    theo = gc_usd / OZ_TO_GRAM * cny
    prem = (au_px / theo - 1) * 100
    sc = 70 - abs(prem) * 12
    if prem > 3:
        level, advice = "国内大幅溢价", "国内价格含非基本面溢价，追多等于多付钱，等溢价收敛。"
    elif prem > 1:
        level, advice = "国内溢价", "进口亏损，国内偏贵，做多优先选外盘或等回归。"
    elif prem > -1:
        level, advice = "基本平水", "内外价差正常，价格主要由国际金与汇率驱动。"
    elif prem > -3:
        level, advice = "国内折价", "国内偏便宜，做多国内略占优。"
    else:
        level, advice = "国内大幅折价", "价差异常，先排查汇率或合约口径再下单。"
    ev = ["沪金 %.2f 元/克" % au_px, "COMEX 折算 %.2f 元/克" % theo,
          "溢价 %+.2f%%" % prem]
    return _dim("内外盘价差", W["premium"], sc, level, ev, advice)


def _dim_dxy(dxy_rows, cny):
    if not dxy_rows or len(dxy_rows) < 21:
        return _miss("美元与汇率", W["dxy"], "美元指数序列不足")
    last = dxy_rows[-1]["c"]
    chg20 = (last / dxy_rows[-21]["c"] - 1) * 100
    sc = 50 - max(-25, min(25, chg20 * 6))
    ev = ["美元指数 %.2f" % last, "20 日 %+.2f%%" % chg20]
    if cny and cny.get("chg_pct") is not None:
        ev.append("USDCNY %.4f（%+.2f%%）" % (cny["last"], cny["chg_pct"]))
    if chg20 < -2:
        level, advice = "美元走弱", "美元趋势性走弱，对金价构成中期支撑。"
    elif chg20 < -0.5:
        level, advice = "美元偏弱", "美元小幅回落，边际利好黄金。"
    elif chg20 > 2:
        level, advice = "美元走强", "美元趋势性走强，是金价最主要的中期逆风。"
    elif chg20 > 0.5:
        level, advice = "美元偏强", "美元企稳偏强，压制金价，做多需更强驱动。"
    else:
        level, advice = "美元横盘", "美元指数横盘，金价方向由其他变量决定。"
    return _dim("美元与汇率", W["dxy"], sc, level, ev, advice)


def _dim_etf(etf_q, etf_rows):
    fallback = False
    if not etf_q or not etf_q.get("last"):
        # 盘前/盘后新浪快照会清空最新价，用最近一根日K兜底（engine 行：[date,o,c,h,l,v]）
        if etf_rows and len(etf_rows) >= 2:
            etf_q = {"last": etf_rows[-1][2], "prev_close": etf_rows[-2][2],
                     "amount": etf_rows[-1][2] * etf_rows[-1][5],
                     "date": etf_rows[-1][0]}
            fallback = True
        else:
            return _miss("黄金ETF资金", W["etf"], "518880 快照与日K均缺失")
    amt = etf_q.get("amount") or 0
    avg20 = None
    if etf_rows and len(etf_rows) >= 21:
        # engine 行格式：[date, open, close, high, low, volume]
        vals = [r[2] * r[5] for r in etf_rows[-21:-1]]
        avg20 = _mean(vals) if vals else None
    ratio = amt / avg20 if avg20 else None
    prev = etf_q.get("prev_close")
    chg = (etf_q["last"] / prev - 1) * 100 if prev else None
    sc = 55
    if ratio:
        sc += max(-15, min(20, (ratio - 1) * 22))
    if chg is not None and chg > 0:
        sc += 6
    level = "资金关注高" if (ratio or 0) >= 1.3 else ("资金平淡" if (ratio or 1) >= 0.7 else "资金清淡")
    ev = ["518880 %.3f" % etf_q["last"]]
    if chg is not None:
        ev.append("日涨跌 %+.2f%%" % chg)
    ev.append("成交额 %.1f 亿" % (amt / 1e8))
    if avg20:
        ev.append("20 日均 %.1f 亿（%.2f 倍）" % (avg20 / 1e8, ratio))
    if fallback:
        ev.append("盘前快照为空，用 %s 日K兜底" % etf_q["date"])
    advice = "ETF 是场外资金最直接的通道，放量溢价说明散户与配置盘在进场。" \
        if level == "资金关注高" else "ETF 资金无异常，结论以期货侧为主。"
    return _dim("黄金ETF资金", W["etf"], sc, level, ev, advice)


def _dim_liq(bars):
    if len(bars) < 21:
        return _miss("流动性", W["liq"], "日K不足 21 根")
    seg = bars[-20:]
    amt = _mean([b["v"] * AU_LOT_GRAM * b["c"] for b in seg])
    oi = bars[-1]["oi"]
    sc = 40 + min(50, math.log10(max(amt, 1)) * 8)
    level = "充裕" if amt > 5e9 else ("适中" if amt > 1e9 else "偏薄")
    ev = ["20 日均额 %.0f 亿" % (amt / 1e8), "持仓 %.1f 万手" % (oi / 1e4)]
    advice = "流动性充裕，冲击成本低，可用市价单分笔。" if level == "充裕" \
        else "流动性一般，大单请拆分，避免滑点。"
    return _dim("流动性", W["liq"], sc, level, ev, advice)


# ----------------------------------------------------------------------------
# 标的池（沪金 vs A 股黄金标的）
# ----------------------------------------------------------------------------

GOLD_POOL = [
    # code, 名称, tier, 业务属性
    ("600547", "山东黄金", "core", "国内最大黄金生产商之一，矿产金为主，金价弹性最直接的标的"),
    ("600489", "中金黄金", "core", "中国黄金集团旗下，矿山+冶炼一体化"),
    ("600988", "赤峰黄金", "core", "海外金矿占比高，产量成长性强，弹性大"),
    ("002155", "湖南黄金", "core", "黄金+锑钨，金价与锑价双击品种"),
    ("601069", "西部黄金", "core", "新疆区域金矿，纯金业务占比高"),
    ("001337", "四川黄金", "core", "单一金矿资产，纯度最高的金价映射"),
    ("000975", "银泰黄金", "core", "合质金与铅锌伴生，成本控制较好"),
    ("601899", "紫金矿业", "satellite", "铜金双主业，金价只是其中一条腿，β 被铜稀释"),
    ("002237", "恒邦股份", "satellite", "黄金冶炼为主，加工费模式，金价弹性弱于矿山"),
    ("002716", "金贵银业", "satellite", "白银冶炼，随贵金属板块联动但主业非黄金"),
    ("000506", "中润资源", "satellite", "斐济金矿资产，A 股里少数海外金矿标的，市值小波动大"),
    ("601212", "白银有色", "satellite", "白银+有色，贵金属β但黄金权重低"),
    ("300139", "晓程科技", "satellite", "加纳金矿资产，市值小、波动大"),
    ("600916", "中国黄金", "related", "黄金珠宝零售，赚加工与品牌溢价，金价上涨反而压毛利"),
    ("600612", "老凤祥", "related", "黄金饰品龙头，消费属性强于资源属性"),
    ("002867", "周大生", "related", "珠宝零售，与金价关联来自存货与终端需求"),
]

_TIER_META = {
    "core": {"name": "核心层", "desc": "黄金采掘主业，金价即业绩，做放大器首选"},
    "satellite": {"name": "卫星层", "desc": "伴生/冶炼/白银，有关联但被其他金属或加工费稀释"},
    "related": {"name": "关联层", "desc": "珠宝零售与饰品，消费属性压过资源属性"},
}


def _idx_rows():
    """上证指数日K（部分相关要剥离大盘），engine 行格式 [date,o,c,h,l,v]"""
    def load():
        rows = engine._kline_fetch_raw("sh000001", 300)
        if not rows:
            rows = engine.kline_get("000001")
        return rows or []
    return _cached("gold:idx", 600, load)


def _series_map(rows):
    """[{d,c}] 或 engine 行 -> {date: close}"""
    m = {}
    for r in rows or []:
        if isinstance(r, dict):
            if r.get("d") and r.get("c"):
                m[r["d"]] = float(r["c"])
        elif isinstance(r, (list, tuple)) and len(r) >= 3:
            m[r[0]] = float(r[2])
    return m


def _build_pool(gold_bars, notes):
    """16 只黄金标的的实测相关/β/滚动稳定性。

    口径：日收益率、近 120 个共同交易日；偏相关 = 剥离上证指数后的相关（这才是「真黄金关联」）。
    """
    gs = _series_map(gold_bars)
    idx = _series_map(_idx_rows())
    gdates = sorted(gs.keys())

    pool, sig = [], []
    missing = []

    for code, name, tier, biz in GOLD_POOL:
        rows = engine.kline_get(code)
        if not rows or len(rows) < 130:
            # 本地缓存缺失（新股/长期停牌/未进扫描池）时现补一次，避免标的池条数飘
            rows = _cached("gold:kl:%s" % code, 3600,
                           lambda c=code: engine._kline_fetch_raw(engine.symbol_of_code(c), 300))
        if not rows or len(rows) < 130:
            missing.append("%s %s 本地日K不足" % (code, name))
            continue
        s = _series_map(rows)
        dates_all = [d for d in gdates if d in s and d in idx]
        if len(dates_all) < 90:
            missing.append("%s %s 与沪金共同交易日不足" % (code, name))
            continue
        dates = dates_all[-121:]
        g = [gs[d] for d in dates]
        k = [s[d] for d in dates]
        m = [idx[d] for d in dates]
        rg, rk, rm = _returns(g), _returns(k), _returns(m)
        n = min(len(rg), len(rk), len(rm))
        rg, rk, rm = rg[-n:], rk[-n:], rm[-n:]
        raw = _corr(rk, rg)
        rkm = _corr(rk, rm)
        rgm = _corr(rg, rm)
        pcorr = None
        if raw is not None and rkm is not None and rgm is not None:
            den = (1 - rkm ** 2) * (1 - rgm ** 2)
            if den > 1e-9:
                pcorr = (raw - rkm * rgm) / math.sqrt(den)

        var = _sd(rg) ** 2
        beta = None
        if var > 1e-12 and raw is not None:
            beta = raw * _sd(rk) / _sd(rg)
        # 涨段/跌段 β：黄金上涨日与下跌日分别回归斜率
        ups = [(rk[i], rg[i]) for i in range(n) if rg[i] > 0]
        dns = [(rk[i], rg[i]) for i in range(n) if rg[i] < 0]

        def _seg_beta(pairs):
            if len(pairs) < 8:
                return None
            xs = [p[1] for p in pairs]
            ys = [p[0] for p in pairs]
            v = _sd(xs) ** 2
            if v <= 1e-12:
                return None
            mx, my = _mean(xs), _mean(ys)
            return sum((xs[i] - mx) * (ys[i] - my) for i in range(len(xs))) / (v * (len(xs) - 1))

        beta_up = _seg_beta(ups)
        beta_down = _seg_beta(dns)

        # 滚动 60 日相关的标准差（越小越稳定）
        # ★ 必须用比 120 日更长的窗口来滚：原来拿被截到 121 根的 dates 去滚 60 日，
        #   len(dates)-60 只剩 61 个起点、每 5 日取一个 → 样本不足，roll_sigma 恒为 None。
        roll = []
        dl = dates_all[-260:]
        if len(dl) >= 80:
            for i in range(0, len(dl) - 60, 5):
                w_g = [gs[d] for d in dl[i:i + 61]]
                w_k = [s[d] for d in dl[i:i + 61]]
                c = _corr(_returns(w_k), _returns(w_g))
                if c is not None:
                    roll.append(c)
        roll_sigma = _sd(roll) if len(roll) >= 3 else None

        closes = [r[2] for r in rows]
        last = closes[-1]
        chg20 = (last / closes[-21] - 1) * 100 if len(closes) > 21 else None
        chg60 = (last / closes[-61] - 1) * 100 if len(closes) > 61 else None

        # 领先/滞后：lag>0 表示股票滞后黄金
        best_lag, best_c = None, None
        for lag in range(-3, 4):
            if lag >= 0:
                a = _returns(k)[:len(rk) - lag] if lag else rk
                b = rg[lag:] if lag else rg
            else:
                a = rk[-lag:]
                b = rg[:len(rg) + lag]
            c = _corr(a, b)
            if c is not None and (best_c is None or abs(c) > abs(best_c)):
                best_c, best_lag = c, lag

        strength = "无关"
        if pcorr is not None:
            if pcorr >= 0.6:
                strength = "强跟随"
            elif pcorr >= 0.4:
                strength = "中跟随"
            elif pcorr >= 0.2:
                strength = "弱跟随"
        bits = [strength]
        if pcorr is not None:
            bits.append("偏相关 %.2f" % pcorr)
        if beta is not None:
            bits.append("β %.2f" % beta)
        if beta_up is not None and beta_down is not None:
            if beta_up > beta_down * 1.3:
                bits.append("涨时弹性大于跌时")
            elif beta_down > beta_up * 1.3:
                bits.append("跌时跟得更紧（放大亏损）")
        if best_lag is not None:
            bits.append("最佳滞后 %+d 日" % best_lag)
        note = "；".join(bits)

        okk = tier == "core" and (pcorr or 0) >= MIN_PCORR
        it = {
            "name": name, "code": code, "tier": tier, "biz": biz,
            "pcorr": _r(pcorr, 2), "raw": _r(raw, 2), "beta": _r(beta, 2),
            "beta_up": _r(beta_up, 2), "beta_down": _r(beta_down, 2),
            "roll_sigma": _r(roll_sigma, 3), "lag": best_lag,
            "last": _r(last, 2), "chg_20": _r(chg20, 2), "chg_60": _r(chg60, 2),
            "note": note, "tradable": okk,
        }
        pool.append(it)
        if okk:
            sig.append(it)

    order = {"core": 0, "satellite": 1, "related": 2}
    pool.sort(key=lambda x: (order.get(x["tier"], 9), -(x["pcorr"] or -9)))
    summ = {}
    for k in ("core", "satellite", "related"):
        rows = [p for p in pool if p["tier"] == k]
        if rows:
            summ[k] = {"name": _TIER_META[k]["name"], "desc": _TIER_META[k]["desc"],
                       "count": len(rows)}
    if missing:
        notes.append("标的池跳过 %d 只：%s" % (len(missing), "、".join(missing[:4])))
    return pool, sig, summ


# ----------------------------------------------------------------------------
# 组装 /api/gold
# ----------------------------------------------------------------------------

def _build_gold():
    notes = []
    updated = time.strftime("%Y-%m-%d %H:%M:%S")

    main_q, main_info, contracts = _resolve_main(notes)
    bars_all = _fut_daily("AU0")
    if not bars_all:
        return {"ok": False, "error": "沪金日K取数失败", "empty": True,
                "source": "新浪财经期货", "updated": updated}
    notes.append("沪金连续 AU0 取到 %d 根日K，最新 %s 收盘 %.2f" % (
        len(bars_all), bars_all[-1]["d"], bars_all[-1]["c"]))

    q = main_q
    if q is None:
        try:
            q = _nf_batch(["AU0"]).get("AU0")
            notes.append("主力合约行情缺失，退回 AU0 连续报价")
        except Exception:
            q = None

    live, settled = _split_live(bars_all, q)
    if live:
        notes.append("检测到未收盘K线（%s %s），已从已收盘序列剔除，仅用于图表展示与实时行情条" % (
            live["d"], q.get("time") if q else ""))
    if not settled:
        return {"ok": False, "error": "已收盘日K为空", "empty": True,
                "source": "新浪财经期货", "updated": updated}

    # ---------- 实时行情条 ----------
    if q and q.get("last"):
        base = q.get("prev_settle")
        chg = (q["last"] - base) if base else None
        quote = {
            "code": q["code"], "last": _r(q["last"], 2),
            "chg": _r(chg, 2),
            "chg_pct": _r(chg / base * 100, 2) if (chg is not None and base) else None,
            "open": _r(q["open"], 2), "high": _r(q["high"], 2), "low": _r(q["low"], 2),
            "prev_settle": _r(base, 2), "chg_base": "昨结算",
            "bid": _r(q["bid"], 2), "ask": _r(q["ask"], 2),
            "volume": q["volume"], "oi": q["oi"],
            "time": q["time"], "trade_day": q["date"],
            "night_session": bool(q.get("night_session")),
            "settled_bar_date": settled[-1]["d"], "settled_close": _r(settled[-1]["c"], 2),
        }
    else:
        quote = {
            "code": (main_info or {}).get("code") or "AU0", "last": None,
            "time": (q or {}).get("time"), "trade_day": (q or {}).get("date"),
            "settled_bar_date": settled[-1]["d"], "settled_close": _r(settled[-1]["c"], 2),
            "chg_base": "昨结算",
        }
        notes.append("实时快照无最新价（可能处竞价时段外），行情条降级为已收盘口径")

    # ---------- 期限结构 / 持仓结构 ----------
    term = _term_structure(contracts, main_q, notes)
    close_now = settled[-1]["c"]

    oi_struct = {"ok": False}
    if len(settled) >= 25:
        oi_now = settled[-1]["oi"]
        oi5 = settled[-6]["oi"] or 0
        oi20 = settled[-21]["oi"] or 0
        pchg20 = (close_now / settled[-21]["c"] - 1) * 100
        if oi_now and oi20:
            oi_chg5 = (oi_now / oi5 - 1) * 100 if oi5 else 0
            oi_chg20 = (oi_now / oi20 - 1) * 100
            pattern, desc, _sc = _oi_pattern(pchg20, oi_chg20)
            vol5 = _mean([b["v"] for b in settled[-5:]])
            vol20 = _mean([b["v"] for b in settled[-20:]])
            oi_struct = {
                "ok": True, "pattern": pattern, "pattern_desc": desc,
                "oi_last": int(oi_now), "oi_chg_5": _r(oi_chg5, 1),
                "oi_chg_20": _r(oi_chg20, 1), "price_chg_20": _r(pchg20, 2),
                "vol_ratio": _r(vol5 / vol20, 2) if vol20 else None,
                "veto": pattern == "价跌仓增" and oi_chg5 > 2,
            }

    # ---------- 外部行情 ----------
    gc = xau = xag = None
    try:
        hf = _get("https://hq.sinajs.cn/list=hf_GC,hf_XAU,hf_XAG", encoding="gbk")
        for line in hf.split(";"):
            if "hq_str_hf_GC=" in line:
                gc = _hf_parse(line)
            elif "hq_str_hf_XAU=" in line:
                xau = _hf_parse(line)
            elif "hq_str_hf_XAG=" in line:
                xag = _hf_parse(line)
    except Exception:
        notes.append("外盘（COMEX/伦敦金银）取数失败")
    cny = _fx_cny()
    dxy = _dxy_daily()
    try:
        etf_q = _stock_quote("sh518880")
    except Exception:
        etf_q = None
    etf_rows = None
    try:
        etf_rows = engine._kline_fetch_raw("sh518880", 120)
    except Exception:
        etf_rows = None

    # 金银比序列（伦敦金/伦敦银，同日期对齐）
    gs_ratio_now, gs_ratio_hist = None, []
    try:
        ax = _series_map(_global_daily("XAU"))
        ag = _series_map(_global_daily("XAG"))
        for d in sorted(set(ax) & set(ag)):
            if ag[d]:
                gs_ratio_hist.append(ax[d] / ag[d])
        if xau and xag and xag.get("last"):
            gs_ratio_now = xau["last"] / xag["last"]
        elif gs_ratio_hist:
            gs_ratio_now = gs_ratio_hist[-1]
        gs_ratio_hist = gs_ratio_hist[-250:]
    except Exception:
        notes.append("金银比序列取数失败")

    # ---------- 十二维 ----------
    dims = [
        _dim_trend(settled),
        _dim_oi(settled),
        _dim_term(term),
        _dim_momentum(settled),
        _dim_volume(settled),
        _dim_position(settled),
        _dim_vola(settled),
        _dim_gs(gs_ratio_now, gs_ratio_hist),
        _dim_premium(close_now, (gc or {}).get("last"), (cny or {}).get("last")),
        _dim_dxy(dxy, cny),
        _dim_etf(etf_q, etf_rows),
        _dim_liq(settled),
    ]

    avail = [x for x in dims if x["score"] is not None]
    wsum = sum(x["weight"] for x in avail)
    checkup_score = _r(sum(x["score"] * x["weight"] for x in avail) / wsum, 1) if wsum else None

    # 策略适配分：右侧交易更看趋势+持仓+动量+位置
    SW = {"趋势结构": 0.22, "持仓结构": 0.22, "动量强度": 0.18,
          "位置与回撤": 0.14, "量能配合": 0.12, "期限结构": 0.06, "波动率状态": 0.06}
    sw = sum(SW.get(x["name"], 0) for x in avail)
    strategy_score = None
    if sw:
        strategy_score = _r(sum(x["score"] * SW.get(x["name"], 0) for x in avail) / sw, 1)

    # ---------- 硬性否决 ----------
    vetoes = []
    if oi_struct.get("veto"):
        vetoes.append("价跌仓增：持仓 5 日 %+.1f%% 而价格 20 日 %+.2f%%，新资金在做空" % (
            oi_struct["oi_chg_5"], oi_struct["price_chg_20"]))
    closes = [b["c"] for b in settled]
    ma60 = _ma(closes, 60)
    seg = settled[-250:] if len(settled) >= 250 else settled
    hi = max(b["h"] for b in seg)
    dd = (close_now / hi - 1) * 100
    if ma60 and close_now < ma60 and dd < -12:
        vetoes.append("收盘 %.2f 跌破 MA60 %.2f 且距 250 日高点 %+.1f%%，右侧结构已破坏" % (
            close_now, ma60, dd))
    prem_dim = [x for x in dims if x["name"] == "内外盘价差"]
    if prem_dim and prem_dim[0]["level"] in ("国内大幅溢价", "国内大幅折价"):
        vetoes.append("内外盘价差异常（%s），国内价格含非基本面成分，结论不可靠" % prem_dim[0]["level"])
    vol_dim = [x for x in dims if x["name"] == "波动率状态"]
    if vol_dim and vol_dim[0]["level"] == "极端放大":
        vetoes.append("波动率极端放大（%s），止损极易被扫" % (vol_dim[0]["evidence"] or ["--"])[0])
    mom_dim = [x for x in dims if x["name"] == "动量强度"]
    if mom_dim and mom_dim[0]["level"] == "超买":
        vetoes.append("RSI14 超买（%s），追高赔率差" % (mom_dim[0]["evidence"] or ["--"])[-1])

    if vetoes and (checkup_score or 0) < 65:
        verdict = "回避"
    elif (checkup_score or 0) >= 70 and not vetoes:
        verdict = "可介入"
    elif (checkup_score or 0) < 50:
        verdict = "回避"
    else:
        verdict = "观察"

    trend_dim = dims[0]
    verdict_note = "%s 口径（%s 收盘 %.2f）：%s；持仓%s；%s。综合 %.1f 分，策略适配 %.1f 分，结论「%s」。" % (
        (main_info or {}).get("code") or "AU0", settled[-1]["d"], close_now,
        trend_dim["level"], oi_struct.get("pattern") or "无数据",
        (term.get("state") if term.get("ok") else "期限结构缺数据"),
        checkup_score or 0, strategy_score or 0, verdict)

    # ---------- 标的池 ----------
    pool, signal_pool, pool_summary = _build_pool(bars_all, notes)

    # 前端契约：mp.main 是主力对象，mp.ranking 在**顶层**（index.html:19344 mn = mp.main）
    main_pick = {}
    if main_info:
        main_pick = {
            "main": {k: main_info.get(k) for k in ("name", "code", "volume", "oi", "reason")},
            "ranking": main_info.get("ranking") or [],
        }

    kline = [{"d": b["d"], "o": _r(b["o"], 2), "h": _r(b["h"], 2), "l": _r(b["l"], 2),
              "c": _r(b["c"], 2), "v": int(b["v"]), "oi": int(b["oi"])}
             for b in bars_all[-120:]]

    out = {
        "ok": True, "empty": False,
        "updated": updated,
        "source": "新浪财经期货（沪金 nf_AU0／各月合约）· 新浪外盘（COMEX · 伦敦金银）· 东财（美元指数）",
        "checkup": {
            "trade_date": settled[-1]["d"],
            "code": (main_info or {}).get("code") or "AU0",
            "bars": len(settled),
            "term_structure": term,
            "dims": dims,
            "verdict": verdict,
            "verdict_note": verdict_note,
            "checkup_score": checkup_score,
            "strategy_score": strategy_score,
            "price": _r(close_now, 2),
            "liquidity": _r(_mean([b["v"] * AU_LOT_GRAM * b["c"] for b in settled[-20:]]), 0),
            "vetoes": vetoes,
        },
        "quote": quote,
        "live_bar": live,
        "main_pick": main_pick,
        "oi_structure": oi_struct,
        "term_note": "期限结构用主力月与下一个有量的远月实时报价计算",
        "kline": kline,
        "pool": pool,
        "signal_pool": signal_pool,
        "pool_summary": pool_summary,
        "min_pcorr": MIN_PCORR,
        "notes": notes,
        "disclaimer": "十二维权重表为本系统自建口径（无公开出处），结论为规则化推断，不构成投资建议。",
    }
    return out


def api_gold(params):
    params = params or {}
    force = str(params.get("refresh") or "").lower() in ("1", "true", "yes")
    return _cached("gold:main", 0 if force else 60, _build_gold)


# ----------------------------------------------------------------------------
# 体系溯源 /api/gold_kb
# ----------------------------------------------------------------------------

def api_gold_kb(params):
    """知识库：书目与官方数据源、待接入维度、硬编码的坑、明确未找到来源的项。

    原则：只写可核查的公开书目与官方机构页面，不编造页码与结论正文。
    """
    books = [
        {
            "title": "期货市场技术分析", "title_en": "Technical Analysis of the Futures Markets",
            "author": "John J. Murphy（约翰·墨菲）", "type": "经典教材",
            "core": [
                "价格与持仓量（Open Interest）必须配对解读：价涨仓增＝新资金做多，价跌仓增＝新资金做空，价涨仓减＝空头回补，价跌仓减＝多头清算。",
                "持仓量在趋势中途放大是趋势延续的确认，在趋势末端放大则可能是分歧加剧。",
                "趋势的定义来自高低点序列，均线只是趋势的平滑代理，不能替代结构判断。",
            ],
            "dims": ["趋势结构", "持仓结构", "动量强度"],
            "src": "https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm",
        },
        {
            "title": "CFTC 交易商持仓报告", "title_en": "Commitments of Traders (COT)",
            "author": "美国商品期货交易委员会（CFTC）", "type": "官方周报",
            "core": [
                "每周五公布截至周二的 COMEX 黄金期货分类持仓：商业头寸（生产商/贸易商）与非商业头寸（基金/投机）。",
                "非商业净多头是投机情绪的代理变量，极值区常对应价格拐点，但极值可持续数月，不能单独择时。",
                "商业净空头持续扩大，通常反映产业套保意愿增强，对上涨空间构成压力。",
            ],
            "dims": ["持仓结构", "动量强度"],
            "src": "https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm",
        },
        {
            "title": "黄金价格与需求数据", "title_en": "GoldHub · Gold Prices & Demand Trends",
            "author": "世界黄金协会（World Gold Council）", "type": "行业官方数据",
            "core": [
                "季度《Gold Demand Trends》拆分央行购金、金条金币、ETF、珠宝、工业五大需求。",
                "央行购金是近年金价中枢上移的核心变量，属季度频率，不能用于日内择时。",
                "全球黄金 ETF 持仓变化是场外资金最直接的流向指标（本系统用国内 518880 作代理）。",
            ],
            "dims": ["黄金ETF资金", "流动性"],
            "src": "https://www.gold.org/goldhub/data/gold-prices",
        },
        {
            "title": "LBMA 贵金属定盘价", "title_en": "LBMA Precious Metal Prices",
            "author": "伦敦金银市场协会（LBMA）", "type": "官方基准价",
            "core": [
                "LBMA Gold Price 于伦敦时间 10:30 与 15:00 两次拍卖定盘，是全球现货合约与 ETF 净值的主要基准。",
                "定盘价是拍卖形成的可成交价，与连续报价不同；跨市场套利必须以可成交价而非屏幕价计算。",
            ],
            "dims": ["内外盘价差"],
            "src": "https://www.lbma.org.uk/prices-and-data/lbma-precious-metal-prices",
        },
        {
            "title": "The Golden Dilemma", "title_en": "The Golden Dilemma (2013)",
            "author": "Claude B. Erb, Campbell R. Harvey（Financial Analysts Journal）",
            "type": "学术论文",
            "core": [
                "黄金没有现金流，估值只能相对其他资产（实际利率、CPI、人均收入）比较，不能给绝对「合理价」。",
                "金价与实际利率长期负相关，但相关关系在不同年代会失效，不能当恒等式使用。",
                "「黄金抗通胀」在长周期成立，在短周期（1-5 年）经常不成立。",
            ],
            "dims": ["美元与汇率", "位置与回撤"],
            "src": "https://www.nber.org/papers/w18706",
        },
        {
            "title": "The Golden Constant", "title_en": "The Golden Constant: The English and American Experience 1560-1976",
            "author": "Roy W. Jastram", "type": "经济史专著",
            "core": [
                "跨越四百年的购买力序列：黄金长期维持购买力，但中间可出现数十年的偏离。",
                "「长期」是世纪尺度，用该书结论做一两年交易会系统性误判。",
            ],
            "dims": ["位置与回撤"],
            "src": "https://www.gold.org/goldhub/research/gold-constant",
        },
        {
            "title": "上海期货交易所黄金期货合约", "title_en": "SHFE Gold Futures Contract Spec",
            "author": "上海期货交易所", "type": "交易所规则",
            "core": [
                "交易单位 1000 克/手，报价单位 元（人民币）/克，最小变动价位 0.02 元/克。",
                "交易日为日盘 09:00-11:30、13:30-15:00 与夜盘 21:00-次日 02:30；夜盘成交计入下一交易日。",
                "每日结算价按成交量加权计算，涨跌停与保证金以**上一交易日结算价**为基准 —— 这就是涨跌幅必须用昨结算而非昨收的原因。",
            ],
            "dims": ["流动性", "期限结构"],
            "src": "https://www.shfe.com.cn/products/",
        },
    ]

    new_dims = [
        {"prio": "P1", "name": "CFTC 非商业净多头", "state": "未接入",
         "def": "net_noncomm = 非商业多头 - 非商业空头，取周频并做 3 年分位",
         "note": "周报，滞后 3 天；对中期顶部/底部有参考，不能做日内。需解析 CFTC 的 txt/csv 直连。"},
        {"prio": "P1", "name": "全球黄金 ETF 持仓", "state": "部分接入",
         "def": "GLD/IAU 持仓吨数周变化 + 国内 518880 份额变化",
         "note": "当前只用 518880 的成交额与涨跌作代理，未取份额（需基金公告或 WGC 月报）。"},
        {"prio": "P2", "name": "上期所黄金库存/仓单", "state": "未接入",
         "def": "SHFE 每周库存仓单（千克）环比变化",
         "note": "仓单大幅增加常对应现货过剩，是期限结构转向的先行指标。需抓上期所周报页面。"},
        {"prio": "P2", "name": "进口盈亏（沪伦价差）", "state": "已接入（近似）",
         "def": "沪金 - COMEX折算价（美元/盎司 ÷ 31.1035 × USDCNY），阈值 ±2%",
         "note": "已用 COMEX 折算近似；严格口径应用 LBMA 下午定盘价 + 增值税与运费，差异约 0.3-0.8%。"},
        {"prio": "P3", "name": "美国 10 年期 TIPS 实际利率", "state": "未接入",
         "def": "实际利率 = 10Y 名义收益率 - 10Y 通胀预期（FRED: DFII10）",
         "note": "黄金定价的第一性变量，但需 FRED API key（免费），且国内访问不稳定。"},
        {"prio": "P3", "name": "央行购金（季度）", "state": "未接入",
         "def": "WGC 季度央行净购金（吨）",
         "note": "季度频率，只能用于年度中枢判断，不适合本页的右侧交易口径。"},
        {"prio": "P4", "name": "金矿 AISC 成本曲线", "state": "未接入",
         "def": "全维持成本（All-in Sustaining Cost）行业分位",
         "note": "来自公司财报，季度更新；用于判断金价长期底部，与日内交易无关。"},
    ]

    pitfalls = [
        "涨跌幅基准是**昨结算价**，不是昨收盘价 —— 用昨收会把 0.3% 的差异算成方向错误，盘口高低点染色也会反。",
        "沪金连续（AU0）是主力合约拼接序列，换月当天存在跳空；用它算的涨跌幅、持仓变化在换月日会失真，必须结合主力合约代码一起看。",
        "内外盘价差必须换算单位：COMEX 报价是美元/盎司，1 金衡盎司 = 31.1035 克（不是 28.35 常衡盎司）；漏掉汇率会把 6.6 倍的差异算成溢价。",
        "夜盘 21:00-02:30 的成交按上期所规则计入**下一交易日**，所以盘中看到的「今日涨跌」与日K最后一根的口径可能不是同一天。",
        "持仓量必须与价格同向配对才有意义：单看「持仓增加」无法判断是多头进场还是空头进场，必须配上同期价格方向。",
        "期货没有「主力资金净流入」这个官方指标 —— 用成交量×价格估算的是成交额，不是净流入；把成交额当资金流会得出相反结论。",
        "黄金股不含持仓量维度：股票侧无法复现期货的持仓结构，所以标的池只能回答「能不能做放大器」，不能回答「该不该买」。",
    ]

    not_found = [
        "沪金「十二维体检」的权重表：无公开出处，为本系统自建口径（趋势/持仓/期限/动量/量能/位置/波动/金银比/内外价差/美元/ETF/流动性），已在页面上明确标注不可与个股十二维横向比较。",
        "黄金「合理价」的权威公式：学术界（Erb & Harvey）明确否定存在稳定估值锚，本系统因此不给目标价。",
        "央行月度购金数据：WGC 仅发布季度口径，月度数据无官方来源，故不提供。",
        "沪金主力资金净流入：期货市场无此官方统计口径，不编造。",
    ]

    return {
        "ok": True, "empty": False,
        "updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "note": "本页只列可核查的公开书目、官方数据源与量化定义，不含书籍正文；"
                "凡无公开出处的口径（如本系统自建的十二维权重）均显式标注为自建。",
        "books": books,
        "new_dims": new_dims,
        "pitfalls": pitfalls,
        "not_found": not_found,
    }


# ----------------------------------------------------------------------------
# 自测：python gold.py
# ----------------------------------------------------------------------------

if __name__ == "__main__":
    t0 = time.time()
    d = api_gold({})
    print("gold  %d ms  ok=%s empty=%s" % ((time.time() - t0) * 1000, d.get("ok"), d.get("empty")))
    if d.get("ok"):
        c = d["checkup"]
        print("  trade_date=%s code=%s bars=%s verdict=%s score=%s strategy=%s" % (
            c["trade_date"], c["code"], c["bars"], c["verdict"],
            c["checkup_score"], c["strategy_score"]))
        q = d["quote"]
        print("  quote last=%s chg=%s(%s%%) base=%s settled=%s@%s" % (
            q.get("last"), q.get("chg"), q.get("chg_pct"), q.get("chg_base"),
            q.get("settled_close"), q.get("settled_bar_date")))
        print("  live_bar=%s  main=%s" % (
            bool(d["live_bar"]), ((d.get("main_pick") or {}).get("main") or {}).get("code")))
        for x in c["dims"]:
            print("    %-8s w=%.2f sc=%-4s %-8s %s" % (
                x["name"], x["weight"], x["score"], x["level"], " | ".join(x["evidence"])))
        print("  vetoes=%s" % c["vetoes"])
        print("  oi=%s" % d["oi_structure"])
        print("  term=%s" % c["term_structure"])
        print("  pool=%d signal=%d summary=%s" % (
            len(d["pool"]), len(d["signal_pool"]), list(d["pool_summary"].keys())))
        for p in d["pool"]:
            print("    %-6s %-6s %-9s pcorr=%-6s beta=%-5s lag=%-2s %s" % (
                p["code"], p["name"], p["tier"], p["pcorr"], p["beta"], p["lag"], p["note"]))
        print("  notes=%s" % d["notes"])
    kb = api_gold_kb({})
    print("kb books=%d dims=%d pitfalls=%d notfound=%d" % (
        len(kb["books"]), len(kb["new_dims"]), len(kb["pitfalls"]), len(kb["not_found"])))
