#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股右侧交易罗盘 · 复刻版 —— 数据与算法引擎（纯标准库，零第三方依赖）

三层结构
--------
1) 数据层
   - 全市场快照：新浪 Market_Center.getHQNodeData（分页并行，落盘 data/snapshot.json，按天缓存）
   - 日 K      ：新浪 CN_MarketData.getKLineData（落盘 data/kline/{code}.json，按交易日保鲜）
   - 行业映射  ：新浪行业节点 newSinaHy.php + 逐节点成分（落盘 data/industry.json，按天缓存）
   - 指数日 K  ：sh000001，用来确定「最近交易日」

2) 指标层
   MA / EMA / MACD(12,26,9) / ATR14 / 布林 / 摆动结构（分型高点低点）
   缠论：包含处理 → 分型 → 笔 → 中枢 → 背驰 → 三类买卖点

3) 策略层
   maturity()   右侧成熟度（结构 25 + 突破 15 + 量能 20 + 动量 15 + 位置 25 = 100）
   checkup()    十二维体检 + 硬性否决 + 操作计划（买点/止损/目标/仓位/离场信号）
   scan_all()   一次全市场扫描，同时产出：
                  data/strong.json     强势股榜（严格四条件）
                  data/right_side.json 右侧六池 + 总榜 + 风险名单 + 市场环境

性能口径
--------
日 K 是唯一的重活（每只票一个请求）。策略：
  · 32 并发，实测约 60 请求/秒，全市场 4700 只首次约 80~120 秒
  · 每只票的日 K 落盘并记 trade_date，同一交易日再扫描直接命中磁盘，0 请求
  · 因此「首次扫描慢、之后秒开」，与页面文案一致
"""

import json
import math
import os
import re
import ssl
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 可写数据目录。桌面便携版（electron-builder portable）每次启动都会把程序解压到
# 一个新的临时目录，若 data/ 跟着解压目录走，等于每次都从零开始：重下 85MB 行情、
# 重扫全市场 3 分钟。所以允许用 COMPASS_STORE 把缓存钉在 exe 旁边的固定目录。
# 命令行直接跑 server.py 时该变量不存在，行为与原来完全一致。
STORE_DIR = os.environ.get("COMPASS_STORE") or BASE_DIR
DATA_DIR = os.path.join(STORE_DIR, "data")
KLINE_DIR = os.path.join(DATA_DIR, "kline")
for _d in (DATA_DIR, KLINE_DIR):
    if not os.path.isdir(_d):
        os.makedirs(_d, exist_ok=True)

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
SINA_REF = "https://finance.sina.com.cn"
_SSL = ssl.create_default_context()
_SSL.check_hostname = False
_SSL.verify_mode = ssl.CERT_NONE

KL_DATALEN = 300          # 需要 MA250 与 5 日回看，300 根够用
KL_WORKERS = 40

_weight_lock = threading.Lock()
_scan_lock = threading.Lock()
_mem = {}
_mem_lock = threading.Lock()

SCAN_STATE = {
    "running": False, "phase": "", "target": "", "total": 0, "done": 0,
    "started_at": None, "finished_at": None, "error": None, "last": {},
}

# ----------------------------------------------------------------------------
# 基础设施
# ----------------------------------------------------------------------------


def cached(key, ttl, producer):
    now = time.time()
    with _mem_lock:
        hit = _mem.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
    val = producer()
    with _mem_lock:
        _mem[key] = (now, val)
    return val


def http_get(url, referer=SINA_REF, timeout=20, encoding="utf-8"):
    headers = {"User-Agent": UA, "Accept": "*/*", "Accept-Language": "zh-CN,zh;q=0.9"}
    if referer:
        headers["Referer"] = referer
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout, context=_SSL) as r:
        return r.read().decode(encoding, "ignore")


def http_json(url, referer=SINA_REF, timeout=20, encoding="utf-8"):
    txt = http_get(url, referer=referer, timeout=timeout, encoding=encoding).strip()
    if txt and txt[0] not in "[{":
        idx = min([i for i in (txt.find("["), txt.find("{")) if i >= 0], default=-1)
        if idx >= 0:
            txt = txt[idx:]
    return json.loads(txt)


def read_json(path, default=None):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def write_json(path, obj):
    """原子写：先写 .tmp 再 replace，避免请求方读到半个文件。

    ★ 2026-11-05：重试原来只包住 os.replace，但 Windows 上真正失败的是**打开 .tmp**
    （杀软/搜索索引会短暂占用刚创建的文件，直接 Permission denied）。实测桌面端跑完
    一轮扫描后日志里出现 `[Errno 13] Permission denied: data/index.json.tmp`，
    index.json 因此没更新 —— 而这正是「数据看起来不更新」的一条暗线。
    另外两个线程同时写同一 path 会抢同一个 .tmp（一个 replace 掉，另一个 open 失败），
    所以临时文件名带上 pid+线程号。
    """
    payload = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    last = None
    for i in range(6):
        tmp = "%s.%d.%d.tmp" % (path, os.getpid(), threading.get_ident())
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(payload)
            os.replace(tmp, path)
            return True
        except OSError as e:
            last = e
            try:
                os.remove(tmp)
            except OSError:
                pass
            time.sleep(0.08 * (i + 1))
    print("  [引擎] 写盘失败:", path, last)
    return False


def _r(v, n=2):
    if v is None:
        return None
    try:
        return round(float(v), n)
    except (TypeError, ValueError):
        return None


def _num(v, d=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


def code_of_symbol(sym):
    """sh600519 -> 600519"""
    s = str(sym or "").strip().lower()
    return s[-6:] if len(s) >= 8 else s


def symbol_of_code(code):
    c = str(code).strip()
    if c[:1] == "6":
        return "sh" + c
    if c[:1] in ("4", "8", "9"):
        return "bj" + c
    return "sz" + c


def secid_of_code(code):
    c = str(code).strip()
    if c[:1] == "6":
        return "1." + c
    return "0." + c


def board_of(code):
    c = str(code)
    if c[:2] in ("60", "68"):
        return "沪市主板" if c[0] == "6" and c[1] == "0" else "科创板"
    if c[:2] in ("00", "30"):
        return {"00": "深市主板", "30": "创业板"}.get(c[:2], "深市主板")
    if c[:2] in ("43", "83", "87", "92"):
        return "北交所"
    if c[:1] == "9":
        return "沪市B股" if c[0] == "9" else "深市B股"
    return "其他"


def is_a_share(code):
    c = str(code)
    if len(c) != 6 or not c.isdigit():
        return False
    if c[:2] in ("60", "68", "00", "30", "43", "83", "87", "92"):
        return True
    return False


def is_st(name):
    n = str(name or "")
    return ("ST" in n.upper()) or ("退" in n) or ("*" in n)


# ----------------------------------------------------------------------------
# 数据层 1：全市场快照
# ----------------------------------------------------------------------------

SNAP_URL = ("https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
            "Market_Center.getHQNodeData?page=%d&num=100&sort=symbol&asc=1&node=%s")
SNAP_FILE = os.path.join(DATA_DIR, "snapshot.json")
# 盘中复用磁盘快照的时长：太短会每秒抓 1.6MB（新浪会限流），太长价格就不动了。
# 60 秒是「实时雷达 10 秒一轮」与「别把新浪打挂」之间的折中：价格最多延迟一分钟。
SNAP_DISK_TTL = 60


def _norm_snap_row(r):
    code = str(r.get("code") or "")
    price = _num(r.get("trade"))
    return {
        "code": code,
        "symbol": r.get("symbol") or symbol_of_code(code),
        "name": r.get("name") or "",
        "price": price,
        "pct": _num(r.get("changepercent")),
        "open": _num(r.get("open")),
        "high": _num(r.get("high")),
        "low": _num(r.get("low")),
        "prevclose": _num(r.get("settlement")),
        "vol": _num(r.get("volume")),          # 股
        "amount": _num(r.get("amount")),        # 元
        "mv_yi": _num(r.get("mktcap")) / 1e4,   # 新浪 mktcap 单位万元 -> 亿元
        "nmc_yi": _num(r.get("nmc")) / 1e4,
        "turnover": _num(r.get("turnoverratio")),
        "pe": _num(r.get("per")),
        "pb": _num(r.get("pb")),
        "board": board_of(code),
    }


def snapshot(ttl=None):
    """全市场快照（沪深A + 北交所，覆盖 ~5700 只）。

    ★ TTL 必须跟着交易时段走，不能写死 120 秒：
      盘中价格每几十秒就变，2 分钟缓存会让「实时雷达/实时行情」看到的其实是
      两分钟前的价；盘后价格不再变，缓存久一点省请求。
      调用方显式传 ttl（如扫描 worker 传 1）时按调用方的来。
    """
    if ttl is None:
        ttl = 20 if trading_window() else 300
    return cached("snapshot", ttl, _snapshot_load)


def trading_window():
    """是否处于「价格会变」的时段：工作日 09:15~15:10（含集合竞价与尾盘）。

    只用来决定缓存长短与是否重抓，不做交易日历判断 —— 节假日会多抓几次，
    代价是几个请求，比「明明开着盘却读到昨天的价」便宜得多。
    """
    now = time.localtime()
    if now.tm_wday >= 5:
        return False
    hm = now.tm_hour * 60 + now.tm_min
    return 9 * 60 + 15 <= hm <= 15 * 60 + 10


def _snapshot_load():
    disk = read_json(SNAP_FILE) or {}
    td = latest_trade_date()
    now = time.time()
    trading = trading_window()
    # ★ 缓存键必须带「时间」，不能只看交易日：
    #   td 一天只变一次，若「td 相同就直接读磁盘」，盘中价格会定格在最后一次
    #   写盘的时刻（2026-11-05 实测：10:35 写盘后到 11:04 再没动过，而快照明明
    #   可以每 20 秒重抓）。这正是「页面数据不自动更新」的根因。
    if disk.get("rows"):
        if not trading and disk.get("td") == td:
            return {"td": td, "rows": disk["rows"], "source": "disk"}
        if now - (disk.get("ts") or 0) < (SNAP_DISK_TTL if trading else 600):
            return {"td": td, "rows": disk["rows"], "source": "disk-fresh"}

    def one(p):
        for node in ("hs_a",):
            try:
                return http_json(SNAP_URL % (p, node), timeout=20) or []
            except Exception:
                pass
        return []

    rows = []
    with ThreadPoolExecutor(max_workers=12) as ex:
        for chunk in ex.map(one, range(1, 80)):
            rows.extend(chunk)
            if not chunk:
                break
    out = []
    for r in rows:
        row = _norm_snap_row(r)
        if not is_a_share(row["code"]) or row["price"] <= 0:
            continue
        out.append(row)
    # ★ 残页不能覆盖整表：盘前（09:15 前）绝大多数股票 price=0 会被过滤掉，
    #   新浪也可能只回首页 —— 2026-11-04 09:10 实测只抓到 2 只，直接把 5218 只的
    #   快照覆盖成 2 只，之后所有榜单全空。这里改为：不足 1000 只、或不足上一份的
    #   70%，都视为抓取失败，沿用磁盘上的旧快照（标记 stale），绝不写盘。
    #   为什么会加「相对比例」这半句：2026-11-05 发现磁盘上那份只有 2000 行，
    #   而盘中完整抓取有 5572 行 —— 盘前那一轮抓了 2000 条有效行，过了 1000 的
    #   绝对阈值就写盘了，结果「全市场池」整天只有 2000 只，页面上看不出来。
    prev = disk.get("rows") or []
    too_few = len(out) < 1000 or (prev and len(out) < len(prev) * 0.7)
    if too_few:
        if prev:
            return {"td": disk.get("td") or td, "rows": prev,
                    "source": "disk-stale", "stale": True, "partial": len(out)}
        return {"td": td, "rows": out, "source": "sina", "stale": True, "partial": len(out)}
    write_json(SNAP_FILE, {"td": td, "rows": out, "ts": now})
    return {"td": td, "rows": out, "source": "sina"}


def snap_index():
    """code -> 快照行"""
    return {r["code"]: r for r in snapshot()["rows"]}


# ----------------------------------------------------------------------------
# 数据层 2：日 K（磁盘保鲜）
# ----------------------------------------------------------------------------

KL_URL = ("https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
          "CN_MarketData.getKLineData?symbol=%s&scale=240&ma=no&datalen=%d")
INDEX_FILE = os.path.join(DATA_DIR, "index.json")


def _kl_path(code):
    return os.path.join(KLINE_DIR, str(code) + ".json")


def kline_get(code):
    """读本地日 K：[ [date, open, close, high, low, vol], ... ]"""
    return (read_json(_kl_path(code)) or {}).get("rows")


def _kline_fetch_raw(symbol, datalen=KL_DATALEN):
    try:
        data = http_json(KL_URL % (symbol, datalen), timeout=25)
    except Exception:
        return None
    if not isinstance(data, list) or not data:
        return None
    rows = []
    for it in data:
        try:
            rows.append([
                it["day"],
                float(it["open"]), float(it["close"]),
                float(it["high"]), float(it["low"]),
                float(it.get("volume") or 0),
            ])
        except (KeyError, TypeError, ValueError):
            continue
    return rows or None


def latest_trade_date():
    """最近一个「已收盘」交易日：上证指数日 K 的最后一根。

    ★ 缓存键不能用「今天抓过就算数」：
      盘前/盘中抓的时候，当日 K 线还没生成（新浪日 K 盘中不返回当日那一根），
      抓到的 td 必然是昨天。若因此把 fetched 记成今天，就再也不会重抓 ——
      **收盘后也切不到今天的 K 线**。2026-11-05 实测：09:06 抓成 2026-11-04 后，
      整个交易日（含 15:00 收盘后）都停在 11-04，所有榜单的 trade_date 跟着错一天。

      正确语义：只有「td 已经是今天」才整天锁定；否则按时间窗口重试，直到当日
      K 线出现。窗口外的重试间隔拉长，避免节假日空转。
    """
    disk = read_json(INDEX_FILE) or {}
    today = time.strftime("%Y-%m-%d")
    if disk.get("td") == today:
        return today

    # 重试间隔分三段：交易窗口内 5 分钟；收盘后 15:10~16:30 每 3 分钟看一眼
    # （当日 K 线就在这段时间生成，早点切过去才能让自动扫描跟上）；
    # 其余时段 30 分钟（夜间/周末没有新 K 线，抓了也白抓）。
    now = time.localtime()
    hm = now.tm_hour * 60 + now.tm_min
    if trading_window():
        ttl = 300
    elif now.tm_wday < 5 and 15 * 60 + 10 <= hm <= 16 * 60 + 30:
        ttl = 180
    else:
        ttl = 1800
    if disk.get("td") and time.time() - (disk.get("checked_ts") or 0) < ttl:
        return disk["td"]

    rows = _kline_fetch_raw("sh000001", 4)
    td = rows[-1][0] if rows else (disk.get("td") or today)

    # ★ 新浪的指数日 K 与个股日 K **不是同时更新**的：2026-11-05 15:33 实测，
    #   sh600000 / sz300750 已经有当日那根，而 sh000001 还停在 11-04。
    #   只拿指数当判据会形成死锁：td 停在昨天 → ensure_klines 认为「已到位」
    #   → 一只都不重抓 → 个股明明有今天的数据也永远进不来。
    #   所以指数还没到今天就再探两只高流动性个股（一沪一深），取最大日期。
    #   日 K 不可能出现未来日期，这里用系统日期兜住异常值。
    if td < today:
        for probe in ("sh600000", "sz000001"):
            pr = _kline_fetch_raw(probe, 2)
            if pr and pr[-1][0] > td and pr[-1][0] <= today:
                td = pr[-1][0]
                rows = pr

    write_json(INDEX_FILE, {
        "td": td, "fetched": today, "checked_ts": time.time(),
        "prev": rows[-2][0] if rows and len(rows) > 1 else td,
    })
    return td


def kline_fresh(code, td=None):
    d = read_json(_kl_path(code)) or {}
    if not d.get("rows"):
        return False
    return d.get("td") == (td or latest_trade_date())


def ensure_klines(codes, td=None, workers=KL_WORKERS, progress=None):
    """把 codes 的日 K 补齐到当前交易日。返回 (ok, failed)。"""
    td = td or latest_trade_date()
    need = []
    for c in codes:
        d = read_json(_kl_path(c)) or {}
        if d.get("td") == td and d.get("rows"):
            continue
        need.append(c)
    total = len(need)
    if not total:
        if progress:
            progress(0, 0)
        return 0, 0

    done = [0]
    failed = [0]
    lock = threading.Lock()

    def one(code):
        rows = _kline_fetch_raw(symbol_of_code(code), KL_DATALEN)
        if rows:
            write_json(_kl_path(code), {"code": code, "td": rows[-1][0], "rows": rows})
            ok = 1
        else:
            ok = 0
        with lock:
            done[0] += 1
            if not ok:
                failed[0] += 1
            if progress and done[0] % 40 == 0:
                progress(done[0], total)
        return ok

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(one, need))
    if progress:
        progress(done[0], total)
    return done[0], failed[0]


# ----------------------------------------------------------------------------
# 数据层 3：行业映射（新浪行业节点）
# ----------------------------------------------------------------------------

HY_FILE = os.path.join(DATA_DIR, "industry.json")
HY_LIST_URL = "https://vip.stock.finance.sina.com.cn/q/view/newSinaHy.php"


def industry_map():
    return cached("industry", 3600, _industry_load)


def _industry_load():
    td = latest_trade_date()
    disk = read_json(HY_FILE) or {}
    if disk.get("td") == td and disk.get("map"):
        return disk["map"]
    try:
        txt = http_get(HY_LIST_URL, encoding="gbk", timeout=15)
        mt = re.search(r"\{.*\}", txt, re.S)
        raw = json.loads(mt.group(0)) if mt else {}
        nodes = {}
        for k, v in raw.items():
            parts = str(v).split(",")
            if len(parts) >= 2:
                nodes[k] = parts[1]
    except Exception:
        return disk.get("map") or {}

    base = ("https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
            "Market_Center.getHQNodeData?page=%d&num=100&sort=symbol&asc=1&node=%s")

    def one(item):
        node, name = item
        got = {}
        for page in range(1, 6):
            try:
                rows = http_json(base % (page, node), timeout=15) or []
            except Exception:
                break
            for r in rows:
                c = str(r.get("code") or "")
                if c:
                    got[c] = name
            if len(rows) < 100:
                break
        return got

    out = {}
    try:
        with ThreadPoolExecutor(max_workers=10) as ex:
            for got in ex.map(one, list(nodes.items())):
                out.update(got)
    except Exception:
        pass
    if out:
        write_json(HY_FILE, {"td": td, "map": out})
    return out


# ----------------------------------------------------------------------------
# 指标层
# ----------------------------------------------------------------------------


def sma(vals, n):
    out = [None] * len(vals)
    s = 0.0
    for i, v in enumerate(vals):
        s += v
        if i >= n:
            s -= vals[i - n]
        if i >= n - 1:
            out[i] = s / n
    return out


def ema(vals, n):
    out = [None] * len(vals)
    k = 2.0 / (n + 1.0)
    e = None
    for i, v in enumerate(vals):
        e = v if e is None else e + k * (v - e)
        out[i] = e
    return out


def macd(closes, fast=12, slow=26, sig=9):
    ef = ema(closes, fast)
    es = ema(closes, slow)
    dif = [a - b for a, b in zip(ef, es)]
    dea = ema(dif, sig)
    hist = [2.0 * (d - s) for d, s in zip(dif, dea)]
    return dif, dea, hist


def atr(rows, n=14):
    trs = []
    for i in range(1, len(rows)):
        h, l, pc = rows[i][3], rows[i][4], rows[i - 1][2]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < n:
        return None
    return sum(trs[-n:]) / float(n)


def boll(closes, n=20, k=2.0):
    if len(closes) < n:
        return None, None, None
    win = closes[-n:]
    mid = sum(win) / n
    var = sum((x - mid) ** 2 for x in win) / n
    sd = math.sqrt(var)
    return mid + k * sd, mid, mid - k * sd


def swing_points(rows, k=3):
    """3 根确认的摆动高/低点（原始 K，不做包含处理）。"""
    highs, lows = [], []
    n = len(rows)
    for i in range(k, n - k):
        h = rows[i][3]
        l = rows[i][4]
        if all(h >= rows[j][3] for j in range(i - k, i + k + 1) if j != i):
            highs.append((i, h))
        if all(l <= rows[j][4] for j in range(i - k, i + k + 1) if j != i):
            lows.append((i, l))
    return highs, lows


# ----------------------------------------------------------------------------
# 指标层：缠论（简化但完整的一条链）
# ----------------------------------------------------------------------------


def _merge_bars(rows):
    """包含关系处理。返回 [[high, low, first_idx, last_idx], ...]"""
    bars = []
    for i, r in enumerate(rows):
        h, l = r[3], r[4]
        if not bars:
            bars.append([h, l, i, i])
            continue
        ph, pl = bars[-1][0], bars[-1][1]
        contain = (h <= ph and l >= pl) or (h >= ph and l <= pl)
        if contain:
            up = True
            if len(bars) >= 2:
                up = bars[-1][0] >= bars[-2][0]
            if up:
                bars[-1][0] = max(ph, h)
                bars[-1][1] = max(pl, l)
            else:
                bars[-1][0] = min(ph, h)
                bars[-1][1] = min(pl, l)
            bars[-1][3] = i
        else:
            bars.append([h, l, i, i])
    return bars


def _fractals(bars):
    """合并 K 上的分型；索引统一换成**原始 K 的下标**（bars[i][3]），
    这样下游拿它去切 hist / closes 不会错位。"""
    out = []
    for i in range(1, len(bars) - 1):
        h, l = bars[i][0], bars[i][1]
        ph, pl = bars[i - 1][0], bars[i - 1][1]
        nh, nl = bars[i + 1][0], bars[i + 1][1]
        idx = bars[i][3]
        if h > ph and h > nh and l > pl and l > nl:
            out.append(["top", idx, h])
        elif l < pl and l < nl and h < ph and h < nh:
            out.append(["bot", idx, l])
    return out


def _bi_list(fr, min_gap=4):
    """分型 → 笔。返回 [(i0, i1, p0, p1, dir)]"""
    bi = []
    last = None
    for kind, i, px in fr:
        if last is None:
            last = [kind, i, px]
            continue
        if kind == last[0]:
            if (kind == "top" and px > last[2]) or (kind == "bot" and px < last[2]):
                last = [kind, i, px]
            continue
        if i - last[1] >= min_gap and ((kind == "top" and px > last[2]) or
                                      (kind == "bot" and px < last[2])):
            bi.append((last[1], i, last[2], px, "up" if kind == "top" else "down"))
            last = [kind, i, px]
    return bi


def _centers(bi):
    """三个连续笔重叠成中枢；只保留互不重叠的连续中枢。"""
    out = []
    i = 0
    while i + 2 < len(bi):
        seg = bi[i:i + 3]
        highs = [max(b[2], b[3]) for b in seg]
        lows = [min(b[2], b[3]) for b in seg]
        zg, zd = min(highs), max(lows)
        if zg <= zd:
            i += 1
            continue
        j = i + 3
        while j < len(bi):
            h = max(bi[j][2], bi[j][3])
            l = min(bi[j][2], bi[j][3])
            if l <= zg and h >= zd:
                j += 1
            else:
                break
        out.append({"zg": zg, "zd": zd, "from": bi[i][0], "to": bi[min(j, len(bi)) - 1][1]})
        i = j
    return out


def chan(rows):
    """缠论结构：走势类型 / 笔 / 中枢 / 背驰 / 三类买卖点 → 0~100 分。"""
    if not rows or len(rows) < 40:
        return None
    bars = _merge_bars(rows)
    fr = _fractals(bars)
    bi = _bi_list(fr)
    closes = [r[2] for r in rows]
    _, _, hist = macd(closes)
    centers = _centers(bi)

    # 只认「近端」中枢：远端中枢（30 个交易日以前结束的）不再主导走势判断
    recent = [c for c in centers if c["to"] >= len(rows) - 70] or centers

    walk = "盘整"
    if recent:
        # 走势类型按「现价相对最后一个近端中枢的位置」判：离开中枢上沿=上涨、
        # 跌破下沿=下跌、还在中枢内=盘整。中枢序列只作加减分，不单独定生死。
        c = recent[-1]
        if closes[-1] > c["zg"]:
            walk = "上涨"
        elif closes[-1] < c["zd"]:
            walk = "下跌"
    elif len(bi) >= 3:
        if bi[-1][4] == "up" and bi[-3][2] < bi[-1][2]:
            walk = "上涨"
        elif bi[-1][4] == "down" and bi[-3][2] > bi[-1][2]:
            walk = "下跌"

    # 背驰：最后两个同向笔的 MACD 面积对比（新高低点 + 面积收缩 25% 以上）
    # 与买卖点同理，最后一笔可能仍在延伸：用收盘价修正端点，否则会出现
    # 「walk=上涨 却判底背驰」的自相矛盾（笔端点滞后于最新价格）。
    divergence = None
    same = [b for b in bi if b[4] == "up"]
    if len(same) >= 2 and hist:
        a, b = same[-2], same[-1]
        area_a = sum(abs(h) for h in hist[a[0]:a[1] + 1] if h)
        area_b = sum(abs(h) for h in hist[b[0]:b[1] + 1] if h)
        b_hi = max(b[3], closes[-1]) if b is bi[-1] else b[3]
        if b_hi > a[3] and area_b < area_a * 0.75 and area_a > 0:
            divergence = "顶背驰"
    if divergence is None:
        same = [b for b in bi if b[4] == "down"]
        if len(same) >= 2 and hist:
            a, b = same[-2], same[-1]
            area_a = sum(abs(h) for h in hist[a[0]:a[1] + 1] if h)
            area_b = sum(abs(h) for h in hist[b[0]:b[1] + 1] if h)
            b_lo = min(b[3], closes[-1]) if b is bi[-1] else b[3]
            if b_lo < a[3] and area_b < area_a * 0.75 and area_a > 0:
                divergence = "底背驰"

    # 三类买卖点（最近一笔方向 + 与最后中枢的位置关系）
    #   三买：向上离开中枢后，回调笔的低点不进中枢（低点 > ZG）
    #   三卖：向下离开中枢后，反抽笔的高点不进中枢（高点 < ZD）
    bs = None
    if recent and len(bi) >= 2:
        c = recent[-1]
        last = bi[-1]
        prev = bi[-2]
        # ★ 未完成笔修正（2026-11-03）
        # 笔的端点是「最后一个已确认分型」，而最新收盘可能已经延伸出这段笔之外。
        # 直接用未完成笔的端点判三类买卖点，会出现「收盘在 ZG 上方、却判三卖」的
        # 自相矛盾：贵州茅台 2026-11-02 收盘 1439，而在用的向上笔端点只有 1288，
        # 于是既 walk=上涨（close > ZG 1338）又 bs=三卖（1288 < ZD 1332）。
        # 这里用收盘价对最后一笔端点做延伸修正，让买卖点与现价同步 —— 宁可漏判
        # 一个尚未确认的买点，也不要把正在延伸的上涨判成卖点。
        last_px = max(last[3], closes[-1]) if last[4] == "up" else min(last[3], closes[-1])
        if last[4] == "down" and prev[4] == "up":
            if prev[3] > c["zg"] and last_px > c["zg"]:
                bs = "第三类买点（三买）"
        elif last[4] == "up" and prev[4] == "down":
            if prev[3] < c["zd"] and last_px < c["zd"]:
                bs = "第三类卖点（三卖）"

    # ---- 打分 ----
    # 惩罚做「相干性修正」：均线仍多头（收盘 > MA20 > MA60）时，中枢下移多半只是回踩，
    # 不能和真正的下跌趋势同罚，否则强势股会被结构分一票打到 0 分。
    ma20 = sma(closes, 20)[-1]
    ma60 = sma(closes, 60)[-1]
    strong_up = bool(ma20 and ma60 and closes[-1] > ma20 > ma60)
    score = 50
    if walk == "上涨":
        score += 15
    elif walk == "下跌":
        score += -6 if strong_up else -18
    if bi:
        score += 6 if bi[-1][4] == "up" else -6
    if len(recent) >= 2:
        if recent[-1]["zd"] > recent[-2]["zg"]:
            score += 8
        elif recent[-1]["zg"] < recent[-2]["zd"]:
            score += -4 if strong_up else -8
    if divergence == "顶背驰":
        score += -8 if strong_up else -16
    elif divergence == "底背驰":
        score += 5
    if bs:
        if "三买" in bs:
            score += 10
        elif "三卖" in bs:
            score += -20
    score = max(0, min(100, int(round(score))))

    grade = "good" if score >= 80 else ("watch" if score >= 60 else "risk")
    grade_text = {"good": "结构健康（趋势明确、无反向证据）",
                  "watch": "结构中性（需等确认）",
                  "risk": "结构走坏（反向证据成立）"}[grade]
    segs = [{"from": b[0], "to": b[1], "dir": b[4], "p0": _r(b[2]), "p1": _r(b[3])}
            for b in bi[-8:]]
    return {
        "score": score, "score_max": 100, "grade": grade, "grade_text": grade_text,
        "walk": walk, "bs": bs, "divergence": divergence,
        "centers": [{"zg": _r(c["zg"]), "zd": _r(c["zd"]),
                     "from": c["from"], "to": c["to"]} for c in centers[-3:]],
        "segments": segs,
        "n_bi": len(bi), "n_center": len(centers),
    }


# ----------------------------------------------------------------------------
# 特征提取
# ----------------------------------------------------------------------------


def features(code, rows, snap=None):
    """单只票的全部量化特征（一次算完，供成熟度/六池/体检共用）。"""
    n = len(rows)
    if n < 60:
        return None
    closes = [r[2] for r in rows]
    highs = [r[3] for r in rows]
    lows = [r[4] for r in rows]
    vols = [r[5] for r in rows]
    close = closes[-1]
    prev = closes[-2] if n > 1 else close

    f = {"code": code, "bars": n, "close": close, "prev_close": prev,
         "date": rows[-1][0], "rows": rows}
    for p in (5, 10, 20, 40, 60, 120, 250):
        ma = sma(closes, p)
        f["ma%d" % p] = ma[-1] if len(ma) >= p else None
        if p == 120:
            f["ma120_prev"] = ma[-6] if len(ma) >= p + 5 else None
        if p == 20:
            f["ma20_prev"] = ma[-6] if len(ma) >= p + 5 else None

    dif, dea, hist = macd(closes)
    f["dif"], f["dea"], f["hist"] = dif[-1], dea[-1], hist[-1]
    f["hist_prev"] = hist[-2] if n > 1 else None
    f["dif_prev"], f["dea_prev"] = dif[-2] if n > 1 else None, dea[-2] if n > 1 else None

    a = atr(rows, 14)
    f["atr"] = a
    f["atr_pct"] = (a / close * 100.0) if a and close else None

    up, mid, low = boll(closes, 20, 2.0)
    f["boll_upper"], f["boll_mid"], f["boll_lower"] = up, mid, low

    base5 = sum(vols[-6:-1]) / 5.0 if n >= 6 else 0
    f["vol_ratio"] = (vols[-1] / base5) if base5 > 0 else None
    f["vol_ma5"] = sum(vols[-5:]) / 5.0 if n >= 5 else None
    f["vol_ma20"] = sum(vols[-20:]) / 20.0 if n >= 20 else None

    ma20 = f.get("ma20")
    f["dev20"] = ((close - ma20) / ma20 * 100.0) if ma20 else None
    hi52 = max(highs[-250:]) if n >= 60 else max(highs)
    lo52 = min(lows[-250:]) if n >= 60 else min(lows)
    f["hi250"], f["lo250"] = hi52, lo52
    f["pos52"] = ((close - lo52) / (hi52 - lo52) * 100.0) if hi52 > lo52 else None
    f["hi60"] = max(highs[-60:])
    f["hi20"] = max(highs[-20:])
    f["lo20"] = min(lows[-20:])
    f["chg20"] = ((close / closes[-21] - 1) * 100.0) if n > 21 else None
    f["chg60"] = ((close / closes[-61] - 1) * 100.0) if n > 61 else None
    f["pct"] = ((close / prev - 1) * 100.0) if prev else 0.0
    f["open"] = rows[-1][1]
    f["high"], f["low"] = highs[-1], lows[-1]
    f["vol"] = vols[-1]
    # 20 日均额（元）：新浪日 K 没有成交额字段，用 量×收盘 估算，量级一致
    amts = [vols[i] * closes[i] for i in range(max(0, n - 20), n)]
    f["amount20"] = sum(amts) / len(amts) if amts else 0.0
    f["amount_today"] = vols[-1] * close

    # 平台振幅
    h20, l20 = max(highs[-20:]), min(lows[-20:])
    f["amp20"] = ((h20 - l20) / l20 * 100.0) if l20 > 0 else None

    # 摆动结构
    sh, sl = swing_points(rows, 3)
    f["sw_highs"] = sh[-4:]
    f["sw_lows"] = sl[-4:]
    f["swing_low"] = sl[-1][1] if sl else min(lows[-20:])
    hh = len(sh) >= 2 and sh[-1][1] > sh[-2][1]
    hl = len(sl) >= 2 and sl[-1][1] > sl[-2][1]
    f["hh"], f["hl"] = hh, hl

    if snap:
        f["name"] = snap.get("name") or ""
        f["mv_yi"] = snap.get("mv_yi")
        f["turnover"] = snap.get("turnover")
        f["pe"] = snap.get("pe")
        f["pb"] = snap.get("pb")
    return f


# ----------------------------------------------------------------------------
# 策略层：位置分级 + 右侧成熟度
# ----------------------------------------------------------------------------

LEVELS = {
    "pos3": ("位置3 · 右侧安全位", "贴近均线，处于右侧确认后的低风险介入区", "推荐"),
    "pos2": ("位置2 · 中间位", "已离均线一段距离，需等回踩确认再动手", "谨慎"),
    "pos1": ("位置1 · 追高位", "大幅偏离 20 日线，追进去就是位置1", "回避"),
}


def level_of(dev20):
    if dev20 is None:
        return "pos2"
    if dev20 > 15:
        return "pos1"
    if dev20 > 5:
        return "pos2"
    return "pos3"


def maturity(f):
    """右侧成熟度 0~100：结构 25 + 突破 15 + 量能 20 + 动量 15 + 位置 25。"""
    close = f["close"]
    ma5, ma10, ma20 = f.get("ma5"), f.get("ma10"), f.get("ma20")
    ma40, ma60 = f.get("ma40"), f.get("ma60")
    dev20 = f.get("dev20")
    tags, reasons = [], []

    # ---- 结构（25）----
    struct = 0
    if ma5 and ma10 and ma20 and ma40 and ma60:
        if ma5 > ma10 > ma20 > ma40 > ma60:
            struct, t = 25, "完全多头排列"
            tags.append("五线完全多头")
            reasons.append("MA5>MA10>MA20>MA40>MA60 完全多头排列")
        elif ma5 > ma10 > ma20 > ma60:
            struct, t = 20, "四线多头"
            tags.append("四线多头排列")
            reasons.append("MA5>MA10>MA20>MA60 四线多头排列")
        elif ma5 > ma10 > ma20:
            struct, t = 14, "短多排列"
            tags.append("短多排列")
            reasons.append("MA5>MA10>MA20 短多排列，中长均线未完全跟上")
        elif ma5 > ma20:
            struct, t = 7, "均线交错"
            reasons.append("均线仅部分多头，结构尚未收敛")
        else:
            struct, t = 0, "空头排列"
            reasons.append("均线空头排列，结构不支持右侧介入")
        f["struct_text"] = t
    else:
        f["struct_text"] = "数据不足"

    # 道氏摆动加分/减分（并入结构，上限仍 25）
    if f.get("hh") and f.get("hl"):
        struct = min(25, struct + 2)
        tags.append("高低点同步抬高")
    elif not f.get("hh") and not f.get("hl"):
        struct = max(0, struct - 3)

    # ---- 突破（15）----
    brk, brk_tag = 0, None
    if close >= f["hi60"] * 0.999:
        brk, brk_tag = 15, "创 60 日新高"
    elif close >= f["hi20"] * 0.999:
        brk, brk_tag = 10, "突破 20 日新高"
    elif f.get("hi60") and close >= f["hi60"] * 0.97:
        brk, brk_tag = 6, "逼近 60 日高点"
    if brk_tag:
        tags.append(brk_tag)
        reasons.append(brk_tag + ("（%s）" % _r(f["hi60"])))

    # ---- 量能（20）----
    vr = f.get("vol_ratio")
    vol = 10
    if vr is not None:
        if 1.2 <= vr <= 2.5:
            vol = 20
            reasons.append("5 日均量 / 20 日均量放大到 %.2f，量能温和" % vr)
        elif 1.0 <= vr < 1.2:
            vol = 15
        elif 2.5 < vr <= 4.0:
            vol = 13
            reasons.append("量比 %.2f 偏高，注意高位分歧" % vr)
        elif vr > 4.0:
            vol = 8
            tags.append("天量")
            reasons.append("量比 %.2f 属天量，警惕资金分歧" % vr)
        elif 0.7 <= vr < 1.0:
            vol = 12
            reasons.append("量比 %.2f 缩量，观察是否惜售" % vr)
        else:
            vol = 6
    if f["close"] > f["open"] and vr and vr >= 1.5:
        tags.append("放量收阳")

    # ---- 动量（15）----
    mom = 4
    if f.get("dif") is not None and f.get("dea") is not None:
        if f["dif"] > f["dea"] and f["dif"] > 0:
            mom = 15
            reasons.append("MACD 零轴上方红柱（DIF %.3f > DEA %.3f）" % (f["dif"], f["dea"]))
            tags.append("MACD 零轴上")
        elif f["dif"] > f["dea"]:
            mom = 10
            reasons.append("MACD 红柱但在零轴下方，属弱反弹")
        elif f["dif"] > 0:
            mom = 7
            reasons.append("MACD 零轴上但已翻绿，动量衰减")
        else:
            mom = 2
            reasons.append("MACD 零轴下方死叉，动量偏弱")

    # ---- 位置（25）----
    d = dev20 if dev20 is not None else 8.0
    if d <= 3:
        pos, pos_note = 25, "紧贴 20 日线，回撤空间最小"
    elif d <= 5:
        pos, pos_note = 22, "距 20 日线 5% 以内，仍是右侧安全位"
    elif d <= 8:
        pos, pos_note = 17, "略偏离均线，等回踩更稳"
    elif d <= 10:
        pos, pos_note = 13, "偏离接近一成，追高风险上升"
    elif d <= 15:
        pos, pos_note = 6, "偏离一成以上，进入位置2 上沿"
    else:
        pos, pos_note = 0, "偏离 20 日线超 15%，属位置1 追高位"
    reasons.append("距 MA20 %+.1f%% · 52 周位置 %s%%" % (d, _r(f.get("pos52"), 0)))

    score = int(round(struct + brk + vol + mom + pos))
    score = max(0, min(100, score))
    return {
        "score": score,
        "sub": {"struct": struct, "break": brk, "vol": vol, "mom": mom, "pos": pos},
        "tags": tags,
        "reasons": reasons,
        "pos_note": pos_note,
        "struct_text": f.get("struct_text"),
    }


# ----------------------------------------------------------------------------
# 策略层：量价八态（与前端九宫格同口径）
# ----------------------------------------------------------------------------

VP8 = {
    "升增": ("量增价升", "buy", "增量资金进场推动，趋势健康，最常见的多头主动形态；若已处高位且量极端放大，警惕资金分歧"),
    "升平": ("量平价升", "buy", "量能平稳而价格上行，筹码锁定良好，趋势可持续"),
    "升减": ("量减价升", "背离", "缩量上涨，靠惯性主推，需警惕后继乏力"),
    "平增": ("量增价平", "变盘", "放量滞涨，多空分歧加大，方向待选"),
    "平平": ("量平价平", "均衡", "量价双平，无信息，等方向"),
    "平减": ("量减价平", "警戒", "缩量横盘，观望情绪浓，突破前常见"),
    "跌增": ("量增价跌", "杀跌", "放量下跌，抛压主动释放，右侧纪律不建仓"),
    "跌平": ("量平价跌", "走弱", "缩量阴跌，趋势转弱"),
    "跌减": ("量减价跌", "阴跌", "缩量下跌，卖压衰竭中，等右侧信号"),
}


def volprice(f):
    """量价关系：八态命名 + 三分区闸门（pass/watch/block）+ 警示/确认标签。"""
    pct = f.get("pct") or 0.0
    vr = f.get("vol_ratio") or 1.0
    ma5v = f.get("vol_ma5") or 0
    pdir = "升" if pct > 0.3 else ("跌" if pct < -0.3 else "平")
    vdir = "增" if vr > 1.1 else ("减" if vr < 0.9 else "平")
    name, level, desc = VP8[pdir + vdir]

    warn, confirm, mid, info = [], [], [], []
    close, op, hi, lo = f["close"], f["open"], f["high"], f["low"]
    rng = max(hi - lo, 1e-6)
    upper_shadow = (hi - max(op, close)) / rng

    if pdir == "跌" and vr > 1.8:
        warn.append({"tag": "放量下跌", "name": "放量下跌",
                     "desc": "放量杀跌，主动抛压释放", "evidence": "量比 %.2f · 跌幅 %.2f%%" % (vr, pct)})
    if 2.5 < vr and abs(pct) < 1.5:
        warn.append({"tag": "天量滞涨", "name": "天量滞涨",
                     "desc": "天量而价格不动，多为出货或换手", "evidence": "量比 %.2f · 涨幅 %.2f%%" % (vr, pct)})
    if upper_shadow > 0.55 and pct > -3:
        warn.append({"tag": "长上影", "name": "长上影",
                     "desc": "上冲被砸回，上方抛压重", "evidence": "上影占全天振幅 %.0f%%" % (upper_shadow * 100)})
    if close >= f["hi20"] * 0.999 and vr >= 1.5:
        confirm.append({"tag": "放量突破", "name": "放量突破",
                        "desc": "放量创 20 日新高，突破有真金白银", "evidence": "量比 %.2f" % vr})
    if vdir == "减" and (f.get("dev20") or 0) < 8 and pct > -2:
        confirm.append({"tag": "缩量回调", "name": "缩量回调",
                        "desc": "回踩缩量，获利盘惜售", "evidence": "量比 %.2f" % vr})
    if ma5v and f.get("vol_ma20") and (f["vol_ma20"] / ma5v) < 1.05 and (f.get("pos52") or 50) < 45:
        confirm.append({"tag": "低位堆量", "name": "低位堆量",
                        "desc": "低位量能持续堆积，资金吸筹迹象", "evidence": "52 周位置 %s%%" % _r(f.get("pos52"), 0)})
    if pdir == "平" and vr > 2:
        mid.append({"tag": "天量变盘", "name": "天量变盘",
                    "desc": "天量十字星，方向待选", "evidence": "量比 %.2f" % vr})
    if vr < 0.5:
        mid.append({"tag": "地量", "name": "地量",
                    "desc": "成交量极度萎缩，常见于变盘前夜", "evidence": "量比 %.2f" % vr})

    gate = "block" if warn else ("pass" if confirm else "watch")
    return {
        "gate": gate, "penalty": -10 * len(warn), "bonus": 4 * len(confirm),
        "warn": warn, "confirm": confirm, "mid": mid, "info": info,
        "counts": {"warn": len(warn), "confirm": len(confirm), "mid": len(mid), "info": len(info)},
        "vp8": {"name": name, "level": level, "desc": desc,
                "price_dir": pdir, "vol_dir": vdir},
        "stage": ("拉升期" if (f.get("dev20") or 0) > 0 and pdir == "升" else
                  ("回调期" if pdir == "跌" else "整理期")),
        "advice": ("不建仓（量价警示）" if gate == "block" else
                   ("可建仓（量价确认）" if gate == "pass" else "持有/观察")),
        "pos120": _r(f.get("pos52")),
        "vol_ratio_today": _r(vr),
        "pulse": {"today": bool(vr and vr > 2), "recent": 0},
        "kpattern": {"text": "", "counts": {"bull": 0, "bear": 0, "flat": 0}, "hits": []},
        "trade_date": f.get("date"),
    }


# ----------------------------------------------------------------------------
# 策略层：操作计划（买点 / 止损 / 目标 / 仓位 / 离场）
# ----------------------------------------------------------------------------


def make_plan(f, level_key):
    close = f["close"]
    ma5, ma10, ma20 = f.get("ma5") or close, f.get("ma10") or close, f.get("ma20") or close
    a = f.get("atr") or close * 0.03
    swing_low = f.get("swing_low") or ma20 * 0.97

    if level_key == "pos3":
        lo = min(ma10, close) * 0.99
        hi = max(close, ma5) * 1.01
        note = ""
        position = "回踩 %.2f~%.2f 建底仓 30%% → 站稳 MA5 后加至 60%% → 突破 %.2f 补足" % (lo, hi, f["hi60"])
    elif level_key == "pos2":
        lo, hi = ma10 * 0.99, ma10 * 1.02
        note = "现价已离均线一段，不追现价，等回踩 MA10 再动手"
        position = "回踩 %.2f~%.2f 建底仓 25%% → 确认后加至 50%%" % (lo, hi)
    else:
        lo, hi = ma20 * 0.99, ma20 * 1.03
        note = "位置1 追高位：按纪律不参与，等回落到 MA20 附近再评估"
        position = "不建仓（位置1）"

    # 止损幅度 = min(1.6×ATR%, 结构失效位距离, 8%)，下限 2.5%：
    #   结构位（摆动低点×0.995）破了说明右侧信号失效；ATR 位控制正常波动噪音；
    #   两者取更紧的一侧，再用 8% 封顶防止单笔风险失控。
    d_struct = (close - swing_low * 0.995) / close * 100.0 if close else 8.0
    d_atr = (1.6 * a / close * 100.0) if close else 6.0
    dist = min(max(2.5, min(d_struct, d_atr)), 8.0)
    stop = close * (1.0 - dist / 100.0)
    stop_pct = -dist

    risk = close - stop
    t1 = close + 2.0 * risk
    t2 = max(f.get("hi60") or 0, close + 3.5 * risk)
    return {
        "buy_zone": "%.2f ~ %.2f" % (lo, hi),
        "buy_note": note,
        "stop_loss": _r(stop),
        "stop_pct": _r(stop_pct, 1),
        "target1": _r(t1),
        "target2": _r(t2),
        "position": position,
        "hold": "趋势不破则持有，跌破 20 日线离场",
        "sell_signal": "① 跌破 MA10（%.2f）且次日不收复；② 放量长上影/放量下跌；③ 高位 MACD 死叉" % ma10,
    }


# ----------------------------------------------------------------------------
# 策略层：十二维体检
# ----------------------------------------------------------------------------

DIMS = [
    ("position", "位置安全度", 0.15),
    ("trend", "趋势结构", 0.10),
    ("momentum", "动量确认", 0.08),
    ("volprice", "量价关系", 0.12),
    ("chan", "缠论结构", 0.15),
    ("mainwave", "主升浪买点", 0.10),
    ("buildup", "主力建仓", 0.06),
    ("dragon", "龙头气质", 0.06),
    ("sentiment", "情绪周期", 0.05),
    ("fib4", "斐波那契四度", 0.05),
    ("pankou", "资金盘口", 0.05),
    ("valuation", "估值基本面", 0.03),
]
STRATEGY_KEYS = ("position", "trend", "momentum", "volprice", "chan")
STRATEGY_W = sum(w for k, _, w in DIMS if k in STRATEGY_KEYS)


def _dim_position(f):
    d = f.get("dev20")
    if d is None:
        return 50, "位置未知", ["日 K 不足，无法计算 20 日线偏离"], ""
    if d <= 3:
        s, adv = 100, "已处右侧安全位"
    elif d <= 5:
        s, adv = 85, "仍在右侧安全位（5% 以内）"
    elif d <= 8:
        s, adv = 70, "略偏离，等回踩更优"
    elif d <= 10:
        s, adv = 50, "偏离偏大，需回踩"
    elif d <= 15:
        s, adv = 25, "位置2 上沿，追高风险明显"
    else:
        s, adv = 0, "位置1 追高位，按纪律回避"
    ev = ["距 MA20 %+.2f%%" % d]
    lv = level_of(d)
    ev.append({"pos3": "贴近均线，属右侧确认后的低风险介入区",
               "pos2": "已离均线一段距离，需等回踩确认",
               "pos1": "大幅偏离均线，属追高位"}[lv])
    ev.append("位置三内部三态：%s" % ("横盘整理" if abs((f.get("chg20") or 0)) < 10 else
                                      ("上攻中" if (f.get("chg20") or 0) > 0 else "回落中")))
    return s, lv, ev, adv


def _dim_trend(f):
    ma5, ma10, ma20, ma60 = f.get("ma5"), f.get("ma10"), f.get("ma20"), f.get("ma60")
    s = 30
    ev = []
    if all(v is not None for v in (ma5, ma10, ma20, ma60)):
        ev.append("MA5/10/20/60 = %.2f / %.2f / %.2f / %.2f" % (ma5, ma10, ma20, ma60))
        if ma5 > ma10 > ma20 > ma60:
            s = 100
        elif ma5 > ma10 > ma20:
            s = 80
        elif ma5 > ma20:
            s = 60
        else:
            s = 25
    if f.get("hh") and f.get("hl"):
        s = min(100, s + 5)
        ev.append("道氏摆动结构：更高高点 + 更高低点（+5）")
    elif not f.get("hh") and not f.get("hl"):
        s = max(0, s - 10)
        ev.append("道氏摆动结构：高点与低点同步下移（-10）")
    else:
        ev.append("道氏摆动结构：结构分歧（一升一降）（0）")
    if f.get("ma20") and f.get("ma20_prev"):
        slope = (f["ma20"] / f["ma20_prev"] - 1) * 100
        ev.append("MA20 斜率 %.2f%%" % slope)
        if slope < -1:
            s = max(0, s - 10)
    return s, f.get("struct_text") or "—", ev, ""


def _dim_momentum(f):
    dif, dea, hist = f.get("dif"), f.get("dea"), f.get("hist")
    ev = ["MACD DIF %.3f / DEA %.3f / 柱 %.3f" % (dif or 0, dea or 0, hist or 0)]
    s = 40
    if dif is not None and dea is not None:
        if dif > dea and dif > 0:
            s = 100
        elif dif > dea:
            s = 65
        elif dif > 0:
            s = 45
        else:
            s = 20
    chg20 = f.get("chg20") or 0
    ev.append("近 20 日 %+.1f%%" % chg20)
    if chg20 > 30:
        s = max(0, s - 10)
        ev.append("20 日涨幅超 30%，动量有过热风险（-10）")
    if f.get("hist_prev") is not None and hist is not None:
        ev.append("MACD 柱%s" % ("放大" if hist > f["hist_prev"] else "收缩"))
    return s, ("零轴上多头" if (dif or 0) > 0 else "零轴下"), ev, ""


def _dim_volprice(f, vp):
    s = {"pass": 85, "watch": 60, "block": 25}[vp["gate"]]
    ev = ["量价八态：%s（%s）" % (vp["vp8"]["name"], vp["vp8"]["level"]),
          "今日量比 %s" % _r(f.get("vol_ratio"))]
    if vp["warn"]:
        ev.append("警示：" + "、".join(x["tag"] for x in vp["warn"]))
    if vp["confirm"]:
        ev.append("确认：" + "、".join(x["tag"] for x in vp["confirm"]))
    return s, vp["vp8"]["name"], ev, ""


def _dim_chan(f, ch):
    if not ch:
        return 50, "数据不足", ["日 K 不足 40 根，缠论无法成型"], ""
    ev = ["走势类型：%s · 笔 %d · 中枢 %d" % (ch["walk"], ch["n_bi"], ch["n_center"])]
    if ch["bs"]:
        ev.append("买卖点：" + ch["bs"])
    if ch["divergence"]:
        ev.append("背驰：" + ch["divergence"])
    return ch["score"], ch["walk"], ev, ""


def _dim_mainwave(f):
    dev20 = f.get("dev20") or 0
    chg60 = f.get("chg60") or 0
    s = 40
    ev = ["近 60 日 %+.1f%% · 距 MA20 %+.1f%%" % (chg60, dev20)]
    if f.get("ma5") and f.get("ma10") and f.get("ma20") and f["ma5"] > f["ma10"] > f["ma20"]:
        s += 15
        ev.append("均线多头，具备主升浪基础（+15）")
    if 15 <= chg60 <= 60:
        s += 15
        ev.append("60 日涨幅落在主升区间 15%~60%（+15）")
    elif chg60 > 100:
        s -= 10
        ev.append("60 日涨幅已超 100%，主升段或已过半（-10）")
    if dev20 <= 5:
        s += 20
        ev.append("贴近 MA20，是主升浪中最好的上车位置（+20）")
    elif dev20 > 15:
        s -= 25
        ev.append("距 MA20 超 15%，属主升浪末段的追高（-25）")
    if f.get("close") and f.get("hi60") and f["close"] >= f["hi60"] * 0.999:
        s += 10
        ev.append("创 60 日新高，处于主升段（+10）")
    return max(0, min(100, s)), ("主升浪中" if dev20 <= 5 and chg60 > 10 else "非主升买点"), ev, ""


def _dim_buildup(f):
    vr = f.get("vol_ratio") or 1.0
    pos52 = f.get("pos52") or 50
    s = 50
    ev = ["量比 %.2f · 52 周位置 %s%%" % (vr, _r(pos52, 0))]
    if 1.0 <= vr <= 1.8:
        s += 15
        ev.append("量能温和放大，符合吸筹特征（+15）")
    if pos52 < 45:
        s += 20
        ev.append("处于 52 周区间下沿，建仓成本区（+20）")
    elif pos52 > 85:
        s -= 15
        ev.append("已在 52 周高位，不是建仓区（-15）")
    if f.get("ma20") and f.get("close") and f["close"] > f["ma20"] and (f.get("dev20") or 0) < 8:
        s += 10
        ev.append("站上 MA20 且未偏离，底部抬高（+10）")
    return max(0, min(100, s)), "吸筹迹象" if s >= 65 else "无明显建仓", ev, ""


def _dim_dragon(f):
    chg20 = f.get("chg20") or 0
    pos52 = f.get("pos52") or 50
    s = 40
    ev = ["20 日涨幅 %+.1f%% · 52 周位置 %s%%" % (chg20, _r(pos52, 0))]
    if chg20 > 20:
        s += 25
        ev.append("阶段涨幅领先，具备龙头相（+25）")
    elif chg20 > 8:
        s += 12
    if pos52 > 80:
        s += 15
        ev.append("价格在 52 周高位，强势（+15）")
    if f.get("close") and f.get("hi60") and f["close"] >= f["hi60"] * 0.999:
        s += 10
        ev.append("创 60 日新高（+10）")
    if (f.get("amount20") or 0) < 1e8:
        s -= 20
        ev.append("20 日均额不足 1 亿，流动性撑不起龙头（-20）")
    return max(0, min(100, s)), "龙头相" if s >= 75 else "跟风", ev, ""


def _dim_sentiment(f, market_temp):
    s = int(round(market_temp)) if market_temp is not None else 60
    ev = ["市场情绪温度 %d（由涨跌家数 / 成交额合成）" % s]
    if s >= 80:
        ev.append("情绪过热，右侧信号胜率高但容易接最后一棒")
    elif s >= 55:
        ev.append("情绪中性偏暖，适合做右侧")
    else:
        ev.append("情绪偏冷，突破容易失败，降低仓位")
    return s, ("过热" if s >= 80 else ("偏暖" if s >= 55 else "偏冷")), ev, ""


def _dim_fib4(f):
    lo, hi = f.get("lo250"), f.get("hi250")
    ev = []
    if not lo or not hi or hi <= lo:
        return 50, "数据不足", ["缺 52 周区间，无法算斐波那契"], ""
    close = f["close"]
    retr = (hi - close) / (hi - lo) * 100.0
    ev.append("自 52 周高点回撤 %.1f%%（0.382/0.5/0.618 为关键位）" % retr)
    near = min(abs(retr - x) for x in (23.6, 38.2, 50.0, 61.8))
    if near <= 3:
        s = 85
        ev.append("正落在斐波那契关键位附近（±3%），支撑有效性强（+）")
    elif near <= 8:
        s = 65
    else:
        s = 50
    rows = f.get("rows") or []
    if len(rows) >= 2:
        o1, c1 = rows[-1][1], rows[-1][2]
        o0, c0 = rows[-2][1], rows[-2][2]
        if c0 > o0 and c1 < o1 and c1 < o0 and o1 > c0:
            ev.append("K 线形态：阴包阳（看跌吞没）")
            f["_bear_engulf"] = True
            s = min(s, 25)
    return s, ("关键位支撑" if s >= 80 else "非关键位"), ev, ""


def _dim_pankou(f):
    close, op, hi, lo = f["close"], f["open"], f["high"], f["low"]
    rng = max(hi - lo, 1e-9)
    strength = (close - lo) / rng * 100.0
    s = int(round(strength))
    ev = ["收盘位于当日区间 %.0f%% 处" % strength]
    if strength >= 80:
        ev.append("尾盘收在最高附近，买盘主动（+）")
    elif strength <= 25:
        ev.append("收在最低附近，卖压主导（-）")
    ev.append("当日振幅 %.2f%%" % (rng / (f.get("prev_close") or close) * 100))
    # 内外盘/五档不可得，用收盘强度与量比近似盘口，文档已注明
    return s, ("收盘强势" if s >= 70 else ("收盘弱势" if s <= 35 else "收盘中性")), ev, ""


def _dim_valuation(f):
    pe, pb = f.get("pe"), f.get("pb")
    ev = ["PE %s · PB %s" % (_r(pe, 1) if pe else "—", _r(pb, 2) if pb else "—")]
    if not pe or pe <= 0:
        ev.append("PE 缺失或为负（亏损），按中性处理")
        return 50, "亏损/缺失", ev, ""
    if pe <= 20:
        s, lv = 85, "低估"
    elif pe <= 40:
        s, lv = 70, "合理"
    elif pe <= 70:
        s, lv = 50, "偏高"
    else:
        s, lv = 30, "高估"
    ev.append("PE 位于 %s 区间" % lv)
    return s, lv, ev, ""


def checkup(f, snap=None, market_temp=None, chan_pre=None):
    """十二维体检 + 否决项 + 结论 + 计划。"""
    vp = volprice(f)
    ch = chan_pre if chan_pre is not None else chan(f["rows"])
    dims = []
    vals = {}

    fn = {
        "position": lambda: _dim_position(f),
        "trend": lambda: _dim_trend(f),
        "momentum": lambda: _dim_momentum(f),
        "volprice": lambda: _dim_volprice(f, vp),
        "chan": lambda: _dim_chan(f, ch),
        "mainwave": lambda: _dim_mainwave(f),
        "buildup": lambda: _dim_buildup(f),
        "dragon": lambda: _dim_dragon(f),
        "sentiment": lambda: _dim_sentiment(f, market_temp),
        "fib4": lambda: _dim_fib4(f),
        "pankou": lambda: _dim_pankou(f),
        "valuation": lambda: _dim_valuation(f),
    }
    vetoes = []
    for key, name, w in DIMS:
        res = fn[key]()
        if len(res) == 4:
            sc, lv, ev, adv = res
        else:
            sc, lv, ev = res
            adv = ""
        sc = max(0, min(100, int(round(sc))))
        vals[key] = sc
        dims.append({"key": key, "name": name, "weight": w, "score": sc,
                     "level": lv, "evidence": ev, "advice": adv})

    if f.get("_bear_engulf"):
        vetoes.append("斐波那契否决：K线形态反向（阴包阳（看跌吞没））")
    if ch and ch.get("bs") and "三卖" in ch["bs"]:
        vetoes.append("缠论反向：" + ch["bs"])
    if ch and ch.get("grade") == "risk" and ch.get("divergence") == "顶背驰":
        vetoes.append("缠论反向：顶背驰")

    dev20 = f.get("dev20") or 0
    level_key = level_of(dev20)
    plan = make_plan(f, level_key)
    strategy = round(sum(vals[k] * w for k, _, w in DIMS if k in STRATEGY_KEYS) / STRATEGY_W, 1)
    total = round(sum(vals[k] * w for k, _, w in DIMS), 1)
    liq = f.get("amount_today") or 0
    liq_ok = liq >= 5e7

    if dev20 > 15:
        verdict, note = "回避", "非右侧安全位（距 MA20 %.2f%%）" % dev20
    elif vetoes:
        verdict, note = "回避", "命中硬性否决：" + "；".join(vetoes)
    elif not liq_ok:
        verdict, note = "回避", "流动性不足（当日成交额 %.0f 万 < 5000 万）" % (liq / 1e4)
    elif level_key == "pos3" and strategy >= 75:
        verdict, note = "可介入", "位置3 安全位 + 策略适配分 %.1f ≥ 75" % strategy
    else:
        verdict, note = "观察", ("非右侧安全位（距 MA20 %.2f%%）" % dev20 if level_key != "pos3"
                              else "策略适配分 %.1f 未到 75" % strategy)

    weak = sorted(dims, key=lambda d: d["score"])[:3]
    return {
        "score": total,
        "checkup_score": total,
        "strategy_score": strategy,
        "verdict": verdict,
        "verdict_note": note,
        "vetoes": vetoes,
        "liq_ok": liq_ok,
        "liquidity": round(liq, 0),
        "liq_min": 5e7,
        "dims": dims,
        "weak": [{"key": d["key"], "name": d["name"], "score": d["score"]} for d in weak],
        "radar": [{"key": d["key"], "name": d["name"], "score": d["score"],
                   "max": 100, "weight": d["weight"]} for d in dims],
        "plan": plan,
        "volprice": {"vp8": vp["vp8"], "stage": vp["stage"], "advice": vp["advice"],
                     "counts": vp["counts"], "gate": vp["gate"]},
        "chan": ch,
        "mainwave": None,
        "level_key": level_key,
    }


# ----------------------------------------------------------------------------
# 策略层：市场情绪温度（体检「情绪周期」维度的输入）
# ----------------------------------------------------------------------------


def market_temp():
    snap = snapshot()
    rows = snap["rows"]
    up = sum(1 for r in rows if r["pct"] > 0)
    down = sum(1 for r in rows if r["pct"] < 0)
    total = max(1, up + down)
    breadth = up / float(total) * 100.0
    amount_yi = sum(r["amount"] for r in rows) / 1e8
    score = 50 + (breadth - 50) * 1.2
    if amount_yi > 15000:
        score += 8
    elif amount_yi < 7000:
        score -= 8
    return max(0, min(100, score)), {"up": up, "down": down, "flat": len(rows) - up - down,
                                     "breadth": breadth, "amount_yi": amount_yi}


# ----------------------------------------------------------------------------
# 策略层：强势股扫描
# ----------------------------------------------------------------------------

STRONG_COND = ("非ST · 沪深主板 · 总市值<200亿 · MA5>MA10>MA20多头排列 · 收盘>MA5 · "
               "MACD红柱 · 收盘>MA40>MA60上升通道")
STRONG_SOURCE = "新浪全市场快照 + 新浪日K复算（磁盘按交易日缓存，二次扫描 0 请求）"


def strong_filter(f, snap):
    name = (snap or {}).get("name") or f.get("name") or ""
    if is_st(name):
        return False
    code = f["code"]
    if code[:2] not in ("60", "00"):
        return False
    mv = (snap or {}).get("mv_yi")
    if not mv or mv >= 200:
        return False
    ma5, ma10, ma20 = f.get("ma5"), f.get("ma10"), f.get("ma20")
    ma40, ma60 = f.get("ma40"), f.get("ma60")
    if not all(v for v in (ma5, ma10, ma20, ma40, ma60)):
        return False
    close = f["close"]
    if not (ma5 > ma10 > ma20):
        return False
    if close <= ma5:
        return False
    if not (f.get("dif") is not None and f.get("dea") is not None and f["dif"] > f["dea"]
            and (f.get("hist") or 0) > 0):
        return False
    if not (close > ma40 > ma60):
        return False
    return True


def strong_row(f, snap, mat, ck):
    close = f["close"]
    ma5 = f.get("ma5") or close
    plan = make_plan(f, level_of(f.get("dev20")))
    reasons = list(mat["reasons"])
    reasons.append("满足全部强势股严格条件（多头排列 / 收盘>MA5 / MACD 红柱 / 收盘>MA40>MA60）")
    return {
        "code": f["code"],
        "name": (snap or {}).get("name") or f.get("name") or "",
        "industry": (snap or {}).get("industry") or "--",
        "price": _r(close),
        "pct": _r(f.get("pct")),
        "pct_today": _r(f.get("pct")),
        "mv_yi": _r((snap or {}).get("mv_yi"), 1),
        "score": mat["score"],
        "chan_score": (ck.get("chan") or {}).get("score"),
        "chan_grade": (ck.get("chan") or {}).get("grade"),
        "chan_walk": (ck.get("chan") or {}).get("walk"),
        "composite_score": round(mat["score"] * 0.6 + ((ck.get("chan") or {}).get("score") or 60) * 0.4, 1),
        "weak": mat["score"] < 60,
        "support": _r(f.get("ma10")),
        "resistance": _r(f.get("hi60")),
        "stop_loss": plan["stop_loss"],
        "stop_pct": plan["stop_pct"],
        "stop_basis": "止损幅度取 1.6×ATR14 与结构失效位（%.2f×0.995）中的更紧一侧，8%% 封顶、2.5%% 保底" %
                      (f.get("swing_low") or ma5),
        "dev20": _r(f.get("dev20")),
        "chg20": _r(f.get("chg20")),
        "pos52": _r(f.get("pos52"), 1),
        "vol_ratio": _r(f.get("vol_ratio")),
        "atr_pct": _r(f.get("atr_pct")),
        "dif": _r(f.get("dif"), 3),
        "dea": _r(f.get("dea"), 3),
        "ma5": _r(f.get("ma5")),
        "ma10": _r(f.get("ma10")),
        "ma20": _r(f.get("ma20")),
        "ma60": _r(f.get("ma60")),
        "reasons": reasons,
        "checkup": {
            "score": ck["score"], "strategy_score": ck["strategy_score"],
            "verdict": ck["verdict"], "verdict_note": ck["verdict_note"],
            "liq_ok": ck["liq_ok"], "liquidity": ck["liquidity"],
            "vetoes": len(ck["vetoes"]), "weak": ck["weak"], "dims": ck["dims"],
        },
    }


# ----------------------------------------------------------------------------
# 策略层：右侧六池
# ----------------------------------------------------------------------------


def pool_hit(f, pid):
    """六个池的命中判定（严格照抄报告里的条件阈值）。返回 (hit, reason, tags)"""
    close, op = f["close"], f["open"]
    ma5, ma10, ma20 = f.get("ma5"), f.get("ma10"), f.get("ma20")
    ma60, ma120 = f.get("ma60"), f.get("ma120")
    vr = f.get("vol_ratio") or 0
    tags = []
    if not (ma5 and ma10 and ma20):
        return False, "", []

    if pid == "breakout":
        if close < f["hi60"] * 0.999:
            return False, "", []
        if not (ma5 > ma10 > ma20 > (ma60 or 0)):
            return False, "", []
        if vr < 1.5 or close <= op:
            return False, "", []
        tags = ["四线多头排列", "创 60 日新高", "当日倍量收阳"]
        return True, "突破 60 日新高 · 四线多头排列 · 当日倍量收阳", tags

    if pid == "boll":
        up = f.get("boll_upper")
        if not up or vr < 2.0:
            return False, "", []
        if close <= up or close > up * 1.06:
            return False, "", []
        if close <= op:                      # MA5 向上 + 收阳，缺一不可
            return False, "", []
        tags = ["倍量突破布林上轨", "突破幅度≤6%"]
        return True, "倍量（量比%.1f）突破布林上轨，突破幅度 %.1f%%" % (vr, (close / up - 1) * 100), tags

    if pid == "yearline":
        n = len(f["rows"])
        if n < 260 or not f.get("ma250"):
            return False, "", []
        closes = [r[2] for r in f["rows"]]
        ma250 = sma(closes, 250)
        if ma250[-1] is None or ma250[-6] is None:
            return False, "", []
        above_now = closes[-1] > ma250[-1]
        if not above_now:
            return False, "", []
        if not any(closes[-i] < ma250[-i] for i in range(2, 7) if ma250[-i] is not None):
            return False, "", []
        if not (f.get("ma120") and f.get("ma120_prev") and f["ma120"] > f["ma120_prev"]):
            return False, "", []
        if ma5 <= ma20:
            return False, "", []
        tags = ["首次站上年线", "MA120 向上", "MA5>MA20"]
        return True, "近 5 日内首次站上年线 · MA120 向上 · MA5>MA20", tags

    if pid == "macd":
        dif, dea = f.get("dif"), f.get("dea")
        dp, sp = f.get("dif_prev"), f.get("dea_prev")
        if None in (dif, dea, dp, sp):
            return False, "", []
        if not (dp <= sp and dif > dea and dif > 0):
            return False, "", []
        if vr < 1.2:
            return False, "", []
        tags = ["MACD 零轴上金叉", "量比≥1.2"]
        return True, "MACD 零轴上方金叉（DIF %.3f 上穿 DEA %.3f）· 量比 %.2f" % (dif, dea, vr), tags

    if pid == "platform":
        rows = f.get("rows") or []
        if len(rows) < 22:
            return False, "", []
        # 平台取「不含当日」的前 20 根，否则当日突破本身会把振幅撑破 8%
        ph = max(r[3] for r in rows[-21:-1])
        pl = min(r[4] for r in rows[-21:-1])
        amp = (ph - pl) / pl * 100.0 if pl > 0 else 999
        if amp >= 8:
            return False, "", []
        if close < ph * 0.999:
            return False, "", []
        if vr < 1.5:
            return False, "", []
        tags = ["20 日窄幅平台（振幅%.1f%%）" % amp, "放量突破平台高点"]
        return True, "前 20 日振幅 %.1f%% 的窄幅平台 · 当日放量突破平台高点（%.2f）" % (amp, ph), tags

    if pid == "pullback":
        vma20 = f.get("vol_ma20") or 0
        if not vma20 or f["vol"] >= vma20 * 0.85:
            return False, "", []
        if not (f["low"] <= ma10 * 1.01 and close >= ma10 * 0.99):
            return False, "", []
        if not (ma5 > ma10 > ma20):
            return False, "", []
        if (f.get("chg20") or 0) <= 10:
            return False, "", []
        tags = ["缩量回踩 MA10 不破", "MA5>MA10>MA20", "20 日涨幅>10%"]
        return True, "缩量回踩 MA10 不破（当日量 / 20 日均量 %.2f）" % (f["vol"] / vma20), tags

    return False, "", []


POOL_EMPTY = {
    "breakout": "今日全市场 0 只命中 —— 没有出现「创 60 日新高 + 四线多头 + 倍量收阳」的价格结构确认，说明当前缺乏强共识突破。",
    "boll": "今日全市场 0 只命中 —— 没有「倍量突破布林上轨且幅度不超过 6%」的标的，动能突破条件未成立。",
    "yearline": "今日全市场 0 只命中 —— 近 5 日没有标的由空转多首次站上年线，中长期趋势未改善。",
    "macd": "今日全市场 0 只命中 —— 零轴上方没有新发生的 MACD 金叉，中期动量未确认。",
    "platform": "今日全市场 0 只命中 —— 没有「20 日振幅<8% 的窄幅平台被放量突破」的形态完成，箱体突破机会缺失。",
    "pullback": "今日全市场 0 只命中 —— 没有出现「缩量回踩 10 日线不破」的确认形态，意味当前不宜按回踩逻辑建仓，本身就是一条有效信号。",
}


def pool_stock(f, pid, pname, mat, ck, ch, vp, snap):
    plan = make_plan(f, level_of(f.get("dev20")))
    lv = level_of(f.get("dev20"))
    lname, lnote, _ = LEVELS[lv]
    ma = {k: _r(f.get(k)) for k in ("ma5", "ma10", "ma20", "ma40", "ma60", "ma120", "ma250")}
    hit, reason, tags = pool_hit(f, pid)
    st = {
        "s": ("up" if f.get("hh") and f.get("hl") else ("down" if not f.get("hh") and not f.get("hl") else "mixed")),
        "pos3_state": "横盘整理" if abs(f.get("chg20") or 0) < 10 else ("上攻中" if (f.get("chg20") or 0) > 0 else "回落中"),
        "state_note": "摆动结构 %s · MA20 斜率 %s%%" % (
            "同步抬高" if f.get("hh") and f.get("hl") else ("同步下移" if not f.get("hh") else "分歧"),
            _r(((f["ma20"] / f["ma20_prev"] - 1) * 100) if f.get("ma20_prev") else 0, 2)),
        "ma20_slope": _r(((f["ma20"] / f["ma20_prev"] - 1) * 100) if f.get("ma20_prev") else 0, 3),
        "strength": 0, "hh": bool(f.get("hh")), "hl": bool(f.get("hl")),
        "last_high": _r(f["sw_highs"][-1][1]) if f.get("sw_highs") else None,
        "prev_high": _r(f["sw_highs"][-2][1]) if f.get("sw_highs") and len(f["sw_highs"]) > 1 else None,
        "last_low": _r(f["sw_lows"][-1][1]) if f.get("sw_lows") else None,
        "prev_low": _r(f["sw_lows"][-2][1]) if f.get("sw_lows") and len(f["sw_lows"]) > 1 else None,
        "n_high": len(f.get("sw_highs") or []), "n_low": len(f.get("sw_lows") or []),
    }
    composite = round(mat["score"] * 0.6 + (ch["score"] if ch else 60) * 0.4, 1)
    return {
        "code": f["code"],
        "setcode": "1" if f["code"][:1] == "6" else "0",
        "secid": secid_of_code(f["code"]),
        "name": (snap or {}).get("name") or "",
        "price": _r(f["close"]),
        "pct": _r(f.get("pct")),
        "board": board_of(f["code"]),
        "industry": (snap or {}).get("industry") or "--",
        "tags": tags,
        "ma": ma,
        "dev20": _r(f.get("dev20")),
        "level": lname, "level_key": lv, "level_note": lnote,
        "score": mat["score"], "sub": mat["sub"],
        "pos52": _r(f.get("pos52")),
        "chg20": _r(f.get("chg20")), "chg60": _r(f.get("chg60")),
        "vol_ratio": _r(f.get("vol_ratio")),
        "dif": _r(f.get("dif"), 3), "dea": _r(f.get("dea"), 3),
        "hi60": _r(f.get("hi60")), "hi250": _r(f.get("hi250")),
        "risk": None, "risk_note": "",
        "plan": plan,
        "structure": st,
        "reasons": mat["reasons"],
        "ref_reason": reason,
        "pool_id": pid, "pool": pname,
        "chan_score": ch["score"] if ch else None,
        "chan_grade": ch["grade"] if ch else None,
        "chan_walk": ch["walk"] if ch else None,
        "chan_bs": ch["bs"] if ch else None,
        "composite_score": composite,
        "chan_missing": ch is None,
        "vp": vp,
        "gate": vp["gate"],
        "checkup": {"score": ck["score"], "strategy_score": ck["strategy_score"],
                    "verdict": ck["verdict"], "verdict_note": ck["verdict_note"],
                    "liq_ok": ck["liq_ok"], "liquidity": ck["liquidity"],
                    "vetoes": len(ck["vetoes"]), "weak": ck["weak"], "dims": ck["dims"]},
        "industry_hit": hit,
    }


def risk_of(f):
    """破位 / 走弱判定：命中即剔除出总榜与六池，进 risk_list。"""
    close = f["close"]
    ma20, ma60 = f.get("ma20"), f.get("ma60")
    if ma20 and close < ma20 * 0.97:
        return "破位", "收盘 %.2f 低于 MA20（%.2f）超 3%%，右侧结构已破" % (close, ma20)
    if ma60 and close < ma60 and (f.get("ma20") or 0) < (f.get("ma60") or 0):
        return "走弱", "收盘跌破 MA60（%.2f），中期趋势转弱" % ma60
    if (f.get("chg20") or 0) < -12:
        return "走弱", "近 20 日跌幅 %.1f%%，处于下跌通道" % (f.get("chg20") or 0)
    return None, ""


# ----------------------------------------------------------------------------
# 主力建仓雷达：筹码分布 + 建仓手法 + 六维确认
# ----------------------------------------------------------------------------

def chip_distribution(rows, bins=120):
    """筹码成本分布（陈浩《筹码分布》的简化实现）。

    做法：把近 250 根日 K 的成交量，按当根 (最低价, 最高价) 均匀摊到价格桶里，
    得到一条「成本-持仓量」分布曲线。这是公开数据能拿到的上限 —— 真正的筹码
    分布需要逐笔成交与股东数据，这里只用「量在什么价位成交过」做近似，
    所以结论看相对值（集中/分散、获利盘高低），不要当精确数值用。

    输出：
      concentration 90% 筹码区间宽度 / 中位价（%），越小越集中
      profit        获利盘占比（现价以下的筹码占比）
      cls/control   前端配色与文字：ok 疑似控盘 / mid 较集中 / no 分散
    """
    seg = rows[-250:]
    if len(seg) < 60:
        return None
    lo = min(r[4] for r in seg)
    hi = max(r[3] for r in seg)
    if hi <= lo:
        return None
    step = (hi - lo) / float(bins)
    dist = [0.0] * bins
    nseg = len(seg)
    # 换手衰减：陈浩筹码模型的核心 —— 越早的成交越可能已经换手离场，权重必须衰减。
    # 不做衰减时，250 日前的高位放量会一直压在分布里，把 90% 区间撑到 60%~120%，
    # 于是「有没有主力控盘」永远判成分散（实测 1252 只里 ok 只有 0 只）。
    # 这里用 60 日半衰期的指数衰减近似逐日换手（公开数据拿不到真实换手率序列）。
    for idx, r in enumerate(seg):
        v = r[5] or 0.0
        if v <= 0:
            continue
        days_ago = nseg - 1 - idx
        v *= 0.5 ** (days_ago / 60.0)
        i0 = int((r[4] - lo) / step)
        i1 = int((r[3] - lo) / step)
        i0 = max(0, min(bins - 1, i0))
        i1 = max(0, min(bins - 1, i1))
        if i1 <= i0:
            dist[i0] += v
        else:
            share = v / float(i1 - i0 + 1)
            for i in range(i0, i1 + 1):
                dist[i] += share
    tot = sum(dist)
    if tot <= 0:
        return None

    def q(p):
        acc = 0.0
        for i, d in enumerate(dist):
            acc += d
            if acc >= tot * p:
                return lo + step * (i + 0.5)
        return hi

    p5, p50, p95 = q(0.05), q(0.50), q(0.95)
    close = rows[-1][2]
    profit = sum(d for i, d in enumerate(dist)
                 if lo + step * (i + 0.5) <= close) / tot * 100.0
    width = ((p95 - p5) / p50 * 100.0) if p50 else None
    if width is None:
        cls, ctrl = "no", "分散"
    elif width <= 22 and profit >= 55:
        cls, ctrl = "ok", "疑似控盘"
    elif width <= 45:
        # 阈值放宽到 45%：A 股 250 日振幅普遍 50%~100%，用陈浩原文的 12%/22%
        # 硬套会让「较集中」几乎全空（实测只有 12% 命中）。ok 档保持严格不变 ——
        # 「疑似控盘」是要给建仓决策用的，宁可少判。
        cls, ctrl = "mid", "较集中"
    else:
        cls, ctrl = "no", "分散"
    return {
        "concentration": _r(width, 1), "profit": _r(profit, 1),
        "cls": cls, "control": ctrl,
        "p5": _r(p5), "p50": _r(p50), "p95": _r(p95),
        "detail": "90%% 筹码区间 %.2f ~ %.2f（宽度 %.1f%%）· 获利盘 %.1f%% · "
                  "口径：近 250 日成交量按价格摊布的近似分布" % (p5, p95, width or 0, profit),
    }


BUILDUP_MANNER = {
    "loud":  ("抢筹拉升", "bu-loud", "放量急拉，主力不怕暴露意图，抢筹码",
              "拉升是否延续、有没有放量滞涨", "量比≥2 且当日涨幅≥3%"),
    "press": ("打压吸筹", "bu-press", "阴线缩量下压，用时间换筹码",
              "下跌是否缩量、能否快速收回", "下跌且量比<1、位置在 52 周下半区"),
    "climb": ("缓升吸筹", "bu-climb", "小阳线连续推升，隐蔽吸筹",
              "MA20 是否持续上移、有无突然放量", "温和放量上涨、MA20 缓慢上移"),
    "side":  ("横盘吸筹", "bu-side", "箱体内磨，量能温和",
              "箱体上沿是否被放量突破", "20 日振幅<12%"),
}


def buildup_manner(f):
    """建仓手法四型（隐蔽吸筹 / 高调抢筹的细分）。"""
    pct = f.get("pct") or 0.0
    vr = f.get("vol_ratio") or 1.0
    pos52 = f.get("pos52")
    amp = f.get("amp20") or 0.0
    ma20, ma20p = f.get("ma20"), f.get("ma20_prev")
    slope = ((ma20 / ma20p - 1) * 100.0) if ma20 and ma20p else 0.0

    if vr >= 2.0 and pct >= 3.0:
        k = "loud"
    elif pct < 0 and vr < 1.0 and (pos52 is None or pos52 < 50):
        k = "press"
    elif pct > 0 and 1.0 <= vr < 1.8 and slope > 0:
        k = "climb"
    elif amp < 12:
        k = "side"
    else:
        return None
    name, cls, desc, watch, ev = BUILDUP_MANNER[k]
    return {"key": k, "name": name, "cls": cls, "desc": desc, "watch": watch,
            "evidence": ev + "（实际：量比 %.2f · 涨幅 %.2f%% · 20日振幅 %.1f%%）"
                             % (vr, pct, amp),
            "visible": "显性" if k in ("loud",) else "隐性"}


def buildup_one(f, rows):
    """主力建仓六维确认：位置 / 量能 / 均线 / 筹码 / 结构 / 流动性。

    打分沿用 _dim_buildup 的口径（50 基线 + 加减分），六维只做「满足几条」的
    可解释计数，两者不重复计权 —— 分数看总体，ok_cnt 看证据条数。
    """
    score, level, ev, _ = _dim_buildup(f)
    pos52 = f.get("pos52")
    vr = f.get("vol_ratio") or 1.0
    close, ma20, ma5, ma10 = f.get("close"), f.get("ma20"), f.get("ma5"), f.get("ma10")
    dev20 = f.get("dev20") or 0
    liq = f.get("amount20") or 0

    chip = chip_distribution(rows)
    # 筹码质量与位置深度参与打分：_dim_buildup 只看量能与 52 周位置，
    # 粗筛已经保证 pos52<45，于是所有候选都拿满基线分（实测满屏 95 分，无区分度）。
    # 这里补两档修正，让「同样在低位」的票能按筹码是否锁仓拉开差距。
    if chip:
        if chip["cls"] == "ok":
            score = min(100, score + 5)
        elif chip["cls"] == "no":
            score = max(0, score - 8)
    if pos52 is not None:
        if pos52 < 20:
            score = min(100, score + 3)
        elif pos52 > 38:
            score = max(0, score - 3)

    hits = []
    if pos52 is not None and pos52 < 45:
        hits.append("位置：52 周区间下半区（%.0f%%）" % pos52)
    if 1.0 <= vr <= 1.8:
        hits.append("量能：温和放大（量比 %.2f）" % vr)
    if close and ma20 and close > ma20 and dev20 < 8:
        hits.append("均线：站上 MA20 且未偏离（%+.1f%%）" % dev20)
    if ma5 and ma10 and ma5 > ma10:
        hits.append("均线：MA5 上穿 MA10，短期转强")
    if chip and chip["cls"] in ("ok", "mid"):
        hits.append("筹码：%s（宽度 %s%%）" % (chip["control"], chip["concentration"]))
    if liq >= 5e7:
        hits.append("流动性：20 日均额 %.1f 亿" % (liq / 1e8))

    ok_cnt = len(hits)
    stage = ("吸筹确认" if ok_cnt >= 5 else
             ("吸筹进行中" if ok_cnt >= 3 else "观察"))
    return {
        "score": score, "level_text": level, "ok_cnt": ok_cnt, "ok": ok_cnt >= 4,
        "stage": stage, "hits": hits, "evidence": ev,
        "chips": chip, "manner": buildup_manner(f),
    }


# ----------------------------------------------------------------------------
# 主升浪三类买点（A 突破当日 / B 回踩 5~10 日线 / C 回踩 20 日线）
# ----------------------------------------------------------------------------

MW_SHAPE = {
    "pit":   ("挖坑", "sh-pit", "突破前先挖一个坑，把浮筹洗出去再拉",
              "坑沿（挖坑前的高点）作为持有底线，不破就拿着", "跌破坑底即形态失败"),
    "amass": ("蓄势", "sh-amass", "长期横盘缩量，筹码充分换手后突破",
              "横盘区上沿转为支撑，回踩不破可加仓", "跌回横盘区内部即失败"),
    "runup": ("助跑", "sh-runup", "突破前已温和上涨一段，属于趋势中继",
              "MA20 是助跑线，沿线上移则持有", "放量滞涨或跌破 MA20"),
    "spout": ("井喷", "sh-spout", "突破后连续放量急拉，情绪主升段",
              "只做短线，冲高滞涨即减", "天量后次日不能延续即为顶部"),
}


def _mw_breakout(rows, f):
    """最近一次「收盘创 60 日新高」的日期与当日明细。返回 None 表示近 21 日没有突破。"""
    n = len(rows)
    highs = [r[3] for r in rows]
    closes = [r[2] for r in rows]
    vols = [r[5] for r in rows]
    for back in range(0, 21):
        i = n - 1 - back
        if i < 61:
            break
        prior = max(highs[i - 60:i])
        if prior <= 0:
            continue
        if closes[i] >= prior * 0.999:
            o, c, h, l = rows[i][1], rows[i][2], rows[i][3], rows[i][4]
            base = sum(vols[i - 5:i]) / 5.0 if i >= 5 else 0
            rng = max(h - l, 1e-6)
            prev_close = closes[i - 1] if i >= 1 else c
            return {
                "days_since": back,
                "date": rows[i][0],
                "gain": ((c / prev_close - 1) * 100.0) if prev_close else None,
                "vol_ratio": (vols[i] / base) if base > 0 else None,
                "upper_shadow": ((h - max(o, c)) / rng),
                "close": c,
                "prior_high": prior,
            }
    return None


def _mw_shape(rows, f, bidx):
    """四型形态（麻道明）：挖坑 / 蓄势 / 助跑 / 井喷。bidx 为突破日在 rows 中的下标。"""
    n = len(rows)
    if bidx is None or bidx < 25:
        # 突破日不明时按当前特征兜底
        amp = f.get("amp20") or 99
        chg20 = f.get("chg20") or 0
        if amp < 12:
            k = "amass"
        elif chg20 > 25:
            k = "spout"
        elif chg20 > 5:
            k = "runup"
        else:
            return None
        name, cls, desc, hold, risk = MW_SHAPE[k]
        return {"key": k, "name": name, "cls": cls, "desc": desc, "hold": hold, "risk": risk}

    pre = rows[max(0, bidx - 25):bidx]        # 突破前 25 根
    if not pre:
        return None
    ph = [r[3] for r in pre]
    pl = [r[4] for r in pre]
    pc = [r[2] for r in pre]
    pv = [r[5] for r in pre]
    amp = ((max(ph) - min(pl)) / min(pl) * 100.0) if min(pl) > 0 else 99
    chg = ((pc[-1] / pc[0] - 1) * 100.0) if pc and pc[0] else 0
    # 坑：突破前 25 日内出现过一次「跌破 MA60 后 5 日内收回」的快速下杀
    ma60s = sma([r[2] for r in rows], 60)
    pit = False
    for j in range(max(60, bidx - 25), bidx):
        if j < len(ma60s) and ma60s[j] and rows[j][4] < ma60s[j] * 0.95:
            pit = True
            break

    if pit:
        k = "pit"
    elif amp < 12:
        k = "amass"
    elif chg > 5:
        k = "runup"
    else:
        k = "amass"
    # 突破后是否连续放量急拉 → 井喷覆盖
    post = rows[bidx + 1:]
    if len(post) >= 2:
        ups = [r for r in post if r[2] > r[1]]
        if len(ups) >= 2 and (post[-1][2] / rows[bidx][2] - 1) * 100 > 12:
            k = "spout"
    name, cls, desc, hold, risk = MW_SHAPE[k]
    return {"key": k, "name": name, "cls": cls, "desc": desc, "hold": hold, "risk": risk}


def _mw_w5(f, chip, bidx, rows):
    """五维体检（张华 量-价-线-筹-盘口）：逐维给 ✓/✗ 与依据。"""
    out = []

    vr = f.get("vol_ratio") or 0
    out.append({"k": "量", "ok": vr >= 1.2,
                "note": "量比 %.2f（≥1.20 视为有效放量）" % vr})

    pct = f.get("pct") or 0
    o, c = f.get("open"), f.get("close")
    out.append({"k": "价", "ok": pct > 0 and (o is not None and c is not None and c > o),
                "note": "涨幅 %+.2f%% · 收%s" % (pct, "阳" if (o and c and c > o) else "阴")})

    ma5, ma10, ma20 = f.get("ma5"), f.get("ma10"), f.get("ma20")
    ok_line = bool(ma5 and ma10 and ma20 and ma5 > ma10 > ma20)
    out.append({"k": "线", "ok": ok_line,
                "note": "MA5 %.2f / MA10 %.2f / MA20 %.2f %s" % (
                    ma5 or 0, ma10 or 0, ma20 or 0, "多头排列" if ok_line else "未成多头")})

    conc = (chip or {}).get("concentration")
    out.append({"k": "筹", "ok": conc is not None and conc <= 45,
                "note": "90%% 筹码宽度 %s%%（≤45%% 视为锁定良好）" % (conc if conc is not None else "--")})

    amt = f.get("amount_today") or 0
    out.append({"k": "盘口", "ok": amt >= 1e8,
                "note": "当日成交额 %.2f 亿（≥1 亿可承载资金进出）" % (amt / 1e8)})
    return out


def mainwave_one(f, rows, chip=None):
    """单票主升浪买点判定。返回 (type, payload) 或 (None, None)。

    三类买点的差别只在「什么时候上车」，不在「是不是主升浪」：
      A 突破当日 —— 当日创 60 日新高 + 放量 + 收阳 + 上影不长
      B 回踩 5~10 日线 —— 突破后缩量回踩关键均线企稳
      C 回踩 20 日线 —— 错过前两档的补票位，容错更低
    """
    bo = _mw_breakout(rows, f)
    if not bo:
        return None, None

    days = bo["days_since"]
    close = f["close"]
    ma5, ma10, ma20 = f.get("ma5"), f.get("ma10"), f.get("ma20")
    vr_today = f.get("vol_ratio") or 1.0
    pct = f.get("pct") or 0
    o, h, l = f.get("open"), f.get("high"), f.get("low")
    rng = max((h - l) if (h is not None and l is not None) else 1e-6, 1e-6)
    us = ((h - max(o, close)) / rng) if (h is not None and o is not None) else 0

    def _near(ma, tol=0.02):
        return bool(ma and abs(close / ma - 1) <= tol)

    if days == 0 and vr_today >= 1.5 and pct >= 3 and us <= 0.40 \
            and f.get("close", 0) > (f.get("open") or 0):
        typ, tname = "A", "激进买点 · 突破当日"
        style = "尾盘确认站稳即可小仓介入，执行最快、止损最近"
    elif 1 <= days <= 10 and (_near(ma5, 0.02) or _near(ma10, 0.02)):
        typ, tname = "B", "稳健主仓 · 回踩 5~10 日线"
        style = "主仓首选：突破后缩量回踩关键均线企稳"
    elif 1 <= days <= 20 and _near(ma20, 0.025) and (not ma20 or close >= ma20 * 0.985):
        typ, tname = "C", "末班车 · 回踩 20 日线"
        style = "错过前两档后的补票位，容错更低、需更严止损"
    else:
        return None, None

    # B/C 必须缩量回踩，放量下杀不算买点
    if typ in ("B", "C") and vr_today > 1.5 and pct < -2:
        return None, None

    shape = _mw_shape(rows, f, (len(rows) - 1 - days) if days is not None else None)
    # 筹码只在判定通过后才算 —— 全市场扫描时命中率很低，先算等于白烧 90% 的 CPU
    if chip is None:
        chip = chip_distribution(rows)
    w5 = _mw_w5(f, chip, days, rows)
    w5_ok = sum(1 for x in w5 if x["ok"])

    # 买点分：形态基础 55 + 五维各 6 + 突破质量 15
    score = 55 + w5_ok * 6
    if bo.get("vol_ratio") and bo["vol_ratio"] >= 2:
        score += 6
    if bo.get("upper_shadow") is not None and bo["upper_shadow"] <= 0.2:
        score += 5
    if typ == "A":
        score += 4
    score = max(0, min(100, int(round(score))))

    gap = None
    if len(rows) >= 2:
        prev_h = rows[-2][3]
        if l is not None and prev_h and l > prev_h:
            filled = any(rows[k][4] <= prev_h for k in range(len(rows) - 1, len(rows)))
            gap = {"up": True, "filled": False,
                   "note": "今日最低 %.2f > 昨日最高 %.2f，向上跳空且当日未回补" % (l, prev_h)}

    plan = make_plan(f, level_of(f.get("dev20")))
    amp20 = None
    pre = rows[max(0, len(rows) - 21):]
    if pre:
        ph = max(r[3] for r in pre)
        pl = min(r[4] for r in pre)
        amp20 = round((ph - pl) / pl * 100, 1) if pl else None

    reasons = []
    reasons.append("最近一次创 60 日新高：%s（%s）" % (bo.get("date"), "今日" if days == 0 else "%d 日前" % days))
    if bo.get("gain") is not None:
        reasons.append("突破日涨幅 %+.2f%% · 量比 %.2f · 上影占比 %.0f%%"
                       % (bo["gain"], bo.get("vol_ratio") or 0, (bo.get("upper_shadow") or 0) * 100))
    reasons.append("距 MA20 %+.2f%% · 60 日涨幅 %+.1f%%"
                   % (f.get("dev20") or 0, f.get("chg60") or 0))
    if shape:
        reasons.append("形态判定：%s —— %s" % (shape["name"], shape["desc"]))

    payload = {
        "type": typ, "type_name": tname, "style": style,
        "code": f["code"], "secid": secid_of_code(f["code"]),
        "name": (f.get("name") or ""), "price": _r(close), "pct": _r(pct),
        "board": board_of(f["code"]), "industry": f.get("industry") or "--",
        "score": score, "w5": w5, "w5_ok": w5_ok,
        "shape": shape, "gap": gap,
        "breakout": {
            "days_since": days, "date": bo.get("date"),
            "gain": _r(bo.get("gain")), "vol_ratio": _r(bo.get("vol_ratio")),
            "upper_shadow": _r((bo.get("upper_shadow") or 0) * 100),
        },
        "wash": {"amp20": amp20},
        "plan": plan,
        "reasons": reasons,
        "risk_note": ("距 MA20 已 %+.1f%%，追高需减仓" % (f.get("dev20") or 0)
                      if (f.get("dev20") or 0) > 8 else ""),
        "advice": plan.get("buy_note") or "",
        "discipline": ({"shape_hold": shape["hold"], "shape_risk": shape["risk"]}
                       if shape else {}),
        "checkup": None,
    }
    return typ, payload


# ----------------------------------------------------------------------------
# 斐波那契四度（魏强斌《高抛低吸》方法论量化）
# ----------------------------------------------------------------------------

FIB_LEVELS = (0.236, 0.382, 0.500, 0.618, 0.786)


def fib4_one(f, rows):
    """四度体检：位（回撤位置）/ 态（K 线形态）/ 量（量能配合）/ 动（动量方向）。

    四度各自 0~100，按 30/20/25/25 加权成总分。
    设计取舍：四度里「位」权重最高，因为斐波那契方法的核心假设就是
    「价格在关键比例位获得支撑/压力」，位置不对，其余三度再好也是逆势。
    """
    close = f["close"]
    lo, hi = f.get("lo250"), f.get("hi250")
    ev_zone = {"name": "--", "desc": "缺少 52 周区间"}
    wei = {"name": "位 · 回撤", "max": 100, "score": 50, "desc": "数据不足"}

    if lo and hi and hi > lo:
        span = hi - lo
        retr = (hi - close) / span
        near = min(FIB_LEVELS, key=lambda x: abs(retr - x))
        dist = abs(retr - near) * 100
        if dist <= 2:
            ws = 90
        elif dist <= 5:
            ws = 70
        else:
            ws = 45
        wei = {"name": "位 · 回撤", "max": 100, "score": ws,
               "desc": "自 52 周高点回撤 %.1f%%，最接近 %.3f 位（偏离 %.1f%%）"
                       % (retr * 100, near, dist)}
        zone_names = [(0.236, "浅回撤区（强势整理）"), (0.382, "黄金分割第一支撑"),
                      (0.500, "半分位（多空分界）"), (0.618, "黄金分割深支撑"),
                      (0.786, "极限回撤区（趋势存疑）")]
        zname = "深度回撤区"
        for r_, nm in zone_names:
            if retr <= r_:
                zname = nm
                break
        ev_zone = {"name": zname,
                   "desc": "当前回撤 %.1f%%，处于 %s" % (retr * 100, zname)}

    # 态：近 3 根 K 线的看涨/看跌形态
    tai_s, tai_d = 50, "近 3 日无明确 K 线形态"
    if len(rows) >= 3:
        a, b2, c = rows[-3], rows[-2], rows[-1]
        bull_engulf = (a[2] < a[1] and c[2] > c[1] and c[2] >= a[1] and c[1] <= a[2])
        hammer = ((min(c[1], c[2]) - c[4]) > abs(c[2] - c[1]) * 1.8 and c[2] >= c[1])
        doji = abs(c[2] - c[1]) <= (c[3] - c[4]) * 0.15 if c[3] > c[4] else False
        if bull_engulf:
            tai_s, tai_d = 85, "出现看涨吞没（阳包阴），短线反转信号"
        elif hammer:
            tai_s, tai_d = 78, "长下影阳线，下方有接盘"
        elif doji:
            tai_s, tai_d = 62, "十字星，多空均衡待选择方向"
        elif c[2] > c[1] and c[2] > b2[2]:
            tai_s, tai_d = 70, "连续收阳，短线动能延续"
        elif c[2] < c[1] and c[2] < b2[2]:
            tai_s, tai_d = 35, "连续收阴，短线承压"

    # 量：量能配合
    vr = f.get("vol_ratio") or 1.0
    pct = f.get("pct") or 0.0
    if pct > 0 and vr >= 1.2:
        li_s, li_d = 80, "放量上涨（量比 %.2f），量价配合" % vr
    elif pct > 0 and vr < 0.9:
        li_s, li_d = 62, "缩量上涨（量比 %.2f），筹码锁定但需防后继乏力" % vr
    elif pct < 0 and vr < 0.9:
        li_s, li_d = 75, "缩量回调（量比 %.2f），抛压不重" % vr
    elif pct < 0 and vr >= 1.8:
        li_s, li_d = 25, "放量下跌（量比 %.2f），主动抛压释放" % vr
    else:
        li_s, li_d = 55, "量能平淡（量比 %.2f）" % vr

    # 动：MACD 与均线方向
    dif, dea, hist = f.get("dif"), f.get("dea"), f.get("hist")
    ma5, ma20 = f.get("ma5"), f.get("ma20")
    if dif is not None and dea is not None:
        if dif > dea and dif > 0:
            do_s, do_d = 82, "MACD 零轴上方金叉延续，中期动量向上"
        elif dif > dea:
            do_s, do_d = 70, "MACD 金叉但仍在零轴下方，属反弹级别"
        elif dif < dea and dif < 0:
            do_s, do_d = 28, "MACD 零轴下方死叉，动量向下"
        else:
            do_s, do_d = 45, "MACD 高位死叉，动量转弱"
    else:
        do_s, do_d = 50, "缺少 MACD 数据"
    if ma5 and ma20 and ma5 > ma20 and do_s < 60:
        do_s += 8
        do_d += "（均线仍多头，动量扣分收敛）"

    four = {
        "wei": wei,
        "tai": {"name": "态 · 形态", "max": 100, "score": tai_s, "desc": tai_d},
        "liang": {"name": "量 · 量能", "max": 100, "score": li_s, "desc": li_d},
        "dong": {"name": "动 · 动量", "max": 100, "score": do_s, "desc": do_d},
    }
    score = int(round(ws * 0.30 + tai_s * 0.20 + li_s * 0.25 + do_s * 0.25))

    hits = sum(1 for k in ("wei", "tai", "liang", "dong") if four[k]["score"] >= 70)
    if hits >= 4 and score >= 70:
        grade, gtext = "resonance", "四度共振（位置、形态、量能、动量同时成立）"
    elif hits >= 3:
        grade, gtext = "triple", "三度成立，等第四度补上再动手"
    elif do_s < 40 and li_s < 40:
        grade, gtext = "veto", "量与动量同时走坏，四度否决"
    else:
        grade, gtext = "watch", "四度不齐，暂不具备介入条件"

    # 陷阱识别
    traps = []
    if f.get("close") and f.get("hi60") and close >= f.get("hi60") * 0.999 and vr < 1.0:
        traps.append({"level": "warn", "name": "缩量新高",
                      "desc": "创新高但量能不足，突破缺乏资金确认"})
    if vr > 2.5 and abs(pct) < 1.5:
        traps.append({"level": "warn", "name": "天量滞涨",
                      "desc": "天量而价格不动，警惕出货"})
    if ma20 and close < ma20 * 0.97:
        traps.append({"level": "warn", "name": "跌破 MA20",
                      "desc": "已偏离 20 日线 -%.1f%%，右侧结构受损" % ((close / ma20 - 1) * 100)})
    if not traps:
        traps.append({"level": "ok", "name": "无明显陷阱",
                      "desc": "未检出缩量新高、天量滞涨或破位"})

    # 介入计划（按斐波那契位给区间）
    plan = {}
    if lo and hi and hi > lo:
        buy_lo = lo + (hi - lo) * 0.50
        buy_hi = lo + (hi - lo) * 0.618
        plan = {
            "buy_lo": _r(buy_lo), "buy_hi": _r(buy_hi),
            "stop": _r(buy_lo * 0.97),
            "stop_pct": _r(-3.0),
            "t1": _r(hi - (hi - lo) * 0.236),
            "t2": _r(hi),
            "side_text": ("向上（现价在 0.618 位上方）" if close > buy_hi
                          else ("区间内" if close >= buy_lo else "向下（已破 0.5 位）")),
        }

    return {
        "ok": True, "code": f["code"], "secid": secid_of_code(f["code"]),
        "name": f.get("name") or "",
        "price": _r(close), "pct": _r(pct), "trade_date": f["date"],
        "score": score, "score_max": 100,
        "grade": grade, "grade_text": gtext,
        "four": four, "zone": ev_zone, "traps": traps, "plan": plan,
        "rows": [{"date": r[0], "open": r[1], "close": r[2], "high": r[3],
                  "low": r[4], "vol": r[5]} for r in rows[-120:]],
        "note": "四度加权：位 30% + 态 20% + 量 25% + 动 25%；"
                "斐波那契位取自 52 周高低区间，介入区默认取 0.5~0.618 回撤带。",
        "disclaimer": "斐波那契比例是经验规律而非因果律，仅供研究参考，不构成投资建议。",
    }


# ----------------------------------------------------------------------------
# 持仓体检（卖出侧 · 达瓦斯箱体 + 江恩时间止损 / 50% 回撤法则）
#
# 与买入侧的关系：本模块不参与任何打分，也不改变任何既有结论 ——
# 它只回答「已经持有的怎么走」。字段与 index.html 的 phCard() 严格一一对应。
# ----------------------------------------------------------------------------

PH_TIME_BUDGET = 20      # 江恩时间止损预算（交易日）
PH_WEAK_RETRACE = 61.8   # 回撤超过此值视为结构受损
PH_BOX_KEEP = 6          # 箱体历史保留最近 6 个（与原站 /api/position_health 的 history 长度一致）


def _ph_boxes(rows, sh, sl):
    """达瓦斯箱体：把摆动低点→摆动高点配对成箱体，返回全部箱体（时间正序）。

    配对规则：按时间顺序扫，遇到低点记为箱底，遇到高点且已有箱底则闭合成一箱。
    这样得到的是「一次上攻所跨越的区间」，正是达瓦斯说的"箱体"。
    返回全部而非只返回最近几个 —— 前端要显示「第 N 箱」，N 是历史箱体的真实序号，
    只取最近 3 箱会让所有票都显示成"第 3 箱"。
    """
    pts = sorted([(i, p, "lo") for i, p in sl] + [(i, p, "hi") for i, p in sh],
                 key=lambda x: x[0])
    boxes, cur = [], None
    for i, p, t in pts:
        if t == "lo":
            cur = (i, p)
        elif t == "hi" and cur and i > cur[0]:
            boxes.append({"lo": cur[1], "hi": p, "from": cur[0], "to": i})
            cur = None
    return boxes


def _ph_box_hist(rows, boxes, keep=PH_BOX_KEEP):
    """箱体历史（时间正序，最近 keep 个）。

    ★ 为什么要把 history 单列出来，而不是让前端从 box 自己推：
      ago 是「下沿 / 上沿各自距今天多少根 K 线」。没有这两个数字，
      就看不出这个箱体是刚抬高的还是老箱体 —— 下沿 26 根前定的、上沿
      13 根前定的，和上下沿都是 60 根前定的，含义完全不同。
      字段名与含义对齐原站：lo / hi / mid / lo_ago / hi_ago。
    """
    n = len(rows)
    out = []
    for b in boxes[-keep:]:
        lo, hi = float(b["lo"]), float(b["hi"])
        out.append({
            "lo": _r(lo), "hi": _r(hi), "mid": _r((lo + hi) / 2.0),
            "lo_ago": int(n - 1 - b["from"]), "hi_ago": int(n - 1 - b["to"]),
        })
    return out


def position_health_one(f, rows, name=""):
    """单票持仓体检。字段形状与原站 /api/position_health 的 items[] 逐一对齐。

    契约（原站实测，全部字段都在前端/导出里被读过，缺一个就是半套数据）：
      ok / code / name / price / bars / history / box / gann / time /
      advice / advice_level / computed_at / secid
    """
    close = f["close"]
    sh, sl = swing_points(rows, 3)

    # ---- 达瓦斯箱体 ----
    box = None
    all_boxes = _ph_boxes(rows, sh, sl)
    hist = _ph_box_hist(rows, all_boxes)
    recent = all_boxes[-3:]        # 移动止损只看最近 3 箱
    if recent:
        cur = recent[-1]
        lo, hi = float(cur["lo"]), float(cur["hi"])
        mid = (lo + hi) / 2.0
        if close < lo:
            state = "跌破箱体下沿"
        elif close > hi:
            state = "突破箱体上沿"
        else:
            state = "箱体内震荡"
        trail = max(float(b["lo"]) for b in recent)
        # state_note 文案直接对齐原站三种情形（自选盯盘页会去掉 ** 标记当纯文本用）
        note_map = {
            "跌破箱体下沿": "价格已跌穿最近箱体下沿，**移动止损触发** —— 按规则离场",
            "突破箱体上沿": "价格已站上最近箱体上沿，按达瓦斯规则属**持有/加仓**区",
            "箱体内震荡": "价格仍在最近箱体内，按规则**持有**，跌破下沿才走",
        }
        # 箱体是否「逐级抬高」看**中轴**，不看下沿：
        # 下沿不动、上沿抬高也是健康的箱体上移（收敛型整理），只盯下沿会漏掉。
        prev_mid = None
        if len(recent) >= 2:
            prev_mid = (float(recent[-2]["lo"]) + float(recent[-2]["hi"])) / 2.0
        rising = bool(prev_mid is not None and mid > prev_mid * 1.001)
        rising_note = ("箱体逐级抬高 —— 趋势延续，继续持有" if rising
                       else "箱体中轴未持续抬高 —— 趋势转平，降低预期")
        box = {
            "lo": _r(lo), "hi": _r(hi),
            "lo_ago": int(len(rows) - 1 - cur["from"]),
            "hi_ago": int(len(rows) - 1 - cur["to"]),
            "level": len(hist),                # 前端显示「第 N 箱」：最近 keep 个里的第几个
            "total": len(all_boxes),           # 历史箱体总数（原站不给，留着自己可核查）
            "state": state, "state_note": note_map[state],
            "rising": rising, "rising_note": rising_note,
            "trail_stop": _r(trail),
            "trail_pct": _r((close / trail - 1) * 100.0) if trail else None,
            "width_pct": _r((hi - lo) / lo * 100.0) if lo else None,
        }

    # ---- 50% 回撤法则 / 江恩回撤 ----
    gann = None
    if sl:
        # 最近一条有效上涨腿：从最近摆动低点起，到其后的最高价。
        # 注意不能只取「其后的摆动高点」—— 低点之后往往还没形成新的分型高点，
        # 那样 gann 会长期为 null，50% 回撤法则等于没接（实测 3 只票全空）。
        low_pt = sl[-1]
        after_hi = [r[3] for r in rows[low_pt[0] + 1:]]
        leg_hi = max(after_hi) if after_hi else close
        leg_lo = low_pt[1]
        span = leg_hi - leg_lo
        if span > 0:
            retr = (leg_hi - close) / span * 100.0
            mid50 = leg_hi - span * 0.5
            if close >= leg_hi * 1.02:
                lvl, weak = "已创新高（上涨腿延续）", False
                note = ("上涨腿 %.3f → %.3f，现价已超过腿顶，该腿仍在延续；50%% 位置 %.3f"
                        % (leg_lo, leg_hi, mid50))
            elif retr <= 38.2:
                lvl, weak = "强势（回撤 < 38.2%）", False
                note = ("上涨腿 %.3f → %.3f，现价回撤 %.1f%%；50%% 位置 %.3f"
                        % (leg_lo, leg_hi, retr, mid50))
            elif retr <= PH_WEAK_RETRACE:
                lvl, weak = "中性（回撤 38.2%%~61.8%%）", False
                note = ("上涨腿 %.3f → %.3f，现价回撤 %.1f%%，仍在 61.8%% 以内；50%% 位置 %.3f"
                        % (leg_lo, leg_hi, retr, mid50))
            elif retr <= 100:
                lvl, weak = "结构受损（> 61.8%）", True
                note = "上涨腿 %.3f → %.3f，现价回撤 %.1f%%；50%% 位置 %.3f" % (
                    leg_lo, leg_hi, retr, mid50)
            else:
                lvl, weak = "完全回吐（已跌破上涨腿起点）", True
                note = ("上涨腿 %.3f → %.3f，现价已跌破腿起点 —— "
                        "该上涨波段被完全回吐，结构作废" % (leg_lo, leg_hi))
            gann = {"leg_lo": _r(leg_lo), "leg_hi": _r(leg_hi),
                    "retr_pct": _r(retr), "level": lvl, "weak": weak, "note": note}

    # ---- 江恩时间止损 ----
    tm = None
    if sl:
        low_pt = sl[-1]
        since = len(rows) - 1 - low_pt[0]
        moved = close > low_pt[1] * 1.05
        stop = bool(since > PH_TIME_BUDGET and not moved)
        if stop:
            tnote = ("距最近摆动低点已 %d 个交易日（预算 %d），且未接近期间高点 —— "
                     "**时间止损：横盘超期，资金沉没，按纪律离场**" % (since, PH_TIME_BUDGET))
        elif moved:
            tnote = ("距最近摆动低点 %d 个交易日（预算 %d），已走出（期间高点附近）"
                     % (since, PH_TIME_BUDGET))
        else:
            tnote = ("距最近摆动低点 %d 个交易日（预算 %d），仍在预算内"
                     % (since, PH_TIME_BUDGET))
        tm = {
            "since_low": int(since), "low_price": _r(low_pt[1]),
            "budget": PH_TIME_BUDGET, "moved": bool(moved), "stop": stop,
            "note": tnote, "low_date": rows[low_pt[0]][0],
        }

    # ---- 结论 ----
    # 优先级：跌破箱体下沿 > 跌破移动止损线 > 时间止损 > 回撤受损 > 突破上沿 > 箱内持有。
    # 「跌破移动止损线」必须排在回撤判断之前：移动止损是达瓦斯的实操核心，
    # 现价 11.54 已低于止损线 11.65 却因为回撤只有 32.8% 而判成「结构健康、持有」，
    # 是明显的误判（平安银行 2026-11-02 就是这样）。
    if box and box["state"] == "跌破箱体下沿":
        advice, level = ("移动止损触发：跌破箱体下沿 %.3f，按达瓦斯规则离场" % box["lo"], "warn")
    elif box and box.get("trail_pct") is not None and box["trail_pct"] < 0:
        advice, level = ("现价已跌破移动止损线 %.3f（%.2f%%）→ 按纪律离场，止损线只上移不下移"
                         % (box["trail_stop"], box["trail_pct"]), "warn")
    elif tm and tm["stop"]:
        advice, level = ("距摆动低点已 %d 个交易日仍未走出（预算 %d）→ 时间止损，换仓"
                         % (tm["since_low"], tm["budget"]), "warn")
    elif gann and gann["weak"]:
        advice, level = "回撤已超 61.8%，结构受损 → 减仓至少一半", "warn"
    elif box and box["state"] == "突破箱体上沿":
        advice, level = ("突破箱体上沿 %.3f，持有并把止损上移到 %.3f"
                         % (box["hi"], box["trail_stop"]), "ok")
    elif box:
        advice, level = ("箱体内持有，止损线 %.3f（跌破才走）" % box["trail_stop"], "mid")
    else:
        advice, level = "K 线不足，无法定位箱体 → 暂不产生卖出信号", ""

    return {
        "ok": True,
        "code": f["code"], "name": name or f.get("name") or "",
        "price": _r(close), "pct": _r(f.get("pct")),
        "bars": len(rows),
        "history": hist,
        "box": box, "gann": gann, "time": tm,
        "advice": advice, "advice_level": level,
        "computed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "secid": secid_of_code(f["code"]),
        "trade_date": f["date"],
    }


# ----------------------------------------------------------------------------
# 腾飞点（七条 + 评分闸门）
# ----------------------------------------------------------------------------

TF_MIN_SCORE = 55


def taofei_one(f, rows, strict=True):
    """腾飞点七条判定。返回 dict 或 None。

    七条（与前端口径「MA30/60双线、放量中大阳、上影线、量能倍数」对齐）：
      ① MA30 > MA60（中期双线向上）
      ② 收盘同时站上 MA30 与 MA60
      ③ 当日涨幅 ≥ 5%（中大阳）
      ④ 量比 ≥ 2（放量）
      ⑤ 上影线 ≤ 30%（收盘强势、没被砸回）
      ⑥ 收盘 ≥ 近 60 日最高 × 0.98（贴近突破）
      ⑦ DIF > 0 且 DIF > DEA（动量在零轴上方）
    """
    close = f["close"]
    o, h, l = f.get("open"), f.get("high"), f.get("low")
    if not (o and h and l):
        return None
    closes = [r[2] for r in rows]
    ma30_arr = sma(closes, 30)
    ma30 = ma30_arr[-1] if len(ma30_arr) >= 30 else None
    ma60 = f.get("ma60")
    ma10 = f.get("ma10")
    ma20 = f.get("ma20")
    pct = f.get("pct") or 0.0
    vr = f.get("vol_ratio") or 0.0
    rng = max(h - l, 1e-6)
    us = (h - max(o, close)) / rng
    dif, dea = f.get("dif"), f.get("dea")
    hi60 = f.get("hi60")

    rules = []
    rules.append(("双线", bool(ma30 and ma60 and ma30 > ma60),
                  "MA30 %.2f %s MA60 %.2f" % (ma30 or 0, ">" if (ma30 and ma60 and ma30 > ma60) else "≤", ma60 or 0)))
    rules.append(("站稳", bool(ma30 and ma60 and close > ma30 and close > ma60),
                  "收盘 %.2f 站上双线" % close))
    rules.append(("中大阳", pct >= 5.0, "当日涨幅 %+.2f%% · 门槛 5%%" % pct))
    rules.append(("放量", vr >= 2.0, "量比 %.2f（≥2.0）" % vr))
    rules.append(("上影", us <= 0.30, "上影占振幅 %.0f%%（≤30%%）" % (us * 100)))
    rules.append(("近高", bool(hi60 and close >= hi60 * 0.98),
                  "收盘距 60 日高点 %.1f%%" % (((close / hi60) - 1) * 100 if hi60 else 0)))
    rules.append(("动量", bool(dif is not None and dea is not None and dif > 0 and dif > dea),
                  "DIF %.3f / DEA %.3f %s" % (dif or 0, dea or 0,
                                              "零轴上方多头" if (dif and dea and dif > 0 and dif > dea) else "未确认")))

    hits = sum(1 for _, ok, _ in rules if ok)
    if strict and hits < 5:
        return None

    score = hits * 12
    if pct >= 7:
        score += 5
    if vr >= 2.5:
        score += 3
    score = max(0, min(100, score))
    if strict and score < TF_MIN_SCORE:
        return None

    # 量价闸门：有警示则扣分（与「默认只看量价确认」的语义一致）
    vp = volprice(f)
    score_final = score
    if vp["gate"] == "block":
        score_final = max(0, score - 15)
    elif vp["gate"] == "pass":
        score_final = min(100, score + 3)

    chg20 = f.get("chg20") or 0
    typ = "起涨型" if chg20 < 15 else "加速型"
    mw_type = ""
    if dif is not None and dea is not None and dif > 0 and us <= 0.35:
        mw_type = "主升确认"

    # 止损：以 MA10 操盘线为准（文档口径），信号日最低价作为下限兜底。
    # 只用「信号日最低 ×0.995」会在大振幅票上给出 -10% 以上的止损距离，
    # 失去可操作性；MA10 更紧且符合「打穿操盘线 3 日不收复即减仓」的纪律。
    stop = round(l * 0.995, 2)
    if ma10 and ma10 * 0.995 > stop and ma10 * 0.995 < close:
        stop = round(ma10 * 0.995, 2)
    # 风险上限：止损距离不超过 -8%。大阳线的当日最低往往远离收盘（青岛食品
    # 实测 -12.9%），止损放那么远会让「按单笔 2% 风险倒推」的仓位小到没有意义。
    cap = round(close * 0.92, 2)
    if cap > stop and cap < close:
        stop = cap
    # 下限：止损至少离现价 -2%，否则 MA10 贴近收盘时会给出 -0.4% 的止损，
    # 一个波动就被扫掉，等于没止损。
    tight = round(close * 0.98, 2)
    if stop > tight:
        stop = tight
    stop = round(min(stop, round(close * 0.999, 2)), 2)
    plan = {
        "defense": {"price": _r(ma20) if ma20 else None,
                    "note": "MA20 为防守位，跌破则结构转弱"},
    }
    return {
        "code": f["code"], "secid": secid_of_code(f["code"]),
        "name": f.get("name") or "", "industry": f.get("industry") or "--",
        "price": _r(close), "pct": _r(pct),
        "score": score, "score_final": score_final,
        "type": typ, "mw_type": mw_type,
        "signal_date": f["date"], "signal_pct": _r(pct), "signal_pct_chg": 0.0,
        "vol_x": _r(vr), "stop": stop,
        "stop_pct": _r((stop / close - 1) * 100.0),
        "plan": plan,
        "rules": [{"k": k, "ok": ok, "note": n} for k, ok, n in rules],
        "hits": hits,
        # 前端 taofeicheck 用 ok_cnt 显示「满足 N/7 条」（不是 hits，字段名别改）
        "ok_cnt": hits,
        "elapsed": 0,
        # 以下为单票复核面板用的展示字段（前端 r.ma / r.pos / r.shadow_pct 等）
        "date": f["date"], "ksource": "sina",
        "conditions": "① MA30>MA60 ② 收盘站上双线 ③ 涨幅≥5% ④ 量比≥2 "
                      "⑤ 上影≤30% ⑥ 收盘≥60日高点×0.98 ⑦ DIF>0且DIF>DEA",
        "ma": {"ma5": _r(f.get("ma5")), "ma10": _r(ma10), "ma20": _r(ma20),
               "ma30": _r(ma30), "ma60": _r(ma60)},
        "pos": _r(f.get("pos52"), 1),
        "shadow_pct": _r(us * 100, 1),
        "gate_text": {"pass": "量价确认", "block": "量价警示"}.get(vp["gate"], "量价中性"),
        "vp": {"gate": vp["gate"], "counts": vp["counts"]},
        "gate": vp["gate"],
        "checkup": None,
    }


# ----------------------------------------------------------------------------
# 龙头气质（四维 + 退潮判定） / 超短池
# ----------------------------------------------------------------------------

def dragon_one(f, rows):
    """龙头四维：带动性 / 封板质量 / 形态位置 / 量能资金（各 0-100，均权合计）。

    说明：原站「12 维体检回避」列在龙头榜上是结构性冲突 —— 龙头天然高偏离、
    价新高量缩，必然被判回避。所以本函数不做体检结论，只给气质分与退潮状态。
    """
    close = f["close"]
    pct = f.get("pct") or 0.0
    chg20 = f.get("chg20") or 0.0
    pos52 = f.get("pos52")
    dev20 = f.get("dev20") or 0.0
    vr = f.get("vol_ratio") or 1.0
    amt = f.get("amount20") or 0.0
    ma5, ma10, ma20 = f.get("ma5"), f.get("ma10"), f.get("ma20")
    o, h, l = f.get("open"), f.get("high"), f.get("low")
    rng = max((h - l) if (h is not None and l is not None) else 1e-6, 1e-6)
    us = ((h - max(o, close)) / rng) if (h is not None and o is not None) else 0.0

    # ① 带动性：阶段涨幅 + 流动性（能不能带动板块）
    d = 30
    if chg20 > 30:
        d = 95
    elif chg20 > 20:
        d = 85
    elif chg20 > 10:
        d = 70
    elif chg20 > 0:
        d = 55
    if amt > 5e8:
        d = min(100, d + 5)

    # ② 封板质量：越接近涨停、上影越短，封得越硬
    if pct >= 9.8:
        fb = 95 if us <= 0.15 else 78
    elif pct >= 7:
        fb = 72
    elif pct >= 5:
        fb = 58
    elif pct > 0:
        fb = 42
    else:
        fb = 20
    if f.get("close") and f.get("hi60") and close >= f["hi60"] * 0.999:
        fb = min(100, fb + 6)

    # ③ 形态位置：均线多头 + 52 周位置（太高反而扣分，属追高）
    xt = 40
    if ma5 and ma10 and ma20 and ma5 > ma10 > ma20:
        xt += 25
    if pos52 is not None:
        if 55 <= pos52 <= 90:
            xt += 20
        elif pos52 > 95:
            xt += 5
        elif pos52 < 40:
            xt -= 10
    if dev20 > 15:
        xt -= 15
    xt = max(0, min(100, xt))

    # ④ 量能资金：放量推动优于缩量新高
    if 1.2 <= vr <= 3.0 and pct > 0:
        ln = 85
    elif vr > 3.0 and pct > 0:
        ln = 70
    elif vr < 1.0 and pct > 0:
        ln = 50
    elif pct <= 0 and vr > 1.8:
        ln = 25
    else:
        ln = 45

    score = int(round((d + fb + xt + ln) / 4.0))
    if score >= 80:
        grade = "dragon"
    elif score >= 65:
        grade = "quasi"
    elif score >= 50:
        grade = "candidate"
    else:
        grade = "fake"

    # 退潮判定：以 MA10 为生命线（文档原文「沿 MA10 上方运行：主升阶段，持有为主」）
    if ma20 and close < ma20 * 0.98:
        rk, rtxt = "ice", "已跌破 MA20，龙头退潮至冰点，不接"
    elif ma10 and close < ma10:
        rk, rtxt = "ebb", "跌破 MA10 生命线，进入退潮期，反弹减仓"
    elif dev20 > 10 and pct > 0:
        rk, rtxt = "accel", "沿 MA10 上方加速，主升段，持有为主但需防冲高滞涨"
    else:
        rk, rtxt = "up", "沿 MA10 上方运行，主升阶段，持有为主，以 MA10 为生命线"

    return {
        "code": f["code"], "secid": secid_of_code(f["code"]),
        "name": f.get("name") or "", "industry": f.get("industry") or "--",
        "price": _r(close), "pct": _r(pct),
        "score": score, "grade": grade,
        "f_daidong": d, "f_fengban": fb, "f_xingtai": xt, "f_liangneng": ln,
        "retreat": rtxt, "retreat_key": rk,
        "checkup": None,
    }


def chaoduan_one(f, rows):
    """超短池：打板/接力候选。返回命中条件清单（hit）与命中数。"""
    close = f["close"]
    pct = f.get("pct") or 0.0
    vr = f.get("vol_ratio") or 1.0
    o, h, l = f.get("open"), f.get("high"), f.get("low")
    rng = max((h - l) if (h is not None and l is not None) else 1e-6, 1e-6)
    us = ((h - max(o, close)) / rng) if (h is not None and o is not None) else 0.0
    ma5, ma10 = f.get("ma5"), f.get("ma10")
    hi60 = f.get("hi60")

    hit = []
    if pct >= 9.8:
        hit.append({"k": "涨停", "ok": True, "note": "当日涨停 %+.2f%%" % pct})
    elif pct >= 5:
        hit.append({"k": "大阳", "ok": True, "note": "当日涨幅 %+.2f%%" % pct})
    if vr >= 2.0:
        hit.append({"k": "倍量", "ok": True, "note": "量比 %.2f" % vr})
    if ma5 and ma10 and ma5 > ma10:
        hit.append({"k": "短多头", "ok": True, "note": "MA5 在 MA10 上方"})
    if hi60 and close >= hi60 * 0.98:
        hit.append({"k": "近新高", "ok": True, "note": "距 60 日高点 %.1f%%" % ((close / hi60 - 1) * 100)})
    if us <= 0.25:
        hit.append({"k": "收强", "ok": True, "note": "上影占振幅 %.0f%%" % (us * 100)})
    if (f.get("amount_today") or 0) >= 1e8:
        hit.append({"k": "够流动", "ok": True, "note": "成交额 %.2f 亿" % ((f.get("amount_today") or 0) / 1e8)})

    n = len(hit)
    if n < 4:
        return None
    # 基数 35 + 每条 8 分，封板强度额外 +10：原式 40+n*9 会让所有 6 条命中的票
    # 一起顶到 100 分，A 档挤满、没有任何区分度（实测前 4 名全是 100）。
    score = min(100, 35 + n * 8)
    if pct >= 9.8 and us <= 0.15:
        score = min(100, score + 10)
    return {
        "code": f["code"], "secid": secid_of_code(f["code"]),
        "name": f.get("name") or "", "industry": f.get("industry") or "--",
        "price": _r(close), "pct": _r(pct),
        "score": score, "score_max": 100,
        "hit": hit, "hit_count": n,
        "grade": ("A" if score >= 85 else ("B" if score >= 70 else "C")),
    }


# ----------------------------------------------------------------------------
# 全市场扫描（后台线程）
# ----------------------------------------------------------------------------


def scan_state():
    st = dict(SCAN_STATE)
    return st


def _set_state(**kw):
    with _scan_lock:
        SCAN_STATE.update(kw)


def _progress(done, total):
    _set_state(done=done, total=total)


def scan_running():
    return bool(SCAN_STATE.get("running"))


def strong_file():
    return os.path.join(DATA_DIR, "strong.json")


def rightside_file():
    return os.path.join(DATA_DIR, "right_side.json")


def load_strong():
    return read_json(strong_file())


def load_rightside():
    return read_json(rightside_file())


def start_scan(target="all", reason=""):
    """启动后台全市场扫描（单例）。返回是否真的启动了。"""
    with _scan_lock:
        if SCAN_STATE.get("running"):
            return False
        SCAN_STATE.update({"running": True, "phase": "启动", "target": target, "done": 0,
                           "total": 0, "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                           "finished_at": None, "error": None, "reason": reason})
    t = threading.Thread(target=_scan_worker, args=(target,), daemon=True)
    t.start()
    return True


def _scan_worker(target):
    t0 = time.time()
    try:
        td = latest_trade_date()
        _set_state(phase="拉取全市场快照")
        snap = snapshot(ttl=1)
        rows = snap["rows"]
        # 只扫沪深（北交所流动性差、且强势股/六池都不含），省 250 个请求
        codes = [r["code"] for r in rows if not r["code"].startswith(("4", "8", "9"))]
        snap_map = {r["code"]: r for r in rows}

        _set_state(phase="补齐日K", total=len(codes), done=0)
        ok, failed = ensure_klines(codes, td=td, progress=_progress)

        _set_state(phase="指标计算 · 强势股 · 六池", total=len(codes), done=0)
        ind = industry_map()
        temp, breadth = market_temp()
        meth = read_json(os.path.join(DATA_DIR, "methodology.json")) or {}
        pool_defs = meth.get("pools") or []
        pool_name = {p["id"]: p["name"] for p in pool_defs}
        pool_hits = {p["id"]: [] for p in pool_defs}
        strong = []
        risk_list = []
        stats = {"candidates": 0, "clean": 0, "pos3": 0, "pos2": 0, "pos1": 0,
                 "risk": 0, "avg_score": 0.0}
        score_sum = 0
        scanned = 0

        # 单遍流式处理：每只票算完就地判定，算完即释放日 K，
        # 不把 4700 只的 300 根 K 线同时留在内存里（否则要吃 300MB+）。
        for i, code in enumerate(codes):
            if i % 120 == 0:
                _set_state(done=i)
            rows_k = kline_get(code)
            if not rows_k or len(rows_k) < 60:
                continue
            s = snap_map.get(code) or {}
            if is_st(s.get("name")):
                continue
            f = features(code, rows_k, s)
            if not f:
                continue
            f["industry"] = ind.get(code) or "--"
            s["industry"] = f["industry"]
            scanned += 1
            mat = maturity(f)

            # ---- 强势股（严格四条件）----
            if strong_filter(f, s):
                ch = chan(rows_k)
                ck = checkup(f, s, temp, ch)
                strong.append(strong_row(f, s, mat, ck))

            # ---- 右侧六池 / 总榜 / 风险名单 ----
            # 口径：先看是否命中任一池（=有右侧迹象的候选），再在候选里判破位/走弱。
            if mat["score"] <= 0:
                continue
            hits = [pid for pid in pool_hits if pool_hit(f, pid)[0]]
            if not hits:
                continue
            lv = level_of(f.get("dev20"))
            risk, risk_note = risk_of(f)
            if risk:
                risk_list.append({
                    "code": code, "name": s.get("name"),
                    "setcode": "1" if code[:1] == "6" else "0",
                    "secid": secid_of_code(code),
                    "price": _r(f["close"]), "pct": _r(f.get("pct")),
                    "board": board_of(code), "industry": f["industry"],
                    "dev20": _r(f.get("dev20")), "risk": risk, "risk_note": risk_note,
                })
                stats["risk"] += 1
                continue
            stats["candidates"] += 1
            stats[lv] += 1
            score_sum += mat["score"]

            ch = chan(rows_k)
            vp = volprice(f)
            ck = checkup(f, s, temp, ch)
            for pid in hits:
                pool_hits[pid].append(
                    pool_stock(f, pid, pool_name.get(pid, pid), mat, ck, ch, vp, s))

        strong.sort(key=lambda r: (-(r["score"] * 0.6 + (r.get("chan_score") or 60) * 0.4),
                                   -(r["chg20"] or 0)))
        write_json(strong_file(), {
            "updated": time.strftime("%Y-%m-%d %H:%M:%S"),
            "trade_date": td,
            "conditions": STRONG_COND,
            "source": STRONG_SOURCE,
            "total_scanned": scanned,
            "kline_failed": failed,
            "count": len(strong),
            "list": strong,
            "elapsed": round(time.time() - t0, 1),
        })

        # ---------- 右侧六池：排序与统计 ----------
        _set_state(phase="六池分级 · 汇总", done=0, total=len(codes))
        pools = []
        rank_all = []
        for lst in pool_hits.values():
            rank_all.extend(lst)

        # 去重总榜（同一只票可能命中多池，取综合分最高的一条）
        best = {}
        for r in rank_all:
            k = r["code"]
            if k not in best or (r["composite_score"] or 0) > (best[k]["composite_score"] or 0):
                best[k] = r
        rank = list(best.values())
        lv_order = {"pos3": 0, "pos2": 1, "pos1": 2}
        rank.sort(key=lambda r: (lv_order.get(r["level_key"], 9), -(r["composite_score"] or 0)))
        rank = rank[:30]

        stats["clean"] = stats["candidates"]
        stats["avg_score"] = round(score_sum / stats["candidates"], 2) if stats["candidates"] else 0

        for p in pool_defs:
            lst = pool_hits.get(p["id"], [])
            lst.sort(key=lambda r: (lv_order.get(r["level_key"], 9),
                                    {"可介入": 0, "观察": 1, "回避": 2}.get(
                                        (r.get("checkup") or {}).get("verdict"), 3),
                                    -(r["score"] or 0)))
            lc = {"pos3": 0, "pos2": 0, "pos1": 0}
            vpc = {"confirm": 0, "warn": 0, "mid": 0, "none": 0}
            for r in lst:
                lc[r["level_key"]] = lc.get(r["level_key"], 0) + 1
                g = (r.get("vp") or {}).get("gate") or "none"
                vpc[{"block": "warn", "pass": "confirm", "watch": "mid"}.get(g, "none")] += 1
            pools.append({
                "id": p["id"], "name": p["name"], "icon": p["icon"],
                "logic": p["logic"], "scan": p["scan"], "ref": p.get("ref") or "",
                "scanned": len(lst), "stocks": lst,
                "level_count": lc, "vp_counts": vpc,
                "empty_msg": POOL_EMPTY.get(p["id"], "今日全市场 0 只命中"),
            })

        # ---------- 市场环境 ----------
        breadth_pct = breadth["breadth"]
        if breadth_pct >= 60:
            phase = "普涨 · 情绪偏暖"
            verdict = "右侧环境偏暖：位置3 标的可正常建仓，注意别在高潮日追突破"
            advice = "只做位置3，追高位一律不碰；情绪高潮日分批减仓而非加仓"
        elif breadth_pct >= 45:
            phase = "结构分化 · 趋势机会集中于少数个股"
            verdict = "右侧环境中性：位置3 占有效候选 %d%%，只做「已确认且贴均线」的标的" % (
                round(stats["pos3"] * 100.0 / max(1, stats["candidates"])))
            advice = "只做位置3，追高位（位置1）一律不碰；分批建仓，单笔风险不超过总资金 2%"
        else:
            phase = "普跌 · 突破易失败"
            verdict = "右侧环境偏冷：突破失败率高，建议空仓或只用一成仓位试错"
            advice = "降低仓位、提高信号门槛（量比≥2 且创 60 日新高才看）"

        signals = [
            {"k": "位置3（安全介入区）", "v": "%d 只 / 有效候选 %d 只" % (stats["pos3"], stats["candidates"]),
             "level": "good" if stats["pos3"] else "neu"},
            {"k": "追高位（位置1）", "v": "%d 只已偏离 20 日线超 15%%" % stats["pos1"], "level": "warn"},
            {"k": "破位/走弱已剔除", "v": "%d 只（不参与右侧池）" % stats["risk"], "level": "neu"},
            {"k": "市场宽度", "v": "上涨 %d / 下跌 %d（上涨占比 %.1f%%）" % (
                breadth["up"], breadth["down"], breadth_pct), "level": "good" if breadth_pct > 50 else "warn"},
        ]

        vp_counts = {"confirm": 0, "warn": 0, "mid": 0, "none": 0}
        for r in rank:
            g = (r.get("vp") or {}).get("gate")
            vp_counts[{"pass": "confirm", "block": "warn", "watch": "mid"}.get(g, "none")] += 1

        write_json(rightside_file(), {
            "updated": time.strftime("%Y-%m-%d %H:%M:%S"),
            "trade_date": td,
            "source": "新浪全市场快照 + 新浪日 K 复算（本机实算，非外部导入）",
            "note": ("六个池子由本机全市场实算得出：扫描 %d 只（K 线失败 %d 只），命中 %d 只；"
                     "破位/走弱 %d 只已剔除，不进池也不进榜。池内与总榜均按位置排序 —— "
                     "位置3 右侧安全位在前，位置2 中间位居中，位置1 追高位在最后。"
                     "评分口径：右侧成熟度 = 结构25+突破15+量能20+动量15+位置25；"
                     "综合分 = 成熟度×0.6 + 缠论×0.4。所有评分与价位均为算法推算，不构成投资建议。"
                     ) % (scanned, failed, sum(len(p["stocks"]) for p in pools), stats["risk"]),
            "market": {"phase": phase, "verdict": verdict, "advice": advice,
                       "breadth_note": "有效候选 %d 只 · 位置3 %d · 位置2 %d · 位置1 %d" % (
                           stats["candidates"], stats["pos3"], stats["pos2"], stats["pos1"]),
                       "signals": signals},
            "concept": meth.get("concept") or {},
            "pools": pools,
            "rank": rank,
            "risk_list": risk_list[:60],
            "stats": stats,
            "discipline": meth.get("discipline") or [],
            "disclaimer": meth.get("disclaimer") or "",
            "vp_counts": vp_counts,
            "vp_note": meth.get("vp_note") or "",
            "elapsed": round(time.time() - t0, 1),
            "temperature": round(temp, 1),
        })

        _set_state(phase="完成", running=False, done=len(codes), total=len(codes),
                   finished_at=time.strftime("%Y-%m-%d %H:%M:%S"),
                   last={"strong": len(strong), "pools": sum(len(p["stocks"]) for p in pools),
                         "elapsed": round(time.time() - t0, 1), "kline_failed": failed,
                         "trade_date": td})
    except Exception as e:
        import traceback
        traceback.print_exc()
        _set_state(running=False, phase="失败", error="%s: %s" % (type(e).__name__, e),
                   finished_at=time.strftime("%Y-%m-%d %H:%M:%S"))


def ensure_scan_async(target="all", reason="stale"):
    """缓存过期或缺失时自动补扫（幂等，不阻塞请求）。"""
    td = latest_trade_date()
    s = load_strong() or {}
    r = load_rightside() or {}
    stale = (s.get("trade_date") != td) or (r.get("trade_date") != td) or not s.get("list")
    if stale and not scan_running():
        return start_scan(target, reason)
    return False


# ----------------------------------------------------------------------------
# 单票体检（在线，读本地日 K；缺失则补一只）
# ----------------------------------------------------------------------------


def checkup_one(code, secid=None, slim=False):
    code = str(code or "").strip()
    if not code and secid:
        code = str(secid).split(".")[-1]
    if not code:
        return {"ok": False, "error": "缺少 code/secid"}
    td = latest_trade_date()
    rows = kline_get(code)
    if not rows or (read_json(_kl_path(code)) or {}).get("td") != td:
        ensure_klines([code], td=td, workers=1)
        rows = kline_get(code)
    if not rows:
        return {"ok": False, "error": "未取到该股日 K（代码 %s 可能已退市或停牌）" % code}
    snap_map = snap_index()
    s = snap_map.get(code) or {}
    if s and s.get("industry") is None:
        s["industry"] = industry_map().get(code) or "--"
    f = features(code, rows, s)
    if not f:
        return {"ok": False, "error": "日 K 不足 60 根，无法体检"}
    f["industry"] = s.get("industry") or "--"
    temp, _ = market_temp()
    ck = checkup(f, s, temp)
    out = {
        "ok": True, "code": code, "secid": secid_of_code(code),
        "name": s.get("name") or "", "board": board_of(code),
        "price": _r(f["close"]), "pct": _r(f.get("pct")),
        "trade_date": f["date"], "ksource": "sina",
        "checkup_score": ck["checkup_score"], "score": ck["checkup_score"],
        "verdict": ck["verdict"], "strategy_score": ck["strategy_score"],
        "verdict_note": ck["verdict_note"],
        "liquidity": ck["liquidity"], "liq_ok": ck["liq_ok"], "liq_min": ck["liq_min"],
        "dims": ck["dims"], "radar": ck["radar"], "vetoes": ck["vetoes"],
        "plan": ck["plan"], "volprice": ck["volprice"], "chan": ck["chan"],
        "mainwave": None, "level_key": ck["level_key"],
        "computed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "data_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "disclaimer": "本体检由公开行情数据与公开方法论框架自动计算，仅供研究参考，不构成投资建议。",
    }
    if slim:
        return {"ok": True, "code": code, "name": out["name"],
                "score": out["score"], "strategy_score": out["strategy_score"],
                "verdict": out["verdict"], "verdict_note": out["verdict_note"],
                "vetoes": len(out["vetoes"]), "liq_ok": out["liq_ok"],
                "liquidity": out["liquidity"], "weak": ck["weak"], "dims": ck["dims"]}
    out["rows"] = [{"date": r[0], "open": r[1], "close": r[2], "high": r[3],
                    "low": r[4], "vol": r[5]} for r in rows[-120:]]
    return out


def chanscore_many(codes, min_score=0):
    out = {}
    for code in codes:
        rows = kline_get(code)
        if not rows:
            continue
        ch = chan(rows)
        if not ch:
            continue
        if ch["score"] < min_score:
            continue
        out[code] = {"score": ch["score"], "grade": ch["grade"], "walk": ch["walk"]}
    return out


def scores_top(n=20):
    """综合评分 Top N：直接取强势股榜（同一份扫描结果），按 6:4 综合分排序。"""
    d = load_strong() or {}
    lst = list(d.get("list") or [])
    lst.sort(key=lambda r: -(r.get("score", 0) * 0.6 + (r.get("chan_score") or 60) * 0.4))
    return {
        "updated": d.get("updated"), "trade_date": d.get("trade_date"),
        "note": "同日强势股扫描结果（成熟度×0.6 + 缠论×0.4）",
        "list": lst[:n], "count": min(n, len(lst)),
    }


def checkup_top(n=60):
    """体检页「全市场策略适配分排行」：直接复用右侧扫描结果里的体检结论。"""
    rs = load_rightside() or {}
    seen = {}
    for pool in rs.get("pools") or []:
        for s in pool.get("stocks") or []:
            ck = s.get("checkup") or {}
            if ck.get("score") is None:
                continue
            prev = seen.get(s["code"])
            if prev and (prev.get("strategy_score") or 0) >= (ck.get("strategy_score") or 0):
                continue
            seen[s["code"]] = {
                "code": s["code"], "secid": s.get("secid"), "name": s["name"],
                "price": s.get("price"), "pct": s.get("pct"),
                "score": ck.get("score"), "strategy_score": ck.get("strategy_score"),
                "verdict": ck.get("verdict"), "verdict_note": ck.get("verdict_note"),
                "vetoes": ck.get("vetoes") or 0, "dims": ck.get("dims") or [],
                "board": s.get("board"), "industry": s.get("industry"),
            }
    items = sorted(seen.values(), key=lambda x: -(x.get("strategy_score") or 0))[:n]
    return {"updated": rs.get("updated"), "trade_date": rs.get("trade_date"),
            "count": len(items), "items": items}
