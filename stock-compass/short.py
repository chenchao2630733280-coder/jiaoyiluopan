# -*- coding: utf-8 -*-
"""短线作战（short）—— 六个池 + 市场环境 + 主线板块 + 龙虎榜资金性格。

设计原则（与全站一致）：
  1. 只接公开接口 + 本地 K 线缓存，不引第三方库（U 盘便携版没有 pip）。
  2. 宁可显式空态，不给假数据 —— 拿不到就返回空列表并写明原因。
  3. 结论由数据推导。池子来自已有扫描结果，量价/体检/缠论复用 engine 的同一套口径，
     本模块只新增两件全站没有的东西：**21 日突破窗口**与**龙虎榜资金性格**。

数据源：
  · 东财 datacenter RPT_DAILYBILLBOARD_DETAILSNEW   当日龙虎榜上榜个股 + 净买额
  · 东财 datacenter RPT_BILLBOARD_DAILYDETAILSBUY   买前五营业部席位
  · 东财 datacenter RPT_BILLBOARD_DAILYDETAILSSELL  卖前五营业部席位
  · 东财 push2 行业板块（主力净流入 / 涨跌家数）+ 板块成分
  · engine.snapshot / kline_get / features / checkup / volprice   本地已有口径
"""
import json
import os
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from statistics import median

import engine

STORE_DIR = os.environ.get("COMPASS_STORE") or engine.BASE_DIR
DATA_DIR = os.path.join(STORE_DIR, "data")
LHB_FILE = os.path.join(DATA_DIR, "lhb.json")

PAYLOAD_TTL = 60       # 整包缓存秒数

_EM_API = "https://datacenter-web.eastmoney.com/api/data/v1/get"
_PUSH_API = "https://push2.eastmoney.com/api/qt/clist/get"
_ULIST_API = "https://push2.eastmoney.com/api/qt/ulist.np/get"
_EM_REF = "https://data.eastmoney.com/"
_QT_REF = "https://quote.eastmoney.com/"


# ---------------------------------------------------------------------------
# 一、东财取数
# ---------------------------------------------------------------------------

def _em(report, columns="ALL", page=1, size=500, filter_="", sort="", desc=False):
    """datacenter-web 通用取数。返回 result.data 列表，失败返回 []。"""
    q = {
        "reportName": report, "columns": columns,
        "pageNumber": page, "pageSize": size,
        "source": "WEB", "client": "WEB",
    }
    if filter_:
        q["filter"] = filter_
    if sort:
        q["sortColumns"] = sort
        q["sortTypes"] = "-1" if desc else "1"
    url = _EM_API + "?" + urllib.parse.urlencode(q)
    try:
        d = engine.http_json(url, referer=_EM_REF, timeout=25)
    except Exception:
        return []
    if not d or not d.get("success", True):
        return []
    r = d.get("result") or {}
    return r.get("data") or []


def _em_paged(report, filter_, size=500, max_pages=8):
    """按页拉全。席位表一天 84 只 × 5 席位 ≈ 420 行，1~2 页就够。"""
    out, page = [], 1
    while page <= max_pages:
        rows = _em(report, page=page, size=size, filter_=filter_)
        if not rows:
            break
        out.extend(rows)
        if len(rows) < size:
            break
        page += 1
    return out


# 网络失败不能静默：_push 返回 {} 时把原因记在这里，由 api_short 带进 payload。
# 之前直接吞异常，一次超时就让「主线板块」整块变空，页面上看不出是没数据还是没请求到。
_NET = {"fails": 0, "last": ""}


def _push(path_q, tries=2):
    """东财 push2 行情接口，失败重试一次。

    重试是必要的：这个接口偶发 20 秒超时，而它承载的是「板块列表」这种
    单点数据 —— 一次失败整块板块就没了，用户只会看到空白。
    """
    err = ""
    for i in range(tries):
        try:
            d = engine.http_json(_PUSH_API + "?" + path_q, referer=_QT_REF, timeout=20)
            if d:
                _NET["fails"] = 0
                _NET["last"] = ""
                return d
            err = "接口返回空"
        except Exception as e:
            err = "%s: %s" % (type(e).__name__, e)
        if i + 1 < tries:
            time.sleep(0.5)
    _NET["fails"] += 1
    _NET["last"] = err
    return {}


# ---------------------------------------------------------------------------
# 二、龙虎榜 · 资金性格
# ---------------------------------------------------------------------------
"""席位归类。**只判三类**，其余一律归为「营业部（游资/大户）」：

  · 机构类：`机构专用` / 券商总部（自营）/ 外资投行在华席位（高盛、摩根大通等）
  · 通道类：沪深股通、港股通专用
  · 散户类：东方财富证券各营业部（含拉萨系，互联网开户集中地）

为什么不做「知名游资营业部」白名单：营业部会改名（国泰君安已改国泰海通）、
游资会换席位，白名单一年就过期，还会把普通营业部误判成游资。
只用上面三类做分母，剩下的大额营业部买单本身就是「游资/大户」的行为特征 ——
判定标准可复算，也不怕改名。
`_HOT_TAGS` 仅用于给用户看「这笔钱可能是谁」，不参与分类。
"""
_INST_FRAGS = ("机构专用", "总部", "自营", "资产管理", "高盛", "摩根大通", "摩根士丹利",
               "瑞银", "中国国际金融", "花旗", "德意志", "汇丰", "渣打", "巴黎")
_PASS_FRAGS = ("沪股通专用", "深股通专用", "港股通")
_RETAIL_FRAGS = ("东方财富证券", "西藏东方财富")

# 知名游资席位画像：只用于 hot_tags 展示，不参与 money 判定
_HOT_TAGS = [
    ("华宝证券股份有限公司上海东大名路", "华宝东大名路"),
    ("华鑫证券有限责任公司上海分公司", "章盟主/养家系"),
    ("上海宛平南路", "炒股养家"),
    ("上海溧阳路", "溧阳路"),
    ("深圳泰然九路", "深圳敢死队"),
    ("佛山季华六路", "佛山系"),
    ("佛山绿景路", "佛山系"),
    ("杭州上塘路", "杭州帮"),
    ("杭州体育场路", "杭州帮"),
    ("绍兴", "绍兴帮"),
    ("厦门厦禾路", "厦门帮"),
    ("成都蜀金路", "成都系"),
    ("上海江苏路", "上海超短"),
    ("宁波桑田路", "宁波敢死队"),
    ("西安西大街", "西安帮"),
    ("无锡梁清路", "无锡帮"),
    ("瑞平路", "华鑫系"),
]

_MONEY_ORDER = ["游资主导", "机构主导", "游资+机构混战", "通道资金主导",
                "散户接盘", "机构出货", "无明显主力"]

_MONEY_TEXT = {
    "游资主导": ("营业部席位买盘占优（游资/大户主导）",
               "短线资金主动进攻，节奏快、持股周期短，跟随要快进快出，破位即走"),
    "机构主导": ("机构专用/外资席位买盘占优",
               "机构建仓通常对应基本面逻辑，节奏慢但延续性强，可给更宽的止损"),
    "游资+机构混战": ("游资与机构同时出现在买方",
                  "两类资金意见一致时爆发力最强，但也最容易被彼此的反手砸盘打乱"),
    "通道资金主导": ("沪深股通席位买盘占优",
                "北向资金偏配置型，对短线形态的推动不如游资直接"),
    "散户接盘": ("东财系（散户大本营）买盘占优而主力净卖出",
               "典型的筹码由主力转移到散户，次日承接力弱，需警惕"),
    "机构出货": ("机构专用席位在卖方占优",
               "机构派发往往是趋势转折的前兆，不宜接力"),
    "无明显主力": ("买卖前五未见明显的主力席位特征",
                "资金结构分散，缺乏主导力量"),
}


def _seat_kind(name):
    n = name or ""
    for f in _PASS_FRAGS:
        if f in n:
            return "pass"
    for f in _INST_FRAGS:
        if f in n:
            return "inst"
    for f in _RETAIL_FRAGS:
        if f in n:
            return "retail"
    return "broker"      # 普通营业部 = 游资/大户


def _seat_tag(name):
    n = name or ""
    for frag, tag in _HOT_TAGS:
        if frag in n:
            return tag
    return ""


def _classify(buys, sells, deal_amt):
    """按买卖前五席位的构成判定资金性格。全部为比例判定，阈值写在这里便于复算。"""
    def total(rows, key):
        return sum(float(r.get(key) or 0) for r in rows)

    buy_sum = total(buys, "BUY")
    sell_sum = total(sells, "SELL")
    net = buy_sum - sell_sum

    def part(rows, key, kind):
        s = 0.0
        for r in rows:
            if _seat_kind(r.get("OPERATEDEPT_NAME")) == kind:
                s += float(r.get(key) or 0)
        return s

    inst_buy = part(buys, "BUY", "inst")
    hot_buy = part(buys, "BUY", "broker")     # 营业部买单 ≈ 游资/大户
    retail_buy = part(buys, "BUY", "retail")
    pass_buy = part(buys, "BUY", "pass")
    inst_sell = part(sells, "SELL", "inst")

    bs = buy_sum or 1.0
    ss = sell_sum or 1.0

    # 先判「钱在往外走」的结构，再判「钱在往里进」的结构。
    # 净卖出时不能叫"某类资金主导"——那会被读成看多信号。
    if inst_sell >= ss * 0.5 and net < 0 and inst_sell >= inst_buy:
        money = "机构出货"
    elif retail_buy >= bs * 0.4 and net < 0:
        money = "散户接盘"
    elif net >= 0 and inst_buy >= bs * 0.5:
        money = "机构主导"
    elif net >= 0 and pass_buy >= bs * 0.5:
        money = "通道资金主导"
    elif net >= 0 and hot_buy >= bs * 0.5:
        money = "游资主导"
    elif net >= 0 and hot_buy >= bs * 0.25 and inst_buy >= bs * 0.25:
        money = "游资+机构混战"
    else:
        money = "无明显主力"

    hot_tags = []
    for r in buys:
        tag = _seat_tag(r.get("OPERATEDEPT_NAME"))
        if tag and tag not in hot_tags:
            hot_tags.append(tag)

    label, _note = _MONEY_TEXT[money]
    warn = ""
    if money == "机构出货":
        warn = "机构在卖方占优，属派发结构"
    elif money == "散户接盘":
        warn = "主力净卖出而散户席位大举买入，筹码结构恶化的典型形态"

    ratio = None
    if deal_amt:
        ratio = round(net / float(deal_amt) * 100, 2)

    return {
        "money": money, "label": label, "warn": warn,
        "net_amt": round(net, 0), "buy_amt": round(buy_sum, 0), "sell_amt": round(sell_sum, 0),
        "net_ratio": ratio,
        "hot_tags": hot_tags,
        "inst_buy": round(inst_buy, 0), "inst_sell": round(inst_sell, 0),
        "retail_buy": round(retail_buy, 0), "pass_buy": round(pass_buy, 0),
        "broker_buy": round(hot_buy, 0),
        "seats_buy": [_seat_row(r, "BUY") for r in buys[:5]],
        "seats_sell": [_seat_row(r, "SELL") for r in sells[:5]],
    }


def _seat_row(r, key):
    """前端 seats_buy 读 {tag,name,buy}，seats_sell 读 {tag,name,sell} —— 键名固定。"""
    tag = _seat_tag(r.get("OPERATEDEPT_NAME"))
    amt = round(float(r.get(key) or 0), 0)
    row = {"name": r.get("OPERATEDEPT_NAME") or "", "tag": tag}
    row["buy" if key == "BUY" else "sell"] = amt
    return row


def build_lhb(force=False):
    """构建当日龙虎榜资金性格表，落 data/lhb.json。

    只取**当日**（最近一个有数据的披露日）。龙虎榜是 T+1 盘后数据，历史值没有
    短线意义，而且历史全量会到几百 MB。缓存文件按 trade_date 判新旧。
    """
    t0 = time.time()
    recent = _em("RPT_DAILYBILLBOARD_DETAILSNEW", columns="TRADE_DATE",
                 size=1, sort="TRADE_DATE", desc=True)
    td = ""
    if recent:
        td = str(recent[0].get("TRADE_DATE") or "")[:10]
    if not td:
        return {"ok": False, "error": "龙虎榜接口无返回", "items": {}, "trade_date": ""}

    old = engine.read_json(LHB_FILE, {}) or {}
    if not force and old.get("trade_date") == td and old.get("items"):
        return old

    filt = "(TRADE_DATE='%s')" % td
    board = _em_paged("RPT_DAILYBILLBOARD_DETAILSNEW", filt)
    buys = _em_paged("RPT_BILLBOARD_DAILYDETAILSBUY", filt)
    sells = _em_paged("RPT_BILLBOARD_DAILYDETAILSSELL", filt)

    by_code_buy, by_code_sell = {}, {}
    for r in buys:
        by_code_buy.setdefault(str(r.get("SECURITY_CODE") or ""), []).append(r)
    for r in sells:
        by_code_sell.setdefault(str(r.get("SECURITY_CODE") or ""), []).append(r)

    items, explains = {}, {}
    skipped = {"非A股": 0, "ST或退市": 0}
    for r in board:
        code = str(r.get("SECURITY_CODE") or "")
        if not code:
            continue
        # 龙虎榜会把可转债、退市整理股也算进来；短线池只要正常 A 股。
        name = str(r.get("SECURITY_NAME_ABBR") or "")
        if not engine.is_a_share(code):
            skipped["非A股"] += 1
            continue
        if engine.is_st(name) or "退" in name:
            skipped["ST或退市"] += 1
            continue
        # 同一只票可能因多个上榜原因出现多行（如"涨幅偏离"+"换手率"），
        # 席位是同一批，只算一次；把原因合并成一个字符串给用户看。
        ex = str(r.get("EXPLANATION") or "").strip()
        if ex:
            lst = explains.setdefault(code, [])
            if ex not in lst:
                lst.append(ex)
        if code in items:
            continue
        d = _classify(by_code_buy.get(code) or [], by_code_sell.get(code) or [],
                      r.get("ACCUM_AMOUNT"))
        d.update({
            "code": code,
            "name": r.get("SECURITY_NAME_ABBR") or "",
            "trade_date": td,
            "pct": engine._r(r.get("CHANGE_RATE")),
            "close": engine._r(r.get("CLOSE_PRICE")),
            "explain": "",
            "board_net_amt": round(float(r.get("BILLBOARD_NET_AMT") or 0), 0),
            "deal_amt": round(float(r.get("ACCUM_AMOUNT") or 0), 0),
            "turnover": engine._r(r.get("TURNOVERRATE")),
        })
        items[code] = d

    for code, d in items.items():
        d["explain"] = "；".join(explains.get(code) or [])

    out = {
        "ok": True, "trade_date": td,
        "updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(items),
        "seats_raw": {"buy": len(buys), "sell": len(sells)},
        "skipped": skipped,
        "elapsed": round(time.time() - t0, 1),
        "note": "龙虎榜为 T+1 盘后披露，覆盖当日异动个股（约 5%~10%），"
                "未上榜不代表没有主力，只代表没触发披露条件。",
        "items": items,
    }
    os.makedirs(DATA_DIR, exist_ok=True)
    engine.write_json(LHB_FILE, out)
    return out


_LHB = {"ts": 0.0, "td": None, "data": None}


def lhb_table(force=False):
    """内存 + 磁盘两级缓存。盘中首次调用约 3~5 秒（4~6 个请求）。"""
    td = engine.latest_trade_date()
    now = time.time()
    if not force and _LHB["data"] and _LHB["td"] == td and now - _LHB["ts"] < 600:
        return _LHB["data"]
    d = build_lhb(force=force)
    if d.get("ok"):
        _LHB.update({"ts": now, "td": td, "data": d})
    return d or {}


# ---------------------------------------------------------------------------
# 三、21 日突破窗口
# ---------------------------------------------------------------------------
"""把「突破后走到第几天」显式化。

书里的时间纪律是：突破刚成立（1~5 日）才有赔率，6~14 日是验证期，
15~20 日窗口将尽该验货，跌回平台即失守。用 20 日新高定义突破，
再数它已经过去几个交易日。全部从 K 线现算，不依赖任何外部标记。
"""
BO_WIN = 21          # 窗口总长（交易日）
BO_LOOKBACK = 20     # 突破定义：收盘价创前 20 日新高


def bo_of(rows):
    if not rows or len(rows) < BO_LOOKBACK + 3:
        return None
    # rows: [date, open, close, high, low, vol]
    n = len(rows)
    last = n - 1
    b = -1
    for i in range(max(BO_LOOKBACK, n - BO_WIN - 1), n):
        hi = max(float(rows[j][3]) for j in range(i - BO_LOOKBACK, i))
        if float(rows[i][2]) > hi:
            b = i
    if b < 0:
        return None

    day = last - b
    close = float(rows[last][2])
    bclose = float(rows[b][2])
    platform = min(float(rows[j][4]) for j in range(max(0, b - 2), b + 1))
    gain = round((close / bclose - 1) * 100, 2) if bclose else 0.0

    if close < platform:
        phase = "失守"
    elif day <= 5:
        phase = "首启"
    elif day <= 14:
        phase = "验证"
    elif day <= 20:
        phase = "末窗"
    else:
        phase = "过窗"

    return {
        "phase": phase, "day": day,
        "breakout_date": rows[b][0],
        "breakout_price": round(bclose, 2),
        "platform": round(platform, 2),
        "gain_since": gain,
        "note": "第 %d 个交易日 · 突破于 %s（平台下沿 %.2f）" % (day, rows[b][0], platform),
    }


# ---------------------------------------------------------------------------
# 四、主线板块（东财行业板块 + 成分股）
# ---------------------------------------------------------------------------
SECTOR_N = 8           # 展示板块数的上限（与原站一致）：是「最多 8 行」，
                       # 不是「必须 8 行」—— 独立板块不够时宁可少一行，不重复凑数

# 板块成分要判定「是否属于可交易范围」，口径必须与 universe 完全一致：
# 主板 60/00 · 非 ST · 总市值 < 200 亿。否则「N 只 · 涨停 X 家」会把
# 创业板/科创板/北交所的票算进来，跟「只做主板」的池子对不上号。
UNI_MV_MAX = 200.0


def _in_universe(row):
    code = str(row.get("f12") or "")
    if code[:2] not in ("60", "00"):
        return False
    if engine.is_st(row.get("f14") or ""):
        return False
    mv_yi = float(row.get("f20") or 0) / 1e8     # f20 = 总市值（元）
    return 0 < mv_yi < UNI_MV_MAX


def _limit_pct(code):
    """涨停判定阈值：创业板/科创板 20%，北交所 30%，其余 10%。

    取 19.5 / 29.5 / 9.8 这种「略低于上限」的值，是为了避开收盘竞价的尾差
    （比如 +9.96% 也是涨停）。北交所必须单列：920/8x/4x 是 30cm，
    当成 10cm 会把 +10% 的票误计成涨停，直接把板块涨停占比算高。
    """
    if code.startswith(("30", "688")):
        return 19.5
    if code.startswith(("4", "8", "920")):
        return 29.5
    return 9.8


def _board_members(bk, total):
    """取板块全部成分（接口单页硬上限 100 条，大板块必须翻页）。

    ★ 两条都不能省，否则强度排名直接失真：
    1) 不翻页 —— 电力设备有 409 只成分，只拿到按涨幅降序的前 100 只，
       涨停占比被放大 3~4 倍、中位涨幅被抬到 7%（真实 2.8%）；
    2) 中途放弃 —— 东财 push2 在并发下会 RemoteDisconnected，以前一失败就
       break，得到「看起来很完整的 100 行」。

    实现上先按 total 算出页数，**页与页并发**抓；某页空就串行补抓（最多 3 次）。
    串行 5 页要 2 秒，并发只要 0.6 秒，8 个板块串起来就是十几秒的差别。

    返回 (rows, complete)。
    """
    size = 100
    want = min(total or size, 800)          # 真实上限：电子 512 只，别自作主张截断
    npages = max(1, min(int((want + size - 1) // size), 8))

    def page(pn):
        for t in range(3):
            d = _push("pn=%d&pz=%d&po=1&np=1&fltt=2&invt=2&fid=f3&fs=b:%s"
                      "&fields=f12,f14,f3,f2,f20" % (pn, size, bk), tries=1)
            batch = ((d.get("data") or {}).get("diff") or [])
            if batch:
                return pn, batch
            time.sleep(0.25 * (t + 1))
        return pn, []

    results = {}
    if npages == 1:
        results[1] = page(1)[1]
    else:
        with ThreadPoolExecutor(max_workers=2) as ex:
            for pn, batch in ex.map(page, range(1, npages + 1)):
                results[pn] = batch
        # 并发偶发断连时，只补真的空了的页，并且串行补 —— 慢一点但正确
        for pn in [i for i in range(1, npages + 1) if not results.get(pn)]:
            results[pn] = page(pn)[1]

    def collect(res):
        out = []
        for pn in sorted(res):
            out.extend(res[pn])
        return [x for x in out if x.get("f3") not in (None, "-")]

    rows = collect(results)
    need = min(want, total or want)

    # 并发抓不齐时做一次「整块串行重抓」：东财在乱序并发下会固定让某一页返回空，
    # 而同样的页在串行请求里是好的。只有取不齐的板块才会走到这里（实测占比很小），
    # 所以这一次额外开销值得 —— 电力设备 411 只只拿到 312 只时，中位涨幅与
    # 涨停占比都会失真，强度排名跟着歪。
    if total and total >= 100 and len(rows) < need * 0.95:
        seq = {}
        for pn in range(1, npages + 1):
            seq[pn] = page(pn)[1]
            time.sleep(0.12)
        alt = collect(seq)
        if len(alt) > len(rows):
            rows = alt

    complete = (not want) or len(rows) >= need * 0.95
    return rows, complete


def _sectors(streak_map, diag=None):
    """板块强度 = 涨幅中位数30 + 龙头溢价20 + 涨停占比30 + 主力净流入20。

    这四项全部有出处：前三项来自板块成分实时行情，第四项来自东财资金流。
    不做"概念板块"，因为概念重叠严重，同一只票会在十几个概念里重复计数；
    行业板块之间也有父子关系（光伏设备 ⊂ 电力设备），所以对成分高度重合的
    板块只保留强度更高的那个，避免同一批票占两行。

    diag（可选 dict）：空结果时写回原因，前端据此显示「拉取失败」而不是「无数据」。
    """
    def _fail(msg):
        if diag is not None:
            diag["error"] = msg
        return []

    q = ("pn=1&pz=%d&po=1&np=1&fltt=2&invt=2&fid=f62&fs=m:90+t:2+f:!50"
         "&fields=f12,f14,f3,f62,f104,f105,f128,f136" % (SECTOR_N + 22))
    d = _push(q)
    boards = ((d.get("data") or {}).get("diff") or [])
    if not boards:
        return _fail("板块列表接口无返回（%s）" % (_NET["last"] or "未知原因"))

    # 候选要留够：东财资金流榜前几名的父子板块极密（半导体/半导体设备/半导体材料、
    # 传媒/营销代理/广告营销、电池/锂电池…），去重之后 14 个候选只剩 6 个独立板块。
    # 留 14 个余量才凑得满 8 行；再多就只是徒增请求，耗时线性上涨。
    cands = boards[:SECTOR_N + 14]
    totals = {b.get("f12"): int(b.get("f104") or 0) + int(b.get("f105") or 0)
              for b in cands if b.get("f12")}

    # ★ 并发只用来省时间，正确性不能交给它：实测 4 路并发拉成分时，东财会偶发
    #   RemoteDisconnected，返回「看着完整的半份数据」（电力设备 412 只只拿到
    #   200 只），强度排名随之在 63 与 70 之间抖动，板块分级一会儿「次主线」
    #   一会儿「主线」。所以并发只跑第一轮，没取全的板块串行补抓一次 ——
    #   单跑一次 2 秒，8 个板块全补也就十几秒，比排名不稳好得多。
    with ThreadPoolExecutor(max_workers=2) as ex:
        futs = {bk: ex.submit(_board_members, bk, totals.get(bk)) for bk in totals}
        members = {k: f.result() for k, f in futs.items()}
    for bk, (rows, ok) in list(members.items()):
        if not ok or not rows:
            members[bk] = _board_members(bk, totals.get(bk))

    out, kept_sets = [], []
    for b in cands:
        if len(out) >= SECTOR_N:
            break
        bk = b.get("f12")
        name = b.get("f14") or ""
        rows, complete = members.get(bk) or ([], False)
        if not bk or not name or not rows:
            continue

        codes = {str(x.get("f12")) for x in rows}

        # 父子板块去重：重叠 ≥85% 时**保留更宽的那个**。
        # 判定方向不能是「先到先得」—— 子板块资金流常排在父板块前面，
        # 那样「半导体设备」（25 只）会把「半导体」（188 只）顶掉，
        # 结果拿一个 25 只的细分当主线，涨停占比也会被小样本放大。
        drop_self = False
        for i, prev in enumerate(kept_sets):
            inter = len(codes & prev)
            if not inter or inter / float(min(len(codes), len(prev))) < 0.85:
                continue
            if len(codes) <= len(prev) * 1.15:
                drop_self = True
            else:
                out.pop(i)
                kept_sets.pop(i)
            break
        if drop_self:
            continue

        pcts = sorted(float(x["f3"]) for x in rows)
        med = median(pcts)
        zt = [x for x in rows if float(x.get("f3") or 0) >= _limit_pct(str(x.get("f12") or ""))]
        zt_ratio = len(zt) / float(len(rows))
        lead = rows[0]
        lead_pct = float(lead.get("f3") or 0)
        premium = max(0.0, lead_pct - med)
        net_yi = float(b.get("f62") or 0) / 1e8

        def cap(v, full):
            return max(0.0, min(100.0, v / full * 100.0))

        strength = round(cap(med, 5.0) * 0.30 + cap(premium, 10.0) * 0.20 +
                         cap(zt_ratio, 0.15) * 0.30 + cap(net_yi + 10, 50.0) * 0.20, 1)
        # 分级用原站的五个词：主线 / 次主线 / 普涨 / 轮动 / 退潮。
        # 阈值跟着强度分档走，70 以上才算主线 —— 这样「主线」稀缺，
        # 页面上看到「主线」两个字才知道今天真只有一条线。
        grade = ("主线" if strength >= 70 else "次主线" if strength >= 55
                 else "普涨" if strength >= 45 else "轮动" if strength >= 35 else "退潮")

        # n / zt_cnt 只统计 universe 内的票（主板·非ST·<200亿）：这两个数是给
        # 「这个板块里我能买几只、有几只涨停」看的，混进 20cm 的票就失去意义。
        uni = [x for x in rows if _in_universe(x)]
        base = uni or rows

        ladder = []
        for i, x in enumerate(base[:5]):
            code = str(x.get("f12") or "")
            st = streak_map.get(code) or 0
            if st >= 2:
                role = "高标"
            elif i == 0:
                role = "龙头"
            elif i == 1:
                role = "二线"
            elif i < 4:
                role = "跟风"
            else:
                role = "补涨"
            ladder.append({"name": x.get("f14") or code, "code": code,
                           "role": role, "pct": round(float(x.get("f3") or 0), 2),
                           "streak": st})

        kept_sets.append(codes)
        out.append({
            "name": name, "code": bk, "strength": strength, "grade": grade,
            "n": len(base),
            "zt_cnt": sum(1 for x in base
                          if float(x.get("f3") or 0) >= _limit_pct(str(x.get("f12") or ""))),
            "partial": not complete,       # 成分没取全时标注，别让 n 看着像全量
            "main_net_yi": round(net_yi, 2), "median_pct": round(med, 2), "lead_pct": round(lead_pct, 2),
            "up": b.get("f104"), "down": b.get("f105"),
            "ladder": ladder,
        })

    out.sort(key=lambda x: -x["strength"])
    if not out and diag is not None:
        diag["error"] = "板块成分全部拉取失败（%d 个候选板块）" % len(cands)
    return out


# ---------------------------------------------------------------------------
# 五、市场环境
# ---------------------------------------------------------------------------

def _streak_map(sent):
    """涨停池里的连板数 → {code: streak}。

    ★ sentiment 的 pools.zt 是**行列表**，不是 {pool: [...]} 结构 ——
    误按 dict 取 .get("pool") 会直接抛 AttributeError。
    """
    m = {}
    for row in ((sent or {}).get("pools") or {}).get("zt") or []:
        if not isinstance(row, dict):
            continue
        code = str(row.get("code") or row.get("f12") or "")
        if code:
            m[code] = int(row.get("streak") or 0)
    return m


CYCLE = [
    ("ice", "冰点"), ("start", "启动"), ("ferment", "发酵"),
    ("speed", "加速"), ("climax", "高潮"), ("ebb", "退潮"),
]
CYCLE_NAME = dict(CYCLE)

# 六阶段的操作含义（《游资交易体系——21 天突破》口径）：
# 阶段不是标签，它直接换算成仓位闸门 —— 这是本页唯一一处「环境决定仓位」的地方。
CYCLE_NOTE = {
    "ice": "涨停稀少、连板断层，冰点期只能试错首板且必须轻仓",
    "start": "赚钱效应刚回暖、连板开始接上，试错成本下降但高标尚未确认",
    "ferment": "梯队成型，是主升参与的黄金窗口，可加仓核心标的",
    "speed": "家数与高度同步扩张，只做龙头不追跟风，盯紧炸板率是否抬头",
    "climax": "家数与高度同时见顶，只减不加 —— 让别人来接，不是自己接",
    "ebb": "炸板率高于封板、连板断层，一律不接力，空仓等冰点",
}

# 阶段 → (仓位文案, 总仓上限%, 单池仓位上限%)
CYCLE_POS = {
    "ice": ("0-2 成", 2, 1),
    "start": ("2-3 成", 3, 1),
    "ferment": ("4-6 成", 6, 2),
    "speed": ("5-7 成", 7, 2),
    "climax": ("5-7 成（只减不加）", 7, 2),
    "ebb": ("0-2 成", 2, 1),
}

CYCLE_LEVEL = {"ice": "warn", "ebb": "warn", "start": "neu",
               "ferment": "good", "speed": "good", "climax": "neu"}


def _cycle_of(zt, mx, zb_rate):
    """涨停家数 + 连板高度 + 炸板率 → 情绪周期六阶段。

    ★ 阈值写死在这里而不是分散在各处，是为了让「为什么今天报发酵」可以
      拿三个数字当场复算：79 家 / 3 板 / 炸板 44% → 发酵。
      顺序也有讲究：先判冰点（家数太少谈不上任何阶段），再判退潮（炸板率
      高且家数没起来），最后才按家数+高度逐档升。
    """
    if zt < 15:
        return "ice"
    if zb_rate is not None and zb_rate >= 45 and zt < 40:
        return "ebb"
    if zt >= 100 and mx >= 5:
        return "climax"
    if zt >= 80 and mx >= 4:
        return "speed"
    if zt >= 50 and mx >= 3:
        return "ferment"
    if zt >= 30:
        return "start"
    return "ebb" if (zb_rate or 0) >= 40 else "start"


def _cycle_heat(zt, mx, seal, up_ratio):
    """情绪热度 0~100：涨停家数 30 + 连板高度 25 + 封板率 25 + 市场广度 20。"""
    h = min(zt / 100.0, 1.0) * 30.0
    h += min(mx / 6.0, 1.0) * 25.0
    h += (max(0.0, min(100.0, seal if seal is not None else 60)) / 100.0) * 25.0
    h += (max(0.0, min(100.0, up_ratio if up_ratio is not None else 50)) / 100.0) * 20.0
    return round(h, 1)


def _ladder_text(zt_rows):
    """涨停梯队文案：N板 X 家，从高到低。"""
    m = {}
    for r in zt_rows:
        if not isinstance(r, dict):
            continue
        k = int(r.get("streak") or 1)
        m[k] = m.get(k, 0) + 1
    if not m:
        return "--"
    return " · ".join("%d板 %d 家" % (k, m[k]) for k in sorted(m, reverse=True))


def _market(sent, sectors, div_cnt=0):
    """市场环境：情绪周期定位 + 仓位闸门 + 五条信号灯。

    前端 renderShort 读的是 m.phase / phase_level / read / index_summary /
    breadth / limit_summary / advice{position,focus,avoid,tactics} /
    signals[{k,v,level}] —— 字段名一个都不能改。
    """
    idx = (sent or {}).get("indices") or []
    bd = (sent or {}).get("breadth") or {}
    lim = (sent or {}).get("limit") or {}
    temp = (sent or {}).get("temperature") or {}
    zt_rows = ((sent or {}).get("pools") or {}).get("zt") or []

    zt = int(lim.get("zt") or 0)
    dt = int(lim.get("dt") or 0)
    zb = int(lim.get("zb") or 0)
    seal = lim.get("seal_rate")
    mx = int(lim.get("max_streak") or 0)
    up, down = bd.get("up"), bd.get("down")
    up_ratio = round(up / float(up + down) * 100, 1) if (up and down) else None
    zb_rate = round(zb / float(zt + zb) * 100, 1) if (zt + zb) else None

    key = _cycle_of(zt, mx, zb_rate)
    name = CYCLE_NAME[key]
    heat = _cycle_heat(zt, mx, seal, up_ratio)
    pos_txt, pos_num, pool_cap = CYCLE_POS[key]
    note = CYCLE_NOTE[key]

    idx_txt = " · ".join("%s %.2f（%.2f%%）" % (i.get("name"), i.get("price") or 0,
                                              i.get("pct") or 0) for i in idx[:3])
    if not idx_txt:
        idx_txt = "指数数据缺失"
    if up_ratio is not None:
        breadth_txt = "上涨 %d 家 / 下跌 %d 家（%s）" % (
            up, down, "涨多于跌" if up > down else ("跌多于涨" if down > up else "涨跌持平"))
    else:
        breadth_txt = "市场宽度数据缺失"

    limit_txt = "涨停 %d 家 · 最高 %d 连板 · 炸板 %d 家" % (zt, mx, zb)
    if zb_rate is not None:
        limit_txt += "（%.0f%%）" % zb_rate

    top = (sectors or [{}])[0] if sectors else {}
    if top.get("name"):
        top_txt = "%s（强度 %.1f · %s · %d 只涨停 / 主力净流入 %+.2f 亿）" % (
            top["name"], top.get("strength") or 0, top.get("grade") or "",
            top.get("zt_cnt") or 0, top.get("main_net_yi") or 0)
        focus = ("紧跟当日最强主线（%s），只做主线龙头与强势突破，宁可错过不做跟风补涨。"
                 % top["name"])
    else:
        top_txt = "板块强度数据缺失"
        focus = "板块层无有效数据，今日以情绪周期定仓位、不做板块内的接力。"

    read = "情绪处于【%s】期：涨停 %d 家、最高 %d 板、炸板率 %s，主线在 %s。%s。" % (
        name, zt, mx, ("%.0f%%" % zb_rate) if zb_rate is not None else "--", top_txt, note)

    if mx >= 4:
        avoid = ("① %d 板以上高位连板高标 —— 只作情绪标杆；② 放量破位的「伪回踩」股；"
                 "③ 无题材支撑的缩量阴跌股。" % max(4, mx - 1))
    else:
        avoid = ("① 4 板以上高位连板高标（今日最高 %d 板，尚未出现）—— 只作情绪标杆；"
                 "② 放量破位的「伪回踩」股；③ 无题材支撑的缩量阴跌股。" % mx)

    signals = [
        {"k": "情绪周期", "level": CYCLE_LEVEL.get(key, "neu"),
         "v": "【%s】涨停 %d 家、连板高度 %d：%s。" % (name, zt, mx, note)},
        {"k": "涨停梯队", "level": "good" if mx >= 3 else ("warn" if mx <= 1 else "neu"),
         "v": _ladder_text(zt_rows)},
        {"k": "主线板块", "level": "good" if top.get("strength", 0) >= 55 else "neu",
         "v": top_txt},
        {"k": "仓位闸门", "level": CYCLE_LEVEL.get(key, "neu"),
         "v": "%s —— %s期，%s" % (pos_txt, name, note)},
        {"k": "MACD 底背离", "level": "good" if div_cnt else "neu",
         "v": ("今日命中 %d 只 —— 存在超跌反弹机会" % div_cnt) if div_cnt
              else "今日 0 只命中 —— 超跌反弹条件未成立"},
    ]

    tactics = ("10:00 前不追高；优先等回踩 5 日线低吸；若高标开板炸板，立即同步降低其他仓位。"
               if key in ("ferment", "speed", "climax") else
               "只做首板试错，不碰连板接力；单票不超过 1 成，错了当天就走。")

    return {
        "phase": "情绪周期 · %s" % name,
        "phase_level": name,
        "temperature": temp.get("score"),
        "index_summary": idx_txt,
        "breadth": breadth_txt,
        "limit_summary": limit_txt,
        "cycle_key": key,
        "cycle_heat": heat,
        "read": read,
        "advice": {
            "position": pos_txt,
            "position_num": pos_num,
            "pool_pos_cap": pool_cap,
            "position_note": "%s期，%s" % (name, note),
            "focus": focus,
            "avoid": avoid,
            "tactics": tactics,
        },
        "signals": signals,
        "score": "%.1f" % heat,
    }


# ---------------------------------------------------------------------------
# 五之二、选股范围（universe）
# ---------------------------------------------------------------------------

KLINE_SCAN_MAX = 1800     # 日 K 精算的硬上限，按当日成交额降序取

# 均线形态回溯窗口（交易日）。原站 maform 的 grade_text 会挂出水芙蓉/断头铡刀这类
# 形态，且带发生日期；实测这些日期分布在最近 10 个交易日内，所以取 10。
# 太短（比如 3）会让上周刚出现的形态直接消失，太长（30）会把早就修复的结构当现行信号。
MA_FORM_LOOKBACK = 10

# 底背离「60 日价格新低」的容忍窗口（交易日）：新低必须发生在这几根之内。
# 取 5 是实测出来的 —— 卡成「当日就是最低那一根」在 4998 只里命中 0 只
# （最低点那天 DIF 几乎都还在 DEA 下方，金叉要等反弹一两天），
# 放到 5 根后同口径能出 9 只，与原站该池的 6 只同一量级。
# 放太大（20 根）会把一个月前就见底、早已反弹的票算成「当下背离」。
DIVERGENCE_NEWLOW_WINDOW = 5


def _universe(snap_map):
    """选股范围：沪深主板（60/00）· 非 ST · 总市值 < 200 亿。

    ★ 这是用户 2026-10-04 的决定 —— 不放开创业板/科创板。理由不是歧视，
      是纪律：20cm 的日常波动本身就吃掉 -5% 止损，同一套止损规则在两种
      涨跌幅制度下不等价。北交所 30cm 同理。

    返回 (全市场家数, 通过快照过滤的快照行)。
    """
    total = len(snap_map)
    pre = []
    for r in snap_map.values():
        code = str(r.get("code") or "")
        if code[:2] not in ("60", "00"):
            continue
        if engine.is_st(r.get("name") or ""):
            continue
        if not (r.get("price") or 0) > 0:
            continue
        mv = r.get("mv_yi") or 0
        # 市值未知的一律不进：放进来等于把「不知道多大」当成「足够小」，
        # 结果就是大盘股混进短线池，这比池子小一圈更糟。
        if mv <= 0 or mv >= UNI_MV_MAX:
            continue
        pre.append(r)
    return total, pre


# ---------------------------------------------------------------------------
# 六、六个池（口径对齐原站战法池）
# ---------------------------------------------------------------------------
#
# 池的 id / 顺序 / 风险等级与原站一一对应：
#   ① breakout   突破池       normal
#   ② limit_up   涨停接力池   normal
#   ③ leader     热点龙头池   normal
#   ④ gap        缺口池       normal
#   ⑤ pullback   回踩观察池   warn
#   ⑥ divergence 底背离池     warn
#
# scan 字段是从原站接口原样抄下来的条件文本 —— 它是这个池的「合同」，
# 代码必须逐条兑现；改条件时两处一起改，否则页面上写的和跑的不是一回事。

POOL_DEFS = [
    {
        "id": "breakout", "icon": "①", "name": "突破池", "risk": "normal",
        "book_ref": "《短线操盘实战技法》第 2 章 · 高低点突破买入法",
        "logic": "放量突破近 20 日高点 + MA5>MA10>MA20 多头排列，"
                 "回踩 5 日线低吸，跌破 10 日线止损。",
        "scan": "全市场主板（60/00·非ST·市值<200亿）→ 快照粗筛涨幅 1%~9.6% 且换手≥1.5% → "
                "日K 精算：收盘≥20日最高 ∧ MA5>MA10>MA20 ∧ 量比≥1.15 ∧ 量比5/20≤4 ∧ 250日位置≤92%",
    },
    {
        "id": "limit_up", "icon": "②", "name": "涨停接力池", "risk": "normal",
        "book_ref": "《短线操盘实战技法》第 6 章 · 强势涨停突破买入法",
        "logic": "今日涨停且封板质量合格（未反复炸板、封成比≥1%）；"
                 "次日竞价高开 0~3% 接力，开板即走。",
        "scan": "东财涨停池（getTopicZTPool）→ 主板过滤 → 炸板次数<2 ∧ 封单额/流通市值≥1% "
                "→ 按 连板数+封成比+换手 排序",
    },
    {
        "id": "leader", "icon": "③", "name": "热点龙头池", "risk": "normal",
        "book_ref": "《游资全景看盘之板块轮动擒龙头》· 主线辨识 + 龙头溢价",
        "logic": "只在当日强势板块（前 6）里取板块内梯队前 3 的票；"
                 "4 板以上或梯队第 3 位之后只作情绪标杆。",
        "scan": "板块层强度排名前 6 的板块 → 取各板块梯队 ladder 前 3 位 "
                "→ 按 板块排名+梯队位次+连板高度 排序",
    },
    {
        "id": "gap", "icon": "④", "name": "缺口池", "risk": "normal",
        "book_ref": "《短线操盘实战技法》第 5 章 · 缺口追涨与支撑买入法",
        "logic": "向上跳空缺口（今日最低>昨日最高，或近 5 日内出现且至今未回补）+ 量能配合；"
                 "缺口下沿即止损锚点，回补即走。",
        "scan": "日K 精算：（今日最低>昨日最高 ∨ 近5日有向上缺口且收盘仍>缺口下沿）"
                "∧ 缺口幅度≥0.2% ∧ 5日/20日量比≥0.7 ∧ 量比≥0.9 ∧ 涨幅<9.6% ∧ 250日位置≤94% "
                "—— 今日缺口权重更高；放宽到「近5日」是因为严格当日缺口全市场每天仅个位数，池子会常年为空",
    },
    {
        "id": "pullback", "icon": "⑤", "name": "回踩观察池", "risk": "warn",
        "book_ref": "《短线操盘实战技法》第 8 章 · 指标均线止损法",
        "logic": "强势股回踩 MA10/MA20：缩量回踩可低吸；放量下跌=破位，"
                 "只列观察并打「破位待确认」。",
        "scan": "日K 精算：MA5>MA10>MA20 ∧ 收盘>MA20 ∧ 距MA20在 -1%~+11% ∧ 近20日涨停≥1次 "
                "∧ 20日涨幅≥5% → 再按量能分流为「缩量回踩」与「放量破位」",
    },
    {
        "id": "divergence", "icon": "⑥", "name": "底背离池", "risk": "warn",
        "book_ref": "《短线操盘实战技法》第 3 章 · 底背离双突破买入法",
        "logic": "MACD 底背离（价格创 60 日新低但 DIF 未创新低）+ 缩量地量 + DIF 上穿 DEA；"
                 "属反弹型机会，仓位须低于趋势买入。",
        "scan": "日K 精算：60日价格新低而 DIF 未新低 ∧ 量比<0.75（缩量地量）∧ DIF>DEA ∧ 250日位置≤45%",
    },
]

POOL_ORDER = {d["id"]: i for i, d in enumerate(POOL_DEFS)}
POOL_BY_ID = {d["id"]: d for d in POOL_DEFS}
POOL_MAX = 40          # 单池最多展示多少只（再多前端也读不完，也会拖慢体检/缠论）


def _hi_ex_today(rows, n=20):
    """不含当日的 n 日最高价。

    ★ 不能直接用 engine.features 的 hi20/hi60：那两个**含当日**，拿来做
      「突破」判定等于让当日自己证明自己 —— 收盘永远 ≤ 当日最高，
      `close >= hi20` 只有「收在当日最高价」时才成立，池子会常年空着。
    """
    seg = rows[-(n + 1):-1]
    return max(r[3] for r in seg) if seg else 0.0


def _vol20_prev(rows, n=20):
    """不含当日的 n 日均量（features 的 vol_ma20 含当日，会把「缩量」判轻）。"""
    seg = rows[-(n + 1):-1]
    return sum(r[5] for r in seg) / float(len(seg)) if seg else 0.0


def _zt_days(rows, n=20, lim=9.8):
    """近 n 日内涨停的天数（按该票所属板的涨停阈值判定）。"""
    cnt = 0
    seg = rows[-(n + 1):]
    for i in range(1, len(seg)):
        pc = seg[i - 1][2]
        if pc > 0 and (seg[i][2] / pc - 1) * 100.0 >= lim:
            cnt += 1
    return cnt


def _pos52(f):
    v = f.get("pos52")
    return 100.0 if v is None else float(v)


def _ma_series(rows, n):
    """逐日 n 日均线，与 rows 等长；不足 n 根的位置为 None。

    ★ 为什么不用 engine.features 里的 ma5/ma10/ma20：那几个只给「当日」一个值，
      而形态判定要判断「昨天在三线下方、今天收在三线上方」，必须要昨日的均线。
      自己滑窗算一遍，避免为了一个辅助字段去改 features 的口径。
    """
    out, s = [], 0.0
    for i, r in enumerate(rows):
        s += float(r[2])
        if i >= n:
            s -= float(rows[i - n][2])
        out.append((s / n) if (i + 1) >= n else None)
    return out


def _maform(f, rows):
    """均线形态（对齐原站 /api/short 的 maform：出水芙蓉 / 多方炮 / 断头铡刀）。

    ★ 原站 maform 只有六个字段 {score, grade, grade_text, brief, bear, veto}，
      前端只消费 brief / bear / hits；实测出现过的形态名只有三种 ——
      出水芙蓉、多方炮、断头铡刀，没命中就是「无均线形态」。
      早先本地自拟的「四线多头 / 三线多头 / 空头排列」是自造档位：页面上会
      出现原站根本没有的标签（`if (x.maform.brief)` 会把它当命中渲染出来），
      所以整块换成原站三式。

    ★ grade_text 必须带形态发生日（原站形如「出水芙蓉（2026-11-02）」）：
      不带日期的话，10 个交易日前的形态和昨天的形态在页面上长得一样新。

    ★ 取「回溯窗口内**最新**的一次命中」而不是优先级最高的那次：
      2026-10-30 断头铡刀、2026-11-02 又出水芙蓉，这时候还挂断头铡刀
      就是把已经修复的结构当成现行风险，方向会反。同一天多命中才按风险优先。

    ★ score 原站同形态有 95/90/85 的浮动（由量能与位置调节），本地给固定档，
      不去反推那套调节权重 —— 宁可差几个点，也不编一套拟合出来的公式。
    """
    base = {"score": 50, "grade": "none", "grade_text": "无均线形态",
            "brief": "", "bear": False, "veto": None, "hits": []}
    if len(rows) < 25:
        return base
    m5, m10, m20 = _ma_series(rows, 5), _ma_series(rows, 10), _ma_series(rows, 20)
    n = len(rows)
    start = max(20, n - MA_FORM_LOOKBACK)
    for i in range(n - 1, start - 1, -1):
        a5, a10, a20 = m5[i], m10[i], m20[i]
        p5, p10, p20 = m5[i - 1], m10[i - 1], m20[i - 1]
        if not (a5 and a10 and a20 and p5 and p10 and p20):
            continue
        d = str(rows[i][0])
        op = float(rows[i][1])
        cl = float(rows[i][2])
        prev = float(rows[i - 1][2])
        if not prev:
            continue
        pct = (cl / prev - 1.0) * 100.0
        # 当日量比：当日量 / 前 5 日均量（不含当日，否则放量会被自己摊薄）
        if i >= 5:
            v5 = sum(float(rows[j][5]) for j in range(i - 5, i)) / 5.0
            vr = (float(rows[i][5]) / v5) if v5 else 0.0
        else:
            vr = 0.0

        # ① 断头铡刀：昨收在多头排列的三线上方，当日一阴同时切断 5/10/20 日线且放量
        if prev > p5 > p10 > p20 and cl < a5 and cl < a10 and cl < a20 \
                and pct <= -4.0 and vr >= 1.3:
            # 原站 veto 带「60 日涨幅 N% 高位」——低位断头铡刀往往只是回调，
            # 涨了一大波之后断头才是趋势终结，这个高度信息不能省。
            if i >= 60:
                c60 = (cl / float(rows[i - 60][2]) - 1.0) * 100.0
                hi_txt = " · 60 日涨幅 %.0f%%%s" % (c60, " 高位" if c60 >= 30 else "")
            else:
                hi_txt = ""
            return {"score": 25, "grade": "bear", "grade_text": "断头铡刀（%s）" % d,
                    "brief": "断头铡刀", "bear": True,
                    "veto": "一阴断三线（%.1f%%）· 量比 %.2f 倍%s，趋势终结级离场信号"
                            % (pct, vr, hi_txt),
                    "hits": [{"name": "断头铡刀", "date": d, "strength": 90}]}

        # ② 出水芙蓉：昨收在三线下方，当日放量阳线一举收上 5/10/20 日线
        if prev < p5 and prev < p10 and prev < p20 \
                and cl > op and cl > a5 and cl > a10 and cl > a20 and vr >= 1.2:
            return {"score": 90, "grade": "bull", "grade_text": "出水芙蓉（%s）" % d,
                    "brief": "出水芙蓉", "bear": False, "veto": None,
                    "hits": [{"name": "出水芙蓉", "date": d, "strength": 85}]}

        # ③ 多方炮：两阳夹一阴，第三根放量阳线收复第一根阳线的收盘
        if i >= 2:
            o1, c1 = float(rows[i - 2][1]), float(rows[i - 2][2])
            o2, c2 = float(rows[i - 1][1]), float(rows[i - 1][2])
            if c1 > o1 and c2 < o2 and o2 < c1 \
                    and cl > op and cl > c1 and vr >= 1.2:
                return {"score": 75, "grade": "bull", "grade_text": "多方炮（%s）" % d,
                        "brief": "多方炮", "bear": False, "veto": None,
                        "hits": [{"name": "多方炮", "date": d, "strength": 75}]}
    return base


def _exit_level(f, stop, defense):
    """三档离场分级（前端按 L0/L1/L2 配红/橙/红描边）。

    L0 灾难：当日暴跌，不等反弹直接走；
    L2 清仓：收盘破了防守线（摆动低点结构位），趋势拐点被证伪；
    L1 减仓：收盘破了 MA10 操盘线，或量价出现警示。
    优先级从 L0 到 L1 递减 —— 同时满足时按最严重的那档报。
    """
    cl = float(f.get("close") or 0)
    pct = f.get("pct") or 0
    ma10 = f.get("ma10")
    if pct <= -9.0:
        return {"level": "L0", "code": "灾难离场", "actions": ["开盘直接离场，不等反弹"],
                "note": "当日跌幅 %.1f%%，属灾难性下跌，任何计划作废" % pct}
    if defense and cl < float(defense):
        return {"level": "L2", "code": "结构清仓",
                "actions": ["收盘前清仓", "不做次日博弈"],
                "note": "收盘 %.2f 已跌破防守线 %.2f，趋势拐点被破" % (cl, float(defense))}
    if stop and cl < float(stop):
        return {"level": "L2", "code": "止损清仓",
                "actions": ["按计划止损", "止损后 3 日内不回补"],
                "note": "收盘 %.2f 已跌破止损位 %.2f" % (cl, float(stop))}
    if ma10 and cl < float(ma10):
        return {"level": "L1", "code": "减仓一半",
                "actions": ["次日冲高减半仓", "剩余仓位以 MA20 为线"],
                "note": "收盘 %.2f 已在 MA10（%.2f）之下，操盘线走坏" % (cl, float(ma10))}
    return {"level": "正常", "code": "", "actions": [], "note": "无离场信号，按原计划持有"}


RISK_PCT = 2.0        # 单笔风险占总资金 2%（固定风险法的分子）
RISK_CAP = 15.0       # 单票风险仓位上限（%）
CAPITAL = 100000.0    # 展示用的名义资金，仅用于把百分比翻成「买多少万」


def _risk_pos(close, stop):
    """固定风险法倒推仓位：单笔风险 2% ÷ 止损距离，再封顶 15%。

    「止损越宽仓位越轻」是这套公式的全部意义 —— 止损 4% 的票可以上 15%，
    止损 10% 的票只能上 2/10=20%…但再宽就不该开，所以 15% 封顶。
    """
    dist = (close - stop) / close * 100.0 if close else 0.0
    if dist <= 0:
        return {"pct": 0.0, "basis": "止损位不低于现价，仓位无法倒推", "risk_per_share": 0.0,
                "note": "价位异常，本票不给仓位建议"}
    raw = RISK_PCT / dist * 100.0
    pct = round(min(raw, RISK_CAP), 1)
    cap_txt = "（封顶 %.0f%%）" % RISK_CAP if raw > RISK_CAP else ""
    return {
        "pct": pct,
        "basis": "单笔风险 %.0f%% ÷ 止损距离 %.1f%% = %.0f%%%s" % (RISK_PCT, dist, raw, cap_txt),
        "risk_per_share": round(close - stop, 2),
        "note": "10 万资金此票最多买 %.1f 万（占总资金 %.0f%%）" % (CAPITAL * pct / 100.0 / 1e4, pct),
    }


def _defense(f, rows):
    """防守线 = 最近一个摆动低点 × 0.995。

    ★ valid=False 的两种情况必须显式给出来，不能悄悄返回 null：
      1) 没有摆动低点（K 线不足）→ price=None；
      2) 最近摆动低点**已经高于现价**（说明趋势在上移而价格掉下来了，
         这个低点不再是支撑）→ price=None + note 说明，前端会印「防守线失效」。
    """
    lows = f.get("sw_lows") or []
    if not lows:
        return {"price": None, "date": f.get("date"), "basis": "K 线不足，未形成摆动低点",
                "valid": False, "note": "防守线失效（结构位缺失），等新摆动低点形成"}
    idx, px = lows[-1]
    date = rows[idx][0] if 0 <= idx < len(rows) else f.get("date")
    level = round(px * 0.995, 2)
    close = float(f.get("close") or 0)
    if level >= close:
        return {"price": None, "date": date,
                "basis": "摆动低点 %.2f（%s）已高于现价 %.2f" % (px, date, close),
                "valid": False, "note": "防守线失效（趋势反转），等新摆动低点形成"}
    return {"price": level, "date": date,
            "basis": "摆动低点 %.2f（%s）× 0.995" % (px, date),
            "valid": True, "note": "跌破 %.2f 即趋势拐点被破，离场" % level}


def _round(v, n=2):
    try:
        return round(float(v), n)
    except (TypeError, ValueError):
        return None


def _atr(f):
    a = f.get("atr")
    cl = float(f.get("close") or 0)
    return float(a) if a else cl * 0.02


def _plan_base(f, rows, buy_lo, buy_hi, buy_label, stop, sell, position, hold):
    """六个池共用的计划骨架：价位全由传入值决定，这里只负责拼装与兜底。"""
    close = float(f.get("close") or 0)
    if close <= 0:
        return {}
    atr = _atr(f)
    # 目标位统一用 ATR 递推，并用前高做地板 —— 目标低于现价 0.5% 等于没设目标
    t1 = max(close + 1.5 * atr, float(f.get("hi20") or 0))
    if t1 <= close * 1.005:
        t1 = close + 1.5 * atr
    t1 = min(t1, close * 1.25)
    t2 = max(t1 + 1.5 * atr, float(f.get("hi60") or 0))
    t2 = min(max(t2, t1 + atr), close * 1.50)
    # 止损必须落在买点下沿之下：龙头池曾给出「买 2.42~2.65、止损 2.46」这种
    # 计划 —— 买点区间里就已经触发止损，等于没给计划。
    stop = min(float(stop), close * 0.995, float(buy_lo) * 0.995)
    dz = "（%s）" % buy_label if buy_label else ""
    return {
        "buy_zone": "%.2f ~ %.2f%s" % (round(buy_lo, 2), round(buy_hi, 2), dz),
        "stop_loss": _round(stop),
        "stop_pct": _round((stop / close - 1) * 100, 1),
        "target1": _round(t1),
        "target2": _round(t2),
        "position": position,
        "hold": hold,
        "sell_signal": sell,
    }


def _finish_plan(plan, f, rows):
    """补齐 plan 的纪律层（defense / risk_pos / exit_level）。

    ★ 前端 renderShortStock 只读 buy_zone/stop_loss/target1/target2/position/
      hold/sell_signal；但罗盘页的 rscPickCard 会读 defense/risk_pos/exit_level。
      同一只票在两处出现时字段必须同形，所以这里一次补齐，不留半套。
    """
    if not plan:
        return plan
    close = float(f.get("close") or 0)
    defense = _defense(f, rows)
    plan["defense"] = defense
    plan["risk_pos"] = _risk_pos(close, float(plan.get("stop_loss") or close * 0.92))
    plan["exit_level"] = _exit_level(f, plan.get("stop_loss"), defense.get("price"))
    return plan


def _stage_hold(f, rows):
    """6 池统一的持仓周期基准：趋势型 1-5 日，反弹型 1-3 日。"""
    return (f.get("pct") or 0)


# ---- 逐池判定 ---------------------------------------------------------------
# 每个 _hit_* 返回 None（不命中）或 {tags, reason, board, hi_risk, plan}
# 条件必须与 POOL_DEFS[pid]["scan"] 逐条对应。

def _hit_breakout(f, rows, ind):
    pct = f.get("pct") or 0
    if not (1.0 <= pct <= 9.6):
        return None
    ma5, ma10, ma20 = f.get("ma5"), f.get("ma10"), f.get("ma20")
    if not (ma5 and ma10 and ma20 and ma5 > ma10 > ma20):
        return None
    hi20 = _hi_ex_today(rows, 20)
    cl = float(f["close"])
    if not hi20 or cl < hi20 * 0.999:
        return None
    vr = f.get("vol_ratio") or 0
    if vr < 1.15:
        return None
    v5, v20 = f.get("vol_ma5") or 0, _vol20_prev(rows, 20)
    if not v5 or not v20 or (v5 / v20) > 4:
        return None
    if _pos52(f) > 92:
        return None

    brk = (cl / hi20 - 1) * 100
    tags = ["放量突破 20 日新高 +%.2f%%" % brk, "MA5>MA10>MA20 多头排列", "量比 %.2f" % vr]
    reason = ("%s：今日收 %.2f 元（%+.2f%%），放量突破近 20 日最高 %.2f 元，"
              "MA5/MA10/MA20（%.2f/%.2f/%.2f）多头排列，量能较前 5 日均量放大至 %.2f 倍，"
              "属标准的高低点突破形态。") % (ind, cl, pct, hi20, ma5, ma10, ma20, vr)
    # 买点：回踩 5 日线，不追现价；止损：跌破 10 日线
    buy_lo, buy_hi = ma5, max(ma5, cl * 0.995)
    stop = min(ma10 * 0.99, cl * 0.92)
    plan = _plan_base(
        f, rows, buy_lo, buy_hi, "回踩 5 日线 %.2f 不破" % ma5, stop,
        "跌破 10 日线（%.2f）或冲高回落 3%% 即走；连续 3 日滞涨减半仓" % ma10,
        "2 成", "1-5 个交易日")
    return {"tags": tags, "reason": reason,
            "board": "%s · 距 250 日高点 %.0f%%" % (ind, _dist250(f)),
            "hi_risk": False, "plan": plan, "_sort": brk}


def _dist250(f):
    hi = f.get("hi250")
    cl = float(f.get("close") or 0)
    if not hi or not cl:
        return 0.0
    return max(0.0, (hi - cl) / hi * 100.0)


def _hit_gap(f, rows, ind):
    if len(rows) < 21:
        return None
    pct = f.get("pct") or 0
    if pct >= 9.6:
        return None
    cl = float(f["close"])
    v5, v20 = f.get("vol_ma5") or 0, _vol20_prev(rows, 20)
    if not v20 or (v5 / v20) < 0.7:
        return None
    if (f.get("vol_ratio") or 0) < 0.9:
        return None
    if _pos52(f) > 94:
        return None

    # 逐日找向上缺口：今日优先（权重高），再看前 4 日是否仍未回补
    best = None
    for back in range(1, 6):
        i = len(rows) - back
        if i < 1:
            break
        lo_i, hi_prev = rows[i][4], rows[i - 1][3]
        if hi_prev <= 0 or lo_i <= hi_prev:
            continue
        gp = (lo_i / hi_prev - 1) * 100.0
        if gp < 0.2:
            continue
        if cl <= hi_prev:          # 缺口已被回补
            continue
        if best is None:
            best = {"back": back, "low": lo_i, "prev_hi": hi_prev, "gap": gp,
                    "date": rows[i][0]}
    if not best:
        return None

    today = best["back"] == 1
    tags = [("今日向上跳空 %.2f%% 未回补" % best["gap"]) if today
            else ("%s 向上跳空 %.2f%% 未回补" % (best["date"][-5:], best["gap"])),
            "缺口下沿 %.2f 元即支撑" % best["prev_hi"],
            "量比 %.2f" % (f.get("vol_ratio") or 0),
            "5日/20日量比 %.2f" % (v5 / v20 if v20 else 0)]
    if today:
        reason = ("%s：今日最低 %.2f 元高于昨日最高 %.2f 元，形成缺口支撑；"
                  "量能 %.2f 倍于近期均量。缺口下沿 %.2f 元是本形态的止损锚点，"
                  "一旦被回补即视为支撑失效。") % (ind, best["low"], best["prev_hi"],
                                              f.get("vol_ratio") or 0, best["prev_hi"])
    else:
        reason = ("%s：%s 向上跳空 %.2f%% 留下缺口（%.2f→%.2f），至今未回补，"
                  "今日收 %.2f 元仍在缺口上方，缺口下沿即支撑。") % (
            ind, best["date"], best["gap"], best["prev_hi"], best["low"], cl)
    # 买点：缺口下沿 → 今日最低（跌回缺口里接）；止损：缺口下沿 × 0.995
    buy_lo, buy_hi = best["prev_hi"], max(best["low"], best["prev_hi"])
    stop = best["prev_hi"] * 0.995
    plan = _plan_base(
        f, rows, buy_lo, buy_hi, "缺口上沿至今日低点", stop,
        "缺口被回补（收盘跌破 %.2f）即离场，缺口支撑失效" % best["prev_hi"],
        "≤2 成", "1-5 个交易日")
    return {"tags": tags[:4], "reason": reason,
            "board": "%s · %s" % (ind, "今日缺口" if today else "近 5 日缺口"),
            "hi_risk": False, "plan": plan, "_sort": (1 if today else 0) * 100 + best["gap"]}


def _hit_pullback(f, rows, ind):
    ma5, ma10, ma20 = f.get("ma5"), f.get("ma10"), f.get("ma20")
    if not (ma5 and ma10 and ma20 and ma5 > ma10 > ma20):
        return None
    cl = float(f["close"])
    if cl <= ma20:
        return None
    dev = (cl / ma20 - 1) * 100.0
    if not (-1.0 <= dev <= 11.0):
        return None
    lim = _limit_pct(str(f["code"]))
    zt = _zt_days(rows, 20, lim)
    if zt < 1:
        return None
    chg20 = f.get("chg20") or 0
    if chg20 < 5:
        return None

    v20 = _vol20_prev(rows, 20)
    v_today = float(f.get("vol") or 0)
    shrink = bool(v20) and v_today < v20 * 0.85
    vr = f.get("vol_ratio") or 0
    tags = ["近 20 日 %d 次涨停" % zt,
            "MA5/10/20 = %.2f/%.2f/%.2f" % (ma5, ma10, ma20)]
    if shrink:
        tags.append("量比 %.2f（缩量回踩）" % vr)
        reason = ("%s：近 20 日 %d 次涨停、20 日涨幅 %+.1f%%，今日缩量回踩 %+.2f%%"
                  "（量比 %.2f），MA5>MA10>MA20 多头排列未破，收盘仍在 MA20（%.2f）上方 "
                  "—— 属健康回踩，可低吸。") % (ind, zt, chg20, f.get("pct") or 0, vr, ma20)
    else:
        tags.append("量比 %.2f（放量下跌）" % vr)
        reason = ("%s：近 20 日 %d 次涨停、20 日涨幅 %+.1f%%，今日放量下跌 %+.2f%%"
                  "（量比 %.2f）且量能高于 20 日均量，虽仍在 MA20（%.2f）上方，"
                  "但已属「放量破位」形态 —— 只列观察，等缩量止跌再谈低吸。") % (
            ind, zt, chg20, f.get("pct") or 0, vr, ma20)
    # 买点：MA10 附近缩量止跌；止损：MA20 × 0.985（均线止损法）
    buy_lo, buy_hi = min(ma10, cl) * 0.995, max(ma10, cl) * 1.01
    stop = ma20 * 0.985
    plan = _plan_base(
        f, rows, buy_lo, buy_hi, "MA10 %.2f 附近缩量止跌" % ma10, stop,
        "放量收破 MA20（%.2f）即离场" % ma20,
        "≤2 成", "1-5 个交易日")
    return {"tags": tags, "reason": reason,
            "board": "%s · 回踩 MA20" % ind,
            "hi_risk": False, "warn": (not shrink), "plan": plan,
            "_sort": zt * 10 + chg20}


def _hit_divergence(f, rows, ind):
    """底背离：价格创 60 日新低，而 DIF 没有跟着创新低。

    ★ 口径按原站 /api/short 的 scan 文本：「60日价格新低而 DIF 未新低 ∧ 量比<0.75
      ∧ DIF>DEA ∧ 250日位置≤45%」。但「新低」不能卡成「今天就是最低那一根」：
      实测「当日收盘 = 60 日最低收盘 ∧ DIF>DEA ∧ 缩量 ∧ pos52≤45」在 4998 只里
      命中 **0** 只 —— 因为跌到最低点那天 DIF 基本都在 DEA 下方，DIF 上穿 DEA
      要等反弹一两天才发生。原站自己能出 6 只，说明它的「新低」允许发生在
      最近几天内。所以这里把新低限定在最近 DIVERGENCE_NEWLOW_WINDOW 根内
      （同口径实测近 5 日内新低 + 同条件 = 9 只，与原站 6 只同一量级）。

    ★ 背离仍是「两段」比较：新低那一根之前的部分里找前一个最低收盘，
      要求新低处的 DIF 高于前低处的 DIF —— 价格创新低而动能没创新低。
      上ー版把窗口放宽成「前后各 30 根」是自创条件（前半段的低点可能在 55 根前），
      命中的票价格新低已经发生一个月，跟「刚创新低」不是一回事。

    ★ 当日仍需 DIF>DEA：背离只有被金叉确认才算成立，否则是下跌中继。
    """
    closes = [r[2] for r in rows]
    if len(closes) < 65:
        return None
    seg = closes[-60:]
    lo_i = min(range(60), key=lambda k: seg[k])
    # 新低必须刚发生：距今超过窗口就是「老底」，不是当下的背离机会
    if (59 - lo_i) > DIVERGENCE_NEWLOW_WINDOW - 1 or lo_i == 0:
        return None
    prev = seg[:lo_i]
    lo_e = min(range(len(prev)), key=lambda k: prev[k])
    difs = engine.macd(closes)[0][-60:]
    d_l, d_e = difs[lo_i], difs[lo_e]
    if d_e is None or d_l is None or d_l <= d_e:      # DIF 同步新低 → 无背离
        return None
    dif, dea = f.get("dif"), f.get("dea")
    if dif is None or dea is None or dif <= dea:      # 背离尚未被金叉确认
        return None
    vr = f.get("vol_ratio") or 1.0
    if vr >= 0.75:                                   # 要求缩量地量
        return None
    if _pos52(f) > 45:                               # 只做低位，不做高位假背离
        return None

    cl = float(f["close"])
    bars_from_low = 59 - lo_i
    tags = ["MACD 底背离（DIF %.3f > DEA %.3f）" % (dif, dea),
            "缩量地量（量比 %.2f）" % vr,
            "60 日新低在 %d 根前" % bars_from_low,
            "250 日位置 %.0f%%" % _pos52(f)]
    reason = ("%s：股价创 60 日新低（%.2f → %.2f）但 MACD DIF 未同步创新低"
              "（%.3f → %.3f），形成底背离；今日缩量（量比 %.2f）且 DIF 上穿 DEA，"
              "超跌反弹条件成立。注意：底背离属反弹型机会，仓位须低于趋势买入。") % (
        ind, seg[lo_e], seg[lo_i], d_e, d_l, vr)
    # 买点：不追高，等缩量回踩到现价下方 4%；止损：现价 -8%（20 日最低通常太远）
    buy_lo, buy_hi = cl * 0.96, cl
    stop = cl * 0.92
    plan = _plan_base(
        f, rows, buy_lo, buy_hi, "不追高，等缩量回踩", stop,
        "跌破 20 日最低（%.2f）或 DIF 再次死叉即离场" % (f.get("lo20") or 0),
        "1 成（反弹型，勿重仓）", "1-3 个交易日")
    return {"tags": tags, "reason": reason,
            "board": "%s · MACD 底背离" % ind,
            "hi_risk": True, "plan": plan, "_sort": -bars_from_low}


# 池 id → 判定函数。eval_scan 按这个表派发，缺一个键就是「该池 0 只命中」。
_HIT = {
    "breakout": _hit_breakout,
    "gap": _hit_gap,
    "pullback": _hit_pullback,
    "divergence": _hit_divergence,
}


# ---- 涨停接力池 / 热点龙头池：候选来自外部数据源，不走 K 线扫描 -------------

def _hit_limit_up(row, f, ind):
    """涨停接力：封板质量合格才接力。

    row 来自东财涨停池（_pool_row 的形状），f 是该票的 K 线特征。
    两条硬门槛：炸板次数 <2（反复开板说明封不住）、封成比 ≥1%
    （封单额 / 流通市值，衡量封板决心而不是绝对金额）。
    """
    if int(row.get("open_times") or 0) >= 2:
        return None
    seal_wan = float(row.get("seal_wan") or 0)          # 万元
    nmc = float((row.get("nmc_yi") or 0))               # 亿元
    if nmc <= 0 or seal_wan <= 0:
        return None
    seal_ratio = seal_wan / (nmc * 1e4) * 100.0          # 万元 / 万元 = 比例
    if seal_ratio < 1.0:
        return None
    streak = int(row.get("streak") or 1)
    tags = ["%d 连板" % streak,
            "封单 %.2f 亿（占流通市值 %.1f%%）" % (seal_wan / 1e4, seal_ratio),
            "换手 %.1f%%" % float(row.get("turnover") or 0)]
    reason = ("%s：%d 板· %s，封单 %.2f 亿、封成比 %.1f%%、换手 %.1f%%。"
              "封成比越高说明封板越坚决；次日竞价高开 0~3%% 为健康区间，可接力。") % (
        ind, streak, "未炸板" if not int(row.get("open_times") or 0)
        else "炸板 %d 次" % int(row.get("open_times") or 0),
        seal_wan / 1e4, seal_ratio, float(row.get("turnover") or 0))
    cl = float(f.get("close") or row.get("price") or 0)
    # 买点：次日竞价 -1%~+3%；止损：涨停前一日收盘 ×0.995（涨停的起点不能被跌穿）
    buy_lo, buy_hi = cl * 0.96, cl
    prev_close = float(f.get("prev_close") or cl / 1.1)
    stop = min(prev_close * 0.995, cl * 0.93)
    plan = _plan_base(
        f, [], buy_lo, buy_hi, "次日竞价高开 -1%~+3% 区间", stop,
        "开板不封 / 冲高回落 3% / 低开 3% 不翻红，任一条即离场",
        "1 成", "1-3 个交易日")
    return {"tags": tags, "reason": reason,
            "board": "%d 板·%s" % (streak, "未炸板" if not int(row.get("open_times") or 0)
                                  else "炸板 %d 次" % int(row.get("open_times") or 0)),
            "hi_risk": streak >= 4, "plan": plan,
            "_sort": streak * 1000 + seal_ratio * 10 + float(row.get("turnover") or 0)}


def _hit_leader(sector, rank, lad, f, ind):
    """热点龙头：只做主线板块里的梯队长，4 板以上只当情绪标杆。"""
    streak = int(lad.get("streak") or 0)
    if streak >= 4:
        return None
    sname = sector.get("name") or ""
    grade = sector.get("grade") or ""
    strength = sector.get("strength") or 0
    pos = rank + 1
    tags = ["%s · %s（强度 %.1f）" % (sname, grade, strength),
            "板块内梯队第 %d 位" % pos]
    if streak:
        tags.append("%d 连板" % streak)
    reason = ("【%s】板块（%s，强度 %.1f，全市场涨停 %d 家 / 主力净流入 %+.2f 亿）内"
              "排名第 %d 的票。按《游资全景看盘》口径，只做主线板块的龙头，"
              "不做跟风补涨。") % (sname, grade, strength, sector.get("zt_cnt") or 0,
                                sector.get("main_net_yi") or 0, pos)
    cl = float(f.get("close") or 0)
    ma5 = f.get("ma5") or cl
    buy_lo, buy_hi = min(ma5, cl) * 0.99, cl
    stop = min(float(f.get("ma10") or cl * 0.93) * 0.99, cl * 0.93)
    plan = _plan_base(
        f, [], buy_lo, buy_hi, "次日竞价不高开超 3%", stop,
        "开板不封即离场；若同板块跟风股集体走弱则减仓",
        "≤2 成", "1-3 个交易日")
    return {"tags": tags, "reason": reason,
            "board": ("%d 板 · %s" % (streak, sname)) if streak else sname,
            "hi_risk": False, "plan": plan,
            "_sort": (6 - sector.get("_rank", 9)) * 100 - pos * 10 + streak}


# ---- 组装单只票的对外结构 ---------------------------------------------------

def _stock_of(f, rows, ind, hit, pool_id):
    """把「特征 + 池判定结果」拼成前端要的完整结构。

    ★ 前端 renderShortStock 直接读 s.plan.buy_zone / s.tags / s.reason /
      s.board / s.high_risk，缺一个就印 undefined；rscPickCard 还额外读
      s.maform.brief / s.plan.defense / s.plan.risk_pos / s.plan.exit_level。
      这里一次给全，字段形状与原站对齐。
    """
    code = str(f["code"])
    plan = _finish_plan(hit.get("plan") or {}, f, rows)
    return {
        "code": code,
        "setcode": "1" if code[:1] == "6" else "0",
        "secid": engine.secid_of_code(code),
        "name": f.get("name") or "",
        "price": _round(f.get("close")),
        "pct": _round(f.get("pct")),
        "board": hit.get("board") or ind,
        "industry": ind,
        "tags": list(hit.get("tags") or [])[:4],
        "reason": hit.get("reason") or "",
        "high_risk": bool(hit.get("hi_risk")),
        "risk_warn": bool(hit.get("warn")),
        "plan": plan,
        "gate": None,
        "mw_type": None, "mw_score": None,
        "maform": _maform(f, rows),
        "pool_id": pool_id,
        "_sort": hit.get("_sort") or 0,
    }


# ---- 候选收集 ---------------------------------------------------------------

def _ctx(code, snap_map):
    """取一只票的 K 线与特征；K 线缺失返回 None。"""
    rows = engine.kline_get(code)
    if not rows or len(rows) < 30:
        return None
    f = engine.features(code, rows, snap_map.get(code))
    if not f:
        return None
    return rows, f


def _industry_map(codes):
    """批量取行业名（f100，如「影视院线」「半导体」）。一页 80 个 secid。

    不复用板块成分表：成分只覆盖候选的十几个板块，池子其余票会退化成
    「行业 --」，实测能缺三分之一。f100 是东财给每只票的细分行业，一次拿全。
    """
    out = {}
    codes = [c for c in dict.fromkeys(codes) if c]
    for i in range(0, len(codes), 80):
        secids = ",".join(("1." if c.startswith("6") else "0.") + c
                          for c in codes[i:i + 80])
        for _ in range(2):
            try:
                d = engine.http_json(_ULIST_API +
                                     "?fltt=2&invt=2&fields=f12,f14,f100&secids=" + secids,
                                     referer=_QT_REF, timeout=15)
            except Exception:
                d = None
            rows = ((d or {}).get("data") or {}).get("diff") or []
            if rows:
                for x in rows:
                    if x.get("f100"):
                        out[str(x.get("f12"))] = x["f100"]
                break
            time.sleep(0.4)
    return out


def _vp_counts(stocks):
    c = {"confirm": 0, "warn": 0, "mid": 0, "none": 0}
    for s in stocks:
        g = ((s.get("vp") or {}).get("gate")) or "none"
        c["warn" if g == "block" else ("confirm" if g == "pass" else "mid")] += 1
    return c


def _pool(id_, **kw):
    d = {"id": id_, "icon": "·", "name": id_, "book_ref": "", "logic": "",
         "scan": "", "scanned": 0, "risk": "normal", "empty_msg": "今日无符合条件标的",
         "stocks": [], "vp_counts": {"confirm": 0, "warn": 0, "mid": 0, "none": 0}}
    d.update(kw)
    return d


def _pool_meta(pid):
    d = POOL_BY_ID[pid]
    return {"icon": d["icon"], "name": d["name"], "book_ref": d["book_ref"],
            "logic": d["logic"], "scan": d["scan"], "risk": d["risk"]}


LHB_TAIL = 12     # 龙虎榜异动标的并入池尾展示的数量上限


def _collect(sent, sectors, snap_map, lhb_items=None):
    """六个池的收集与组装。

    分三步，顺序不能换：
      1) 快照层定 universe（主板 · 非ST · <200亿），并按成交额截断出日K精算集合；
      2) 六个池各自判定 —— 突破/缺口/回踩/底背离走日K扫描，
         涨停接力走东财涨停池，热点龙头走板块梯队；
      3) 跨池去重（窄池先领）+ 统一补齐体检/量价/缠论（只对最终入选的票算，
         因为 checkup 里含缠论，对 2000 只票算一遍要几十秒）。
    """
    universe_total, pre = _universe(snap_map)
    pre_map = {str(r.get("code")): r for r in pre}

    # ---- 1) 日 K 精算集合 ----
    by_amt = sorted(pre, key=lambda r: -(r.get("amount") or 0))
    scan_codes = [str(r["code"]) for r in by_amt[:KLINE_SCAN_MAX]]
    td = engine.latest_trade_date()
    try:
        engine.ensure_klines(scan_codes, td)
    except Exception:
        pass

    feat = {}
    for code in scan_codes:
        ctx = _ctx(code, snap_map)
        if ctx:
            feat[code] = ctx

    pools = []
    seen = {}

    def take(items, limit=POOL_MAX):
        """去重 + 截断。返回 (入选列表, 命中数)。

        去重先到先得，所以调用顺序必须是「窄池优先」—— 龙头池全市场可能只有
        六七只，若让底背离（几十只）先领，真龙会被宽池吞掉。
        """
        hit = len(items)
        out = []
        for st in items:
            code = st["code"]
            if code in seen or len(out) >= limit:
                continue
            seen[code] = st
            out.append(st)
        return out, hit

    diag = {}

    def eval_scan(pid):
        """走日K扫描的四个池：对 feat 里每只票跑一次判定，按 _sort 降序。

        ★ 异常必须计数上报，不能 `except: pass` —— 之前底背离池因为一处
          NameError 被静默吞成「今日 0 只命中」，页面看起来像行情结论，
          实际是代码坏了。宁可多一行 pools_note，也不能让错误假装成空态。
        """
        fn = _HIT[pid]
        hits = []
        for code, (rows, f) in feat.items():
            ind = (pre_map.get(code) or {}).get("industry") or f.get("name") or "--"
            try:
                h = fn(f, rows, ind)
            except Exception as e:
                diag[pid] = diag.get(pid, 0) + 1
                if diag[pid] == 1:
                    diag[pid + "_msg"] = "%s: %s" % (type(e).__name__, e)
                h = None
            if h:
                hits.append((h.get("_sort") or 0, code, rows, f, ind, h))
        hits.sort(key=lambda x: -x[0])
        return hits

    # ---- 2) 逐池判定 ----
    # ① 突破池
    hits = eval_scan("breakout")
    lst = [_stock_of(f, rows, ind, h, "breakout") for _s, _c, rows, f, ind, h in hits]
    st, cnt = take(lst)
    # ★ 空态文案必须与原站 scan 条件用同一套词：这里写「四线多头」而 scan 是
    #   MA5>MA10>MA20，读者会以为池子还多了一道四线条件，进而怀疑是不是条件太严。
    pools.append(_pool("breakout", scanned=len(hits), stocks=st, **_pool_meta("breakout"),
                       empty_msg="今日全市场没有「收盘创 20 日新高 + MA5>MA10>MA20 + 放量收阳」"
                                 "的突破形态，说明当前缺乏强共识突破。"))

    # ② 涨停接力池（候选来自东财涨停池，不走扫描集合）
    zt_rows = ((sent or {}).get("pools") or {}).get("zt") or []
    cand = []
    for r in zt_rows:
        if not isinstance(r, dict):
            continue
        code = str(r.get("code") or "")
        if code[:2] not in ("60", "00") or code not in pre_map:
            continue
        if engine.is_st(r.get("name") or ""):
            continue
        snap = snap_map.get(code) or {}
        r["nmc_yi"] = snap.get("nmc_yi") or 0
        ctx = feat.get(code) or _ctx(code, snap_map)
        if not ctx:
            continue
        rows, f = ctx
        ind = snap.get("industry") or r.get("industry") or "--"
        try:
            h = _hit_limit_up(r, f, ind)
        except Exception as e:
            diag["limit_up"] = diag.get("limit_up", 0) + 1
            diag.setdefault("limit_up_msg", "%s: %s" % (type(e).__name__, e))
            h = None
        if h:
            cand.append((h.get("_sort") or 0, code, rows, f, ind, h))
    cand.sort(key=lambda x: -x[0])
    lst = [_stock_of(f, rows, ind, h, "limit_up") for _s, _c, rows, f, ind, h in cand]
    st, cnt = take(lst)
    pools.append(_pool("limit_up", scanned=len(cand), stocks=st, **_pool_meta("limit_up"),
                       empty_msg="今日涨停池里没有封板质量达标的票（炸板<2 次 且 封成比≥1%），"
                                 "接力空仓等"))

    # ③ 热点龙头池（板块梯队前 3，只取强度前 6 的板块）
    lead_y = sectors or []
    cand = []
    for si, sec in enumerate(lead_y[:6]):
        sec["_rank"] = si
        for li, lad in enumerate((sec.get("ladder") or [])[:3]):
            code = str(lad.get("code") or "")
            if not code or code not in pre_map:
                continue
            ctx = feat.get(code) or _ctx(code, snap_map)
            if not ctx:
                continue
            rows, f = ctx
            ind = (pre_map.get(code) or {}).get("industry") or sec.get("name") or "--"
            try:
                h = _hit_leader(sec, li, lad, f, ind)
            except Exception as e:
                diag["leader"] = diag.get("leader", 0) + 1
                diag.setdefault("leader_msg", "%s: %s" % (type(e).__name__, e))
                h = None
            if h:
                cand.append((h.get("_sort") or 0, code, rows, f, ind, h))
    cand.sort(key=lambda x: -x[0])
    lst = [_stock_of(f, rows, ind, h, "leader") for _s, _c, rows, f, ind, h in cand]
    st, cnt = take(lst)
    pools.append(_pool("leader", scanned=len(cand), stocks=st, **_pool_meta("leader"),
                       empty_msg="今日强势板块的梯队前 3 里没有可接力的日 K（"
                                 "或全部是 4 板以上的高位标杆）"))

    # ④ 缺口池
    hits = eval_scan("gap")
    lst = [_stock_of(f, rows, ind, h, "gap") for _s, _c, rows, f, ind, h in hits]
    st, cnt = take(lst)
    pools.append(_pool("gap", scanned=len(hits), stocks=st, **_pool_meta("gap"),
                       empty_msg="今日与前 4 日均无「未回补的向上跳空缺口」+ 量能配合的形态"))

    # ⑤ 回踩观察池
    hits = eval_scan("pullback")
    lst = [_stock_of(f, rows, ind, h, "pullback") for _s, _c, rows, f, ind, h in hits]
    st, cnt = take(lst)
    pools.append(_pool("pullback", scanned=len(hits), stocks=st, **_pool_meta("pullback"),
                       empty_msg="没有「近 20 日涨停过、MA 多头未破、回踩 MA20 附近」的票，"
                                 "意味今天不宜按回踩逻辑建仓"))

    # ⑥ 底背离池
    hits = eval_scan("divergence")
    lst = [_stock_of(f, rows, ind, h, "divergence") for _s, _c, rows, f, ind, h in hits]
    st, cnt = take(lst)
    pools.append(_pool("divergence", scanned=len(hits), stocks=st, **_pool_meta("divergence"),
                       empty_msg="今日没有「股价创 60 日新低而 DIF 未新低 + 缩量 + DIF 上穿 DEA」"
                                 "的标的，超跌反弹条件未成立"))

    # ---- 3) 统一补齐派生字段 ----
    all_st = [s for p in pools for s in p["stocks"]]
    stocks = list({s["code"]: s for s in all_st}.values())
    ind_map = _industry_map([s["code"] for s in stocks])

    def enrich(s):
        code = s["code"]
        ind = ind_map.get(code)
        if ind:
            s["industry"] = ind
        ctx = feat.get(code) or _ctx(code, snap_map)
        if not ctx:
            s["vp"] = {"gate": "none", "warn": [], "confirm": [], "mid": [], "info": [],
                       "counts": {"warn": 0, "confirm": 0, "mid": 0, "info": 0}}
            s["checkup"] = None
            s["chan_score"] = s["chan_grade"] = s["chan_walk"] = s["chan_bs"] = None
            s["bo"] = None
            return
        rows, f = ctx
        try:
            s["vp"] = engine.volprice(f)
        except Exception:
            s["vp"] = {"gate": "none", "warn": [], "confirm": [], "mid": [], "info": [],
                       "counts": {"warn": 0, "confirm": 0, "mid": 0, "info": 0}}
        s["gate"] = (s["vp"] or {}).get("gate")
        try:
            ck = engine.checkup(f)
            s["checkup"] = {"score": ck["score"], "strategy_score": ck["strategy_score"],
                            "verdict": ck["verdict"], "verdict_note": ck["verdict_note"],
                            "liq_ok": ck["liq_ok"], "liquidity": ck["liquidity"],
                            "vetoes": len(ck["vetoes"]), "weak": ck["weak"], "dims": ck["dims"]}
            ch = ck.get("chan") or {}
        except Exception:
            s["checkup"] = None
            ch = {}
        s["chan_score"] = ch.get("score")
        s["chan_grade"] = ch.get("grade")
        s["chan_walk"] = ch.get("walk")
        s["chan_bs"] = ch.get("bs")
        s["bo"] = bo_of(rows)
        L = (lhb_items or {}).get(code)
        if L:
            s["lhb"] = L
        if s.get("price") in (None, 0):
            s["price"] = _round(f.get("close"))

    # checkup 内含缠论，单票 ~60ms，串行 150 只要 9 秒；每只票彼此独立，
    # 放进线程池把磁盘读与 HTTP 缓存命中的等待叠起来。
    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(enrich, stocks))

    for p in pools:
        p["vp_counts"] = _vp_counts(p["stocks"])
        p["stocks"].sort(key=lambda s: -(s.get("_sort") or 0))
        for s in p["stocks"]:
            s.pop("_sort", None)

    return pools, {"total": universe_total, "prefiltered": len(pre),
                   "kline_scanned": len(feat), "kline_cap": KLINE_SCAN_MAX}, diag


def _diag_text(diag):
    """把逐池异常翻成一句人话。空 dict 返回 ""，前端据此决定要不要显示。

    ★ 这条通道的目的不是报错好看，而是让「0 只命中」与「代码坏了」在页面上
      长得不一样：前者是行情结论，后者必须显眼，否则会被当成「今天没有机会」。
    """
    if not diag:
        return ""
    parts = []
    for pid in ("breakout", "limit_up", "leader", "gap", "pullback", "divergence"):
        n = diag.get(pid) or 0
        if n:
            parts.append("%s有 %d 只判定异常（首例 %s）" % (
                POOL_BY_ID[pid]["name"], n, diag.get(pid + "_msg") or "未知"))
    return "；".join(parts)


def _vp_note():
    return ("量价三分区（与「量价分析」同源口径）：⚠️警示=背离/天量/滞涨/假突破/长上影 → "
            "gate=block，按纪律不建仓；✅确认=放量突破/缩量回调/低位堆量/低位天量/缩量涨停 → "
            "gate=pass；○中性/观察=天量变盘窗口/地量地价 → gate=watch，无明确信息。")


SHORT_NOTE = ("选股范围：沪深主板 60/00 · 非 ST · 总市值 < 200 亿"
              "（用户 2026-10-04 决定不放开创业板/科创板）。"
              "六大池标的由真实扫描得出，非硬编码。")

# ---------------------------------------------------------------------------
# 七、对外接口
# ---------------------------------------------------------------------------

METHODOLOGY = {
    "title": "短线作战体系",
    "desc": "六池全部由全市场真实扫描生成（无硬编码标的），规则提炼自三本短线交易著作的公开框架。"
            "三层顺序不能颠倒：先判断今天能不能做（市场环境）→ 做哪条线（主线板块）→ 选哪只票（六池）。",
    "rules": [
        "只做强势股，不碰弱势股与跟风补涨",
        "信号出现才动手，信号失效立即止损——概率游戏，不是预言游戏",
        "轻仓入场（单票 ≤2 成），做错小亏；只有顺势盈利才加仓",
        "止损三档：自然止损（缺口下沿/关键低点下方）、固定止损（-5%~-8%）、均线止损（跌破 5/10/20 日线）",
        "分批止盈，目标一个一个看，及时锁定利润，不贪长线",
        "仓位由情绪周期决定：退潮/冰点期空仓试错，发酵/加速期才是重仓窗口",
        "适时空仓：节奏比选股更重要",
        "持股 1-5 日，横盘 3 日必走，不恋战",
    ],
    "sources": [
        {"book": "游资全景看盘之板块轮动擒龙头",
         "note": "板块层核心：先定主线板块再看个股；板块强度四维打分"
                 "（涨幅中位数 30 + 龙头溢价 20 + 涨停占比 30 + 主力资金流 20）；"
                 "梯队划分（高标/龙头/二线/跟风/补涨）；只做板块龙头不做跟风补涨。"},
        {"book": "游资交易体系——21天突破",
         "note": "21 日突破窗口与情绪周期（冰点→启动→发酵→加速→高潮→退潮）六阶段定位；"
                 "情绪周期反推仓位上限。已接入本页「市场环境」区的仓位闸门。"},
        {"book": "短线操盘实战技法",
         "note": "11 章框架：操盘理念（概率思维/只做强势股/看长做短）→ "
                 "强势个股三部曲（缺口、涨停、热点龙头）→ 风控（三档止损）→ 交易系统与投资计划。"},
    ],
}

DISCIPLINE = [
    "不追高：涨幅超 7% 的票不追，等回踩再买",
    "不抄底：破位下跌的票不接，等企稳确认",
    "不满仓：单票 ≤2 成，总仓按情绪周期闸门",
    "不加仓摊平：亏损仓位只减不加",
    "不硬扛：跌停止损、开板止盈，尾盘尽量不留隔夜风险",
    "不做没计划的交易：每笔先定好买点、止损、目标位",
]

DISCLAIMER = ("本页所有内容由量化规则自动生成，价位为技术推算值，不构成投资建议。"
              "短线交易风险极高，请独立判断并严格执行止损纪律。"
              "龙虎榜为 T+1 盘后数据，覆盖仅当日异动个股，未上榜不等于没有主力。")

_PAYLOAD = {"ts": 0.0, "td": None, "data": None}


def api_short(params):
    td = engine.latest_trade_date()
    now = time.time()
    force = str(params.get("refresh") or "") in ("1", "true", "yes")
    if not force and _PAYLOAD["data"] and _PAYLOAD["td"] == td \
            and now - _PAYLOAD["ts"] < PAYLOAD_TTL:
        return _PAYLOAD["data"]

    t0 = time.time()
    import server
    sent = {}
    try:
        sent = server.api_sentiment({})
    except Exception:
        sent = {}

    # ★ 日 K 与实时数据可能不在同一天：收盘后东财涨停池马上就能查到当天，
    #   而日 K 要等数据源补当日那根（实测 15:08 时新浪指数日 K 还停在昨天）。
    #   此时**不能**拿「今天的涨停池 + 昨天的日 K 特征」判涨停接力 ——
    #   封单额是今天的、量比是昨天的，几个条件之间互相错位，判出来的票没意义。
    #   整页统一退回日 K 口径，再把实时日期单独报出去，让页面能说清「为什么还是昨天」。
    # ★ 两个日期来源格式不同，必须先统一再比：
    #   engine.latest_trade_date() 给 2026-11-05（带横线，直接给前端展示），
    #   server.api_sentiment 的 trade_date 给 2026-11-05（展示用）→ 去掉横线成 20261105。
    #   早先拿 20261105 直接跟 2026-11-05 比，永远不等 —— 明明同一天也报「日K待更新」。
    td_key = str(td).replace("-", "")
    live_td = str(sent.get("trade_date") or "").replace("-", "")
    kline_lag = bool(live_td and live_td != td_key)
    kline_lag_err = ""
    # 选股口径（同日池）：涨停候选必须与日 K 同一天，否则判定条件错位
    sent_asof = sent
    if kline_lag:
        try:
            # ★ 必须去掉横线：engine.latest_trade_date() 给的是 2026-11-04，
            #   而东财涨停池接口吃 YYYYMMDD。传带横线的格式查回来是空池，
            #   表现就是「涨停接力池 0 只」，看着像今天没有可接力的票。
            zt_asof = server._fetch_pool("zt", str(td).replace("-", ""))
            sent_asof = dict(sent)
            sent_asof["pools"] = dict(sent.get("pools") or {})
            sent_asof["pools"]["zt"] = [server._pool_row(r, "zt")
                                        for r in (zt_asof.get("pool") or [])]
        except Exception as e:
            # 不静默：取不到同日涨停池时，涨停接力/龙头两个池会变 0，
            # 页面必须能看出是「取数失败」还是「今天真没有」。
            kline_lag_err = "%s: %s" % (type(e).__name__, e)
            sent_asof = sent

    streak_map = _streak_map(sent_asof)
    sec_diag = {}
    sectors = _sectors(streak_map, sec_diag)
    lhb = lhb_table(force=force)
    lhb_items = lhb.get("items") or {}

    snap = engine.snapshot()
    snap_map = {str(r.get("code")): r for r in (snap.get("rows") or []) if r.get("code")}

    # 选股走「同日口径」的 sent_asof；市场环境（指数/涨跌家数/涨停家数/梯队）
    # 走实时 sent —— 温度就该反映此刻，而选股必须与日 K 同日，两者不能互相污染。
    pools, universe, diag = _collect(sent_asof, sectors, snap_map, lhb_items)
    # 市场环境要报「MACD 底背离今日命中几只」，所以必须等池子先跑完 ——
    # 这个数字不是装饰，它是「今天有没有超跌反弹机会」的判断依据。
    div_cnt = next((p["scanned"] for p in pools if p["id"] == "divergence"), 0)
    market = _market(sent, sectors, div_cnt)
    # 仓位闸门的具体数字要落进纪律清单，否则「按情绪周期闸门」是一句空话
    disc = list(DISCIPLINE)
    disc[2] = "不满仓：单票 ≤%.0f 成，总仓按情绪周期闸门（今日 %s）" % (
        market["advice"]["pool_pos_cap"], market["advice"]["position"])

    total = sum(len(p["stocks"]) for p in pools)
    note = SHORT_NOTE

    def _dash(s):
        s = str(s or "")
        return "%s-%s-%s" % (s[0:4], s[4:6], s[6:8]) if len(s) == 8 else (s or "--")

    if kline_lag:
        note += ("⚠ 日 K 尚未更新到 %s（数据源当日那根还没生成）：本页量价特征与板块强度"
                 "（含板块内涨停家数）按 %s 收盘计算；指数、涨跌家数、全市场涨停家数为"
                 "实时值（%s）。日 K 到位后本页会自动切到 %s。"
                 % (_dash(live_td), _dash(td), _dash(live_td), _dash(live_td)))
    if kline_lag_err:
        note += "⚠ 同日涨停池取数失败（%s），涨停接力池与热点龙头池可能为空。" % kline_lag_err
    if universe["prefiltered"] > universe["kline_scanned"]:
        note += ("日 K 精算按当日成交额降序取了前 %d 只（快照层命中 %d 只）—— "
                 "成交额排序≈流动性排序，与「短线只做有量的票」同一条原则。"
                 % (universe["kline_scanned"], universe["prefiltered"]))
    out = {
        "ok": True, "empty": total == 0,
        # 交易日必须是日 K 那一天：整页的量价特征都按它算出来的。
        # 早先写的是 sent.get("trade_date")，情绪接口切成实时日后，
        # 这里就会显示今天而数据还是昨天的 —— 比不更新更容易误导。
        "trade_date": td,
        "live_date": _dash(live_td) if live_td else "",
        "kline_lag": kline_lag,
        "updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "source": "东财涨停池 · 东财板块资金流 · 东财龙虎榜 · 本地量价引擎",
        "note": note,
        "elapsed": round(time.time() - t0, 1),
        "universe": universe,
        "market": market,
        "methodology": METHODOLOGY,
        "sectors": sectors,
        "sectors_note": sec_diag.get("error") or "",
        "pools_note": _diag_text(diag),
        "pools": pools,
        "vp_counts": _vp_counts([s for p in pools for s in p["stocks"]]),
        "vp_note": _vp_note(),
        "discipline": disc,
        "disclaimer": DISCLAIMER,
        "lhb_meta": {"trade_date": lhb.get("trade_date"), "count": lhb.get("count") or 0,
                     "seats": lhb.get("seats_raw") or {}, "elapsed": lhb.get("elapsed")},
    }
    if total:
        _PAYLOAD.update({"ts": now, "td": td, "data": out})
    else:
        out["error"] = "六个池均无标的：可能未做收盘扫描，或今日确实没有符合条件的形态"
    return out



# ---- 供 /api/rescan?target=lhb 复用（前端「⟳ 刷新龙虎榜」按钮走这里）----

_ST = {"building": False, "phase": "", "error": ""}


def status():
    return {"building": _ST["building"], "phase": _ST["phase"], "error": _ST["error"],
            "done": 0, "total": 0}


def _build(force=False):
    _ST.update({"building": True, "phase": "拉取龙虎榜", "error": ""})
    try:
        d = build_lhb(force=True)
        _ST["phase"] = "完成（%s，%d 只）" % (d.get("trade_date"), d.get("count") or 0)
        _LHB.update({"ts": time.time(), "td": engine.latest_trade_date(), "data": d})
        _PAYLOAD.update({"ts": 0.0, "data": None})
    except Exception as e:
        _ST["error"] = str(e)
        _ST["phase"] = "失败"
    finally:
        _ST["building"] = False
    return _ST
