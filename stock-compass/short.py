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

POOL_MAX = 40          # 单池最多展示多少只（再多前端也读不完）
TOTAL_MAX = 130        # 跨池去重后的总标的数上限，决定体检/突破窗口的计算量
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
SECTOR_N = 6

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
       break，得到「看起来很完整的 100 行」。现在每页单独重试，且返回
       是否取全，交给调用方决定要不要标注。

    返回 (rows, complete)。
    """
    rows, page, size = [], 1, 100
    want = min(total or size, 800)     # 真实上限：电子 512 只，别自作主张截断
    misses = 0
    while len(rows) < want and page <= 8:
        batch = []
        for t in range(3):             # 每页最多试 3 次，退避 0.3/0.6s
            d = _push("pn=%d&pz=%d&po=1&np=1&fltt=2&invt=2&fid=f3&fs=b:%s"
                      "&fields=f12,f14,f3,f2" % (page, size, bk), tries=1)
            batch = ((d.get("data") or {}).get("diff") or [])
            if batch:
                break
            time.sleep(0.3 * (t + 1))
        if not batch:
            misses += 1
            if misses >= 2:            # 连着两页空 → 认输，但标记不完整
                break
            page += 1
            continue
        rows.extend(batch)
        if len(batch) < size:
            break
        page += 1
    rows = [x for x in rows if x.get("f3") not in (None, "-")]
    complete = (not want) or len(rows) >= min(want, total or want) * 0.95
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
         "&fields=f12,f14,f3,f62,f104,f105,f128,f136" % (SECTOR_N * 3))
    d = _push(q)
    boards = ((d.get("data") or {}).get("diff") or [])
    if not boards:
        return _fail("板块列表接口无返回（%s）" % (_NET["last"] or "未知原因"))

    cands = boards[:SECTOR_N * 3]
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {b.get("f12"): ex.submit(_board_members, b.get("f12"),
                                        int(b.get("f104") or 0) + int(b.get("f105") or 0))
                for b in cands if b.get("f12")}
        members = {k: f.result() for k, f in futs.items()}

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
        grade = ("主线" if strength >= 70 else "次线" if strength >= 55
                 else "观察" if strength >= 40 else "退潮")

        ladder = []
        for i, x in enumerate(rows[:5]):
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
            "n": len(rows), "zt_cnt": len(zt),
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


def _market(sent, sectors):
    idx = (sent or {}).get("indices") or []
    bd = (sent or {}).get("breadth") or {}
    lim = (sent or {}).get("limit") or {}
    temp = (sent or {}).get("temperature") or {}

    zt = int(lim.get("zt") or 0)
    dt = int(lim.get("dt") or 0)
    zb = int(lim.get("zb") or 0)
    seal = lim.get("seal_rate")
    mx = int(lim.get("max_streak") or 0)
    up, down = bd.get("up"), bd.get("down")
    up_ratio = round(up / float(up + down) * 100, 1) if up and down else None

    # 情绪分档：用涨停家数 + 封板率 + 涨跌家数三项定档，阈值写死在这里便于复算
    score = 0
    if zt >= 80:
        score += 2
    elif zt >= 45:
        score += 1
    elif zt <= 20:
        score -= 1
    if dt >= 30:
        score -= 2
    elif dt >= 10:
        score -= 1
    if seal is not None:
        if seal >= 75:
            score += 1
        elif seal < 55:
            score -= 1
    if up_ratio is not None:
        if up_ratio >= 65:
            score += 1
        elif up_ratio <= 35:
            score -= 1

    if score >= 4:
        phase, level = "普涨 · 情绪高潮", "强势"
    elif score >= 2:
        phase, level = "偏暖 · 结构活跃", "偏暖"
    elif score >= -1:
        phase, level = "震荡 · 轮动分化", "中性"
    elif score >= -3:
        phase, level = "偏弱 · 赚钱效应差", "偏弱"
    else:
        phase, level = "普跌 · 情绪冰点", "冰点"

    pos = {2: "7 成以内", 1: "5 成以内", 0: "3~5 成", -1: "2~3 成", -2: "空仓或试错"}[max(-2, min(2, score))]
    idx_txt = " · ".join("%s %+.2f%%" % (i.get("name"), i.get("pct") or 0) for i in idx[:3]) or "指数数据缺失"
    breadth_txt = ("上涨 %s / 下跌 %s（上涨占比 %.1f%%）" % (up, down, up_ratio)) if up_ratio is not None else "市场宽度数据缺失"
    limit_txt = "涨停 %d · 跌停 %d · 炸板 %d" % (zt, dt, zb)
    if seal is not None:
        limit_txt += "（封板率 %.1f%%）" % seal
    if mx:
        limit_txt += " · 最高 %d 连板" % mx

    hot = [s["name"] for s in (sectors or [])[:2]]
    focus = ("主线：%s" % "、".join(hot)) if hot else "板块强度不足，无明确主线"
    if dt >= 25:
        avoid = "跌停 %d 家，退潮期一律不接力" % dt
    elif zb >= 30:
        avoid = "炸板 %d 家，封不住的高位板不碰" % zb
    else:
        avoid = "回避位置1（追高）与破位股"

    read = "情绪%s（%s）；%s。仓位上限 %s。" % (
        level, phase.split(" · ")[-1] if " · " in phase else phase, limit_txt, pos)

    signals = [
        {"k": "涨停 / 跌停", "v": "%d / %d" % (zt, dt), "level": "good" if zt > dt * 3 else ("warn" if dt > zt else "neu")},
        {"k": "封板率", "v": ("%.1f%%" % seal) if seal is not None else "--",
         "level": "good" if (seal or 0) >= 70 else ("warn" if (seal or 100) < 55 else "neu")},
        {"k": "市场宽度", "v": breadth_txt, "level": "good" if (up_ratio or 0) >= 60 else ("warn" if (up_ratio or 100) <= 40 else "neu")},
        {"k": "最高连板", "v": "%d 板" % mx if mx else "--", "level": "good" if mx >= 4 else "neu"},
        {"k": "情绪温度", "v": ("%.1f（%s）" % (temp.get("score"), temp.get("label"))) if temp.get("score") is not None else "--", "level": "neu"},
    ]

    tactics = ("只做首启与验证期，末窗只减不加" if score >= 2
               else "轻仓试错，只做最强主线的一进二" if score >= -1
               else "空仓等待，等跌停潮过去再看首板")

    return {
        "phase": phase, "phase_level": level, "read": read,
        "index_summary": idx_txt, "breadth": breadth_txt, "limit_summary": limit_txt,
        "signals": signals,
        "advice": {"position": pos, "focus": focus, "avoid": avoid, "tactics": tactics},
        "score": score,
    }


# ---------------------------------------------------------------------------
# 六、六个池
# ---------------------------------------------------------------------------

def _ctx(code, snap_map):
    """取一只票的 K 线与特征；K 线缺失返回 None。"""
    rows = engine.kline_get(code)
    if not rows or len(rows) < 30:
        return None
    f = engine.features(code, rows, snap_map.get(code))
    if not f:
        return None
    return rows, f


def _enrich(st, rows, f, lhb_items, need_checkup=True):
    """把前端要的附加字段补齐：体检结论 / 缠论分 / 量价分区 / 突破窗口 / 龙虎榜。"""
    if need_checkup and not st.get("checkup"):
        try:
            st["checkup"] = engine.checkup(f)
        except Exception:
            st["checkup"] = None
    if "chan_score" not in st:
        st["chan_score"] = None
    try:
        st["vp"] = engine.volprice(f)
    except Exception:
        st["vp"] = None
    st["bo"] = bo_of(rows)
    L = (lhb_items or {}).get(st.get("code") or "")
    if L:
        st["lhb"] = L
    return st


def _industry_map(codes):
    """批量取行业名（f100，如「影视院线」「半导体」）。一页 80 个 secid，173 只票 3 次请求。

    不复用板块成分表：成分只覆盖候选的 18 个板块，池子其余票会退化成「行业 --」，
    实测 173 只里有 67 只。f100 是东财给每只票的细分行业，一次拿全。
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


# ---------------------------------------------------------------------------
# 五之二、交易计划（前端 renderShortStock 读 s.plan，缺字段会直接印出 undefined）
# ---------------------------------------------------------------------------
STOP_MAX = 0.07          # 止损最宽 7%：3 日持仓的票，再宽的止损不叫止损


def _make_plan(rows, f, bo):
    """把 K 线特征翻成「买点 / 止损 / 目标 / 仓位 / 持有周期 / 卖出信号」。

    全部由数据推导，没有一条是写死的常数：
      买点   = MA5 与现价之间（回踩短均线接，不追高）
      止损   = max(近 5 日最低, 现价 −7%)，再压到现价之下 ——
               近 5 日最低就是「拉起来那根 K 线的地板」，破了说明这波失败；
               但高位票的 5 日最低可能离现价 20%，所以用 7% 封顶。
      目标① = max(现价 + 1.5×ATR, 20 日高点)，封顶 +25%
      目标② = max(目标① + 1.5×ATR, 60 日高点)，封顶 +50%
      仓位/周期 = 跟 21 天突破窗口走：首启有赔率、末窗只减不加。
    """
    close = float(f.get("close") or 0)
    if close <= 0:
        return {}
    atr = float(f.get("atr") or 0) or close * 0.02
    ma5 = float(f.get("ma5") or close)
    lo5 = min(float(r[4]) for r in rows[-5:]) if len(rows) >= 5 else close * 0.95

    # 买点区间要能真的挂单：现价之上的 4% 内回踩接。若直接写 MA5 到现价，
    # 刚拉过一根大阳线的票会得到「8.12 ~ 10.35」这种 27% 宽的区间，等于没给。
    if close > ma5:
        lo, hi = max(ma5, close * 0.96), close
    else:
        lo, hi = close * 0.98, close
    stop = min(max(lo5, close * (1 - STOP_MAX)), close * 0.995)

    t1 = max(close + 1.5 * atr, float(f.get("hi20") or 0))
    if t1 <= close * 1.005:
        t1 = close + 1.5 * atr
    t1 = min(t1, close * 1.25)
    t2 = max(t1 + 1.5 * atr, float(f.get("hi60") or 0))
    t2 = min(max(t2, t1 + atr), close * 1.50)

    ph = (bo or {}).get("phase") or ""
    position = {"首启": "3 成", "验证": "2 成", "末窗": "1 成（只减不加）",
                "过窗": "0（不参与）", "失守": "0（不参与）"}.get(ph, "1 成")
    hold = {"首启": "1~5 日", "验证": "3~8 日", "末窗": "1~3 日"}.get(ph, "1~3 日")

    # 卖出信号必须只留真正会先触发的那条：MA10 在止损位以下时，写「跌破 MA10 即走」
    # 等于给了一个永远不会先到、也没有意义的位置（实测有票 MA10 在 7.63、止损在 9.63）。
    ma10 = float(f.get("ma10") or close)
    if ma10 > stop:
        sell = "收盘跌破 MA10（%.2f）即走，或 %.1f%% 止损（%.2f）" % (
            ma10, (stop / close - 1) * 100, stop)
    else:
        sell = "%.1f%% 止损（%.2f）为先；MA10 在 %.2f、低于止损位，不另作离场参考" % (
            (stop / close - 1) * 100, stop, ma10)
    if ph == "末窗":
        sell += "；已到末窗，反弹只减不加"
    elif ph in ("过窗", "失守"):
        sell += "；已过突破窗口，不新开仓"

    return {
        "buy_zone": "%.2f ~ %.2f" % (round(lo, 2), round(hi, 2)),
        "stop_loss": round(stop, 2),
        "stop_pct": round((stop / close - 1) * 100, 1),
        "target1": round(t1, 2),
        "target2": round(t2, 2),
        "position": position,
        "hold": hold,
        "sell_signal": sell,
    }


def _tags_of(st, f):
    """标签只列事实，不下判断：涨停 / 大阳 / 倍量 / 突破首启。"""
    out = []
    pct = float(st.get("pct") or f.get("pct") or 0)
    lim = _limit_pct(str(st.get("code") or ""))
    if pct >= lim:
        out.append("涨停")
    elif pct >= 5:
        out.append("大阳")
    if float(f.get("vol_ratio") or 0) >= 2:
        out.append("倍量")
    if (st.get("bo") or {}).get("phase") == "首启":
        out.append("突破首启")
    return out[:3]


def _reason_of(st):
    """一句话结论：只拼接已有的客观事实，不加主观评价。"""
    bits = []
    bo = st.get("bo") or {}
    if bo.get("day"):
        bits.append("突破第 %d 天" % bo["day"])
    ck = (st.get("checkup") or {}).get("verdict")
    if ck:
        bits.append("体检" + ck)
    gate = (st.get("vp") or {}).get("gate")
    bits.append({"pass": "量价确认", "block": "量价警示"}.get(gate, "量价中性"))
    money = (st.get("lhb") or {}).get("money")
    if money:
        bits.append("龙虎榜" + money)
    return " · ".join(bits)


POOL_DEFS = [
    {
        "id": "chaoduan", "icon": "⚡", "name": "超短打板",
        "book_ref": "《游资全景看盘》· 当日强度优先",
        "logic": "只看当日强度与流动性，不看基本面：涨停/大阳、倍量、短均线多头、"
                 "贴近 60 日高点、收盘强势、成交额过亿，命中 ≥4 条入选。持仓 1~3 日。",
        "scan": "超短七条（涨停或 ≥5% / 量比 ≥2 / MA5>MA10 / 距 60 日高点 ≥98% / "
                "上影 ≤25% / 成交额 ≥1 亿）命中 ≥4",
        "risk": "warn",
    },
    {
        "id": "strong", "icon": "🔥", "name": "强势突破",
        "book_ref": "《笑傲股市》· 强势股筛选",
        "logic": "趋势与量能同时达标的中线强势股：均线多头、阶段涨幅居前、"
                 "量比健康、回撤可控。作为短线池的「势」的底座。",
        "scan": "强势股扫描（趋势 / 量能 / 位置 / 回撤多维打分）",
        "risk": "",
    },
    {
        "id": "right3", "icon": "✔", "name": "右侧位置3",
        "book_ref": "右侧交易体系 · 贴均线等确认",
        "logic": "六个右侧池里位置态为「位置3」的并集：贴着均线等确认，"
                 "不猜底、不追高。这是全站验证过的唯一在两个窗口都为正的位置。",
        "scan": "右侧六池（突破 / 布林 / 年线 / MACD / 平台 / 回踩）中位置态 = 位置3",
        "risk": "",
    },
    {
        "id": "dragon", "icon": "🐲", "name": "龙头接力",
        "book_ref": "《龙头股战法》· 只做最强的",
        "logic": "四维（带动性 / 封板质量 / 形态位置 / 量能资金）≥80 的真龙，"
                 "以 MA10 为生命线。接力只做第一次回踩，不接退潮期反抽。",
        "scan": "龙头四维等权合成 ≥80（真龙档）",
        "risk": "warn",
    },
    {
        "id": "mainwave", "icon": "🌊", "name": "主升浪",
        "book_ref": "《主升浪战法》· 三类买点",
        "logic": "A 突破当日、B 回踩 5~10 日线、C 回踩 20 日线。"
                 "短线优先取 A 与 B，C 属于中线加仓位。",
        "scan": "主升浪三类买点（A 突破 / B 回踩短均线 / C 回踩 20 日线）",
        "risk": "",
    },
    {
        "id": "lhb", "icon": "🏦", "name": "龙虎榜异动",
        "book_ref": "龙虎榜席位 · 资金性格",
        "logic": "当日上榜且买方净买入为正的个股，按资金性格标注是游资、机构、"
                 "通道还是散户接盘。**只作对手方提示，不参与任何评分。**",
        "scan": "当日龙虎榜上榜 且 买前五合计 > 卖前五合计",
        "risk": "warn",
    },
]


def _pool(id_, **kw):
    d = {"id": id_, "icon": "·", "name": id_, "book_ref": "", "logic": "",
         "scan": "", "scanned": 0, "risk": "", "empty_msg": "今日无符合条件标的",
         "stocks": [], "vp_counts": {"confirm": 0, "warn": 0, "mid": 0, "none": 0}}
    d.update(kw)
    return d


def _norm_stock(x, code_key="code"):
    return {
        "code": str(x.get(code_key) or x.get("code") or ""),
        "setcode": str(x.get("setcode") or ("1" if str(x.get("code") or "").startswith("6") else "0")),
        "name": x.get("name") or "",
        "industry": x.get("industry") or "--",
        "price": x.get("price"),
        "pct": x.get("pct"),
    }


def _collect(lhb_items):
    """从各数据源收集候选，再去重、限量、统一补齐派生字段。

    三个 server.api_* 都是全市场扫描（各自内部有 180~300 秒缓存），串行跑首屏要
    13 秒，所以并发发起；去重必须串行（后一个池要跳过前一个池已经收走的票），
    因此先取数、再组装。
    """
    import server  # 延迟导入：server 会 import 本模块，运行期再取避免循环依赖

    def safe(fn):
        try:
            return fn()
        except Exception as e:
            return {"__error__": str(e)}

    with ThreadPoolExecutor(max_workers=3) as ex:
        f_cd = ex.submit(safe, lambda: server.api_chaoduan({}))
        f_dr = ex.submit(safe, lambda: server.api_dragon({}))
        f_mw = ex.submit(safe, lambda: server.api_mainwave({}))
        chaoduan, dragon, mainwave = f_cd.result(), f_dr.result(), f_mw.result()

    strong = engine.load_strong() or {}
    right = engine.load_rightside() or {}

    pools, seen = [], {}

    def take(items, limit):
        """去重 + 截断。返回 (入选列表, 命中数)。"""
        hit = len(items)
        out = []
        for x in items:
            code = str(x.get("code") or "")
            if not code or code in seen or len(out) >= limit:
                continue
            st = _norm_stock(x)
            for k in ("chan_score", "chan_grade", "checkup", "vp"):
                if x.get(k) is not None:
                    st[k] = x.get(k)
            seen[code] = st
            out.append(st)
        return out, hit

    # ---- 领取顺序：窄池优先 ----
    # 一只票同时满足两个池时归谁，取决于谁先领。必须窄池优先：龙头池全市场
    # 命中只有 6 只，若让右侧位置3（23 只）先领，真龙会被宽池吞掉 —— 实测
    # 6 只只剩 2 只，龙头池等于作废。展示顺序另按 POOL_DEFS，不受此影响。

    # ---- 龙头接力 ----
    if dragon.get("__error__"):
        pools.append(_pool("dragon", icon="🐲", name="龙头接力",
                           empty_msg="龙头扫描不可用：%s" % dragon["__error__"]))
    else:
        lst = [x for x in (dragon.get("list") or []) if (x.get("score") or 0) >= 80]
        st, hit = take(lst, POOL_MAX)
        pools.append(_pool("dragon", icon="🐲", name="龙头接力",
                           book_ref=POOL_DEFS[3]["book_ref"], logic=POOL_DEFS[3]["logic"],
                           scan=POOL_DEFS[3]["scan"], risk="warn", scanned=hit, stocks=st,
                           empty_msg="今日没有四维 ≥80 的真龙，接力空仓等"))

    # ---- 龙虎榜异动 ----
    lst = [x for x in (lhb_items or {}).values() if (x.get("net_amt") or 0) > 0]
    lst.sort(key=lambda x: -(x.get("net_amt") or 0))
    st, hit = take(lst, POOL_MAX)
    pools.append(_pool("lhb", icon="🏦", name="龙虎榜异动",
                       book_ref=POOL_DEFS[5]["book_ref"], logic=POOL_DEFS[5]["logic"],
                       scan=POOL_DEFS[5]["scan"], risk="warn", scanned=hit, stocks=st,
                       empty_msg="今日龙虎榜无净买入标的（或接口未返回）"))

    # ---- 超短打板 ----
    if chaoduan.get("__error__"):
        pools.append(_pool("chaoduan", icon="⚡", name="超短打板",
                           empty_msg="超短扫描不可用：%s" % chaoduan["__error__"]))
    else:
        st, hit = take(chaoduan.get("list") or [], POOL_MAX)
        pools.append(_pool("chaoduan", icon="⚡", name="超短打板",
                           book_ref=POOL_DEFS[0]["book_ref"], logic=POOL_DEFS[0]["logic"],
                           scan=POOL_DEFS[0]["scan"] + "；候选池取成交额前 %d 名" % (chaoduan.get("total_scanned") or 0),
                           risk="warn", scanned=hit, stocks=st))

    # ---- 主升浪（只取 A、B 两类，C 是中线位）----
    if mainwave.get("__error__"):
        pools.append(_pool("mainwave", icon="🌊", name="主升浪",
                           empty_msg="主升浪扫描不可用：%s" % mainwave["__error__"]))
    else:
        lst = list(mainwave.get("buyA") or []) + list(mainwave.get("buyB") or [])
        st, hit = take(lst, POOL_MAX)
        pools.append(_pool("mainwave", icon="🌊", name="主升浪",
                           book_ref=POOL_DEFS[4]["book_ref"], logic=POOL_DEFS[4]["logic"],
                           scan=POOL_DEFS[4]["scan"], scanned=hit, stocks=st))

    # ---- 强势突破 ----
    lst = strong.get("list") or []
    st, hit = take(lst, POOL_MAX)
    pools.append(_pool("strong", icon="🔥", name="强势突破",
                       book_ref=POOL_DEFS[1]["book_ref"], logic=POOL_DEFS[1]["logic"],
                       scan=POOL_DEFS[1]["scan"] + "（%s 收盘后数据）" % (strong.get("trade_date") or ""),
                       scanned=hit, stocks=st,
                       empty_msg="强势扫描结果为空，先在本页或强势页做一次收盘扫描"))

    # ---- 右侧位置3（最宽，最后领）----
    lst = [x for p in (right.get("pools") or []) for x in (p.get("stocks") or [])
           if x.get("level_key") == "pos3"]
    st, hit = take(lst, POOL_MAX)
    pools.append(_pool("right3", icon="✔", name="右侧位置3",
                       book_ref=POOL_DEFS[2]["book_ref"], logic=POOL_DEFS[2]["logic"],
                       scan=POOL_DEFS[2]["scan"] + "（%s 收盘后数据）" % (right.get("trade_date") or ""),
                       scanned=hit, stocks=st,
                       empty_msg="右侧池里今日没有位置3 标的"))

    # 展示顺序回到 POOL_DEFS（超短 → 强势 → 右侧 → 龙头 → 主升 → 龙虎榜）
    _order = {d["id"]: i for i, d in enumerate(POOL_DEFS)}
    pools.sort(key=lambda p: _order.get(p["id"], 99))

    # 统一补齐：K 线 → 体检 / 量价 / 突破窗口
    snap = engine.snapshot()
    snap_map = {r.get("code"): r for r in (snap.get("rows") or []) if r.get("code")}
    # 先一次性拿全行业：池子定型后才知道要哪些代码，所以放在组装之后、补齐之前。
    ind_map = _industry_map([st["code"] for p in pools for st in p["stocks"]])

    def enrich_one(st):
        ctx = _ctx(st["code"], snap_map)
        ind = ind_map.get(st["code"])
        if ind:
            st["industry"] = ind              # 行业名比 "--" 有用，前端 indPill 会直接显示
        if not st.get("board"):
            st["board"] = (snap_map.get(st["code"]) or {}).get("board")
        if not ctx:
            st["bo"] = None
            st["plan"] = {}
            st["reason"] = "K 线不足 30 根，无法给出交易计划"
            st["tags"] = []
            # 次新股常常没有 30 根 K 线，但快照里有当天的价格与涨幅，
            # 不填这两个会导致卡片上「¥—」、涨跌幅空白，看着像数据丢了。
            row0 = snap_map.get(st["code"]) or {}
            if st.get("price") in (None, 0) and row0.get("price"):
                st["price"] = round(float(row0["price"]), 2)
            if st.get("pct") is None and row0.get("pct") is not None:
                st["pct"] = round(float(row0["pct"]), 2)
            return
        rows, f = ctx
        _enrich(st, rows, f, lhb_items)
        if st.get("price") in (None, 0):
            st["price"] = round(float(f["close"]), 2)
        if st.get("pct") is None:
            st["pct"] = round(float(f.get("pct") or 0), 2)
        st["plan"] = _make_plan(rows, f, st.get("bo"))
        st["tags"] = _tags_of(st, f)
        st["reason"] = _reason_of(st)
        ma10 = float(f.get("ma10") or 0)
        if ma10 and float(f.get("close") or 0) < ma10:
            st["risk_warn"] = True            # 收盘在 MA10 之下 = 生命线已破

    # 173 只票 ×（K 线 + 特征 + 体检 + 量价 + 突破窗口）串行要 10 秒，是首屏最大的一块开销；
    # 每只票之间完全独立，放进线程池后本地 K 线读取与 HTTP 缓存命中的等待可以重叠。
    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(enrich_one, [st for p in pools for st in p["stocks"]]))
    for p in pools:
        p["vp_counts"] = _vp_counts(p["stocks"])
    return pools


# ---------------------------------------------------------------------------
# 七、对外接口
# ---------------------------------------------------------------------------

METHODOLOGY = {
    "desc": "短线作战不预测，只跟随：先判断今天能不能做（市场环境），"
            "再判断做哪条线（主线板块），最后才是选哪只票（六个池）。"
            "三层顺序不能颠倒 —— 环境不对时，最好的形态也会亏钱。",
    "rules": [
        "先环境后个股：情绪冰点只做首板试错，仓位不超过 3 成",
        "只做主线：板块强度 < 40 的票不接力",
        "突破有时间窗：第 1~5 日有赔率，第 15~20 日只减不加",
        "破 MA10 生命线即走，不给自己找理由",
        "龙虎榜只看对手方，不参与打分",
        "单票亏损 5% 无条件止损，不摊平",
    ],
    "sources": [
        {"book": "游资全景看盘", "author": "", "note": "主线辨识与资金性格，用来决定打哪条线"},
        {"book": "笑傲股市", "author": "William O'Neil", "note": "强势股与突破的时间纪律"},
        {"book": "短线交易秘诀", "author": "Larry Williams", "note": "波动率与持仓周期的关系"},
        {"book": "龙头股战法", "author": "", "note": "接力的前提是龙头未退潮"},
    ],
}

DISCIPLINE = [
    "不在情绪高潮日追首板 —— 那是给隔日竞价接盘的人准备的",
    "不做没有主线的孤票，板块强度是短线的地基",
    "突破第 15 日之后只减不加，窗口将尽时要主动验货",
    "跌破 MA10 立即离场，不等反弹确认",
    "龙虎榜出现机构出货或拉萨系接盘，次日不做接力",
    "单票止损 5%、单日回撤 3% 停止交易，先停手再复盘",
]

DISCLAIMER = ("本页为规则化筛选与统计展示，不构成投资建议。短线交易对执行力、"
              "滑点与情绪控制极敏感，任何一条规则失效都可能放大亏损；"
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

    lhb = lhb_table(force=force)
    lhb_items = lhb.get("items") or {}
    streak_map = _streak_map(sent)
    sec_diag = {}
    sectors = _sectors(streak_map, sec_diag)
    market = _market(sent, sectors)
    pools = _collect(lhb_items)

    total = sum(len(p["stocks"]) for p in pools)
    out = {
        "ok": True, "empty": total == 0,
        "trade_date": sent.get("trade_date") or td,
        "updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "source": "东财龙虎榜 / 东财板块资金流 / 本地量价引擎",
        "elapsed": round(time.time() - t0, 1),
        "market": market,
        "methodology": METHODOLOGY,
        "sectors": sectors,
        "sectors_note": sec_diag.get("error") or "",
        "pools": pools,
        "discipline": DISCIPLINE,
        "disclaimer": DISCLAIMER,
        "lhb_meta": {"trade_date": lhb.get("trade_date"), "count": lhb.get("count") or 0,
                     "seats": lhb.get("seats_raw") or {}, "elapsed": lhb.get("elapsed")},
    }
    if total:
        _PAYLOAD.update({"ts": now, "td": td, "data": out})
    else:
        out["error"] = "各池均无标的：可能未做收盘扫描，或扫描结果为空"
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
