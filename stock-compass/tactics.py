"""18 式战法雷达（tactics.py）。

口径说明 —— 页面上不展开的部分都写在这里，避免「数字从哪来」说不清：

1. **18 式从哪来**：页面标题写明「师建《构建你自己的交易系统：18 种最优战法与选股公式
   及编码》（地震出版社 2013）公开目录框架逐章规则化」。原站只在前端文案里点名了四式
   （S05 下跌拐点 / S06 超跌筹码 / S07 妖股筹码 / S09 逆势爆发，且标注为「左侧类，需过
   右侧闸门」），其余编号原站未给出名称。本复刻**保留这四个原名与左侧定性**，其余 14 式
   按同一编号空间、用本站自有语汇（三位置、四重确认、量价、缠论、龙头）逐一规则化，
   名称与判定都写死在下面的 CATALOG 里，不做外部声明式的「原书目录」冒充。
2. **两把尺子**：战法口径（18 式 + 右侧成熟度）与合成口径（再叠加 12 维体检硬性否决）。
   两把尺子打架时**按更严的赢**，并置 `disagree` 如实标注，不修饰、不藏票。
3. **左侧四式为什么要加闸门**：S05/S06/S07/S09 本质是「猜底/接刀」，与右侧交易的前提
   冲突。所以它们只在**已站上 MA20 且 MA20 不再下行**时才计入右侧命中，否则只记
   `gated_hits`（页面上显示「另有 N 式左侧已过闸门」），不进评分、不当买点。
4. **评分不是概率**：`tactic_score` = 命中式按权重加权求和（最强的一式全额、其余各计
   35%），纯粹是可排序的相对强度，没有历史胜率支撑。要看历史表现请去「复盘」页 ——
   那边的结论是：这套打分在 2026-05~11 的采样区间里**与等权基准基本无差异**。
5. **扫描范围**：快照按成交额降序取前 1500 只（盘前成交额为 0 时退化为按流通市值），
   先做战法预筛，只对预筛出信号的票跑 12 维体检（上限 400 只）。全市场逐一体检要
   8~10 秒，收盘后扫描没必要每次都付这个代价。
"""
import json
import os
import threading
import time

import engine

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 与 engine.py 一致：COMPASS_STORE 让桌面便携版把缓存钉在固定目录（见 engine.py 注释）
STORE_DIR = os.environ.get("COMPASS_STORE") or BASE_DIR
STATE_DIR = os.path.join(STORE_DIR, "state")
CACHE_FILE = os.path.join(STATE_DIR, "tactics.json")

UNIVERSE_N = 1500        # 预筛池：成交额（盘前退化市值）前 N
MIN_BARS = 70            # 少于 70 根 K 线不参与（算不出 MA60 与 60 日结构）
DEEP_MAX = 400           # 最多对多少只跑 12 维体检
LIST_MAX = 60            # 主榜长度
LEFT_SET = ("S05", "S06", "S07", "S09")   # 左侧四式，需过右侧闸门

# (编号, 章, 名称, 侧, 权重, 一句话定义)
CATALOG = [
    ("S01", 1, "平台突破", "right", 1.00, "20 日窄幅整理后放量收在区间上沿"),
    ("S02", 2, "新高突破", "right", 0.95, "收盘创 60 日新高且量能确认"),
    ("S03", 2, "均线回踩", "right", 0.90, "上升途中缩量回踩 MA10/MA20 不破"),
    ("S04", 2, "生命线企稳", "right", 0.95, "盘中下探 MA20 后收在其上方，阳线收复"),
    ("S05", 3, "下跌拐点", "right_gated", 0.55, "下跌末端首次放量收阳（左侧，需闸门）"),
    ("S06", 3, "超跌筹码", "right_gated", 0.50, "低位缩量止跌、筹码换手充分（左侧，需闸门）"),
    ("S07", 4, "妖股筹码", "right_gated", 0.50, "高换手高振幅的情绪票回封（左侧，需闸门）"),
    ("S08", 4, "龙头首阴", "right", 0.65, "连板龙头首根阴线不破 MA5，博反包"),
    ("S09", 5, "逆势爆发", "right_gated", 0.50, "大盘走弱而个股放量走强（左侧，需闸门）"),
    ("S10", 5, "缩量错杀", "right", 0.45, "连续下跌但量能枯竭、长下影止跌"),
    ("S11", 6, "放量启动", "right", 0.90, "量比 ≥2 且站上 MA20，启动首日"),
    ("S12", 6, "缩量回踩", "right", 0.80, "上涨后缩量回踩不破 MA10"),
    ("S13", 6, "天量换手", "right", 0.60, "量比 ≥3 且收在当日上半区，换手充分"),
    ("S14", 7, "缺口不补", "right", 0.80, "向上跳空且当日不回补缺口"),
    ("S15", 7, "回抽颈线", "right", 0.85, "突破后回抽颈线缩量获支撑"),
    ("S16", 7, "双底右侧", "right", 0.70, "两个低点接近，现价突破颈线"),
    ("S17", 8, "零轴金叉", "right", 0.85, "MACD 零轴上方金叉/红柱由缩转放"),
    ("S18", 8, "加速主升", "right", 0.75, "连续放量上攻、均线完全多头，主升段"),
]

# 风险预警（不进 18 式目录、不进买入评分，只提示风险）
RISKS = [
    ("R1", 9, "高位放量滞涨", "risk", 0.0, "52 周高位放量但涨幅明显落后"),
    ("R2", 9, "高位长上影", "risk", 0.0, "高位长上影，上方抛压显现"),
    ("R3", 9, "断头铡刀", "risk", 0.0, "一根阴线连破 MA5/MA10/MA20"),
    ("R4", 9, "放量破线", "risk", 0.0, "放量跌破 MA20，结构转弱"),
]

SEV = {"可介入": 0, "观察": 1, "回避": 2}

_ST = {"building": False, "phase": "", "done": 0, "total": 0,
       "started": 0.0, "error": "", "td": ""}
_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# 小工具
# ---------------------------------------------------------------------------

def _r(v, n=2):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f or f in (float("inf"), float("-inf")):
        return None
    return round(f, n)


def _pct(a, b):
    """a 相对 b 的百分比变化；b 为 0 时返回 None。"""
    try:
        if not b:
            return None
        return (float(a) / float(b) - 1.0) * 100.0
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _avg(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else 0.0


def _veto_txt(vs):
    """体检的 vetoes 可能是字符串也可能是字典，前端只负责拼字符串，这里统一成文本。"""
    out = []
    for v in vs or []:
        if isinstance(v, dict):
            txt = v.get("k") or v.get("name") or v.get("text") or ""
            note = v.get("note") or v.get("d") or ""
            out.append(("%s %s" % (txt, note)).strip() or str(v))
        else:
            out.append(str(v))
    return out


def _ctx(f, rows, sr, mkt):
    """把 18 式要用到的派生量一次算好，避免每个判定器重复切片。"""
    cl = f["close"]
    o, h, l = f["open"], f["high"], f["low"]
    pc = f.get("prev_close") or (rows[-2][2] if len(rows) >= 2 else cl)
    rng = (h - l)
    c = dict(f)                      # f 的字段全部可见（ma20/dif/pos52/...）
    c.update({
        "sr": sr or {}, "mkt": mkt,
        "cl": cl, "o": o, "h": h, "l": l, "pc": pc,
        "vol": f.get("vol") or 0.0,
        "vr": f.get("vol_ratio") or 0.0,
        "amp": f.get("amp20") or 0.0,
        "body": abs(cl - o),
        "up_sh": (h - max(o, cl)) / cl * 100.0 if cl else 0.0,
        "dn_sh": (min(o, cl) - l) / cl * 100.0 if cl else 0.0,
        "range_pos": ((cl - l) / rng) if rng > 0 else 0.5,
        "ma20_up": bool(f.get("ma20") and f.get("ma20_prev") and f["ma20"] > f["ma20_prev"]),
        "ma60_up": bool(f.get("ma60") and f.get("close") and f["close"] > f["ma60"]),
        "chg5": _pct(cl, rows[-6][2]) if len(rows) >= 6 else None,
        "chg10": _pct(cl, rows[-11][2]) if len(rows) >= 11 else None,
        "right_ok": bool(f.get("ma20") and cl > f["ma20"]
                         and (f.get("ma20_prev") is None or f["ma20"] >= f["ma20_prev"] * 0.999)),
    })
    return c


def _gate(c):
    """右侧闸门：左侧四式必须站在 MA20 上、且 MA20 不再下行才算数。"""
    return c["right_ok"]


def _res(score, reasons, evidence=None, caveat=""):
    return (int(max(0, min(99, round(score)))), reasons, evidence or {}, caveat)


# ---------------------------------------------------------------------------
# 18 式判定器：命中返回 (score, reasons[], evidence{}, caveat)；不命中返回 None
# ---------------------------------------------------------------------------

def s01(c):
    """平台突破：20 日窄幅整理（振幅 < 18%）+ 收盘站上 20 日高点 + 放量 + 涨幅够。"""
    if not c["hi20"] or not c["vol_ma20"]:
        return None
    if c["amp"] > 18 or c["cl"] < c["hi20"] * 0.995:
        return None
    if c["vr"] < 1.4 or c["pct"] < 2.0:
        return None
    s = 52 + min(18, (c["vr"] - 1.4) * 10) + min(16, c["pct"] * 1.4)
    if c["amp"] < 12:
        s += 8
    if c["range_pos"] > 0.8:
        s += 5
    return _res(s, [
        "20 日振幅仅 %.1f%%，属窄幅平台" % c["amp"],
        "收盘 %.2f 站上平台高点 %.2f" % (c["cl"], c["hi20"]),
        "量比 %.2f 放大、当日涨 %.2f%%" % (c["vr"], c["pct"]),
    ], {"amp20": _r(c["amp"], 1), "vr": _r(c["vr"]), "pct": _r(c["pct"])})


def s02(c):
    """新高突破：收盘创 60 日新高 + 量能确认 + 涨幅不夸张（不是一日游的冲天炮）。"""
    if not c["hi60"] or c["cl"] < c["hi60"] * 0.995:
        return None
    if c["vr"] < 1.15 or c["pct"] < 1.0:
        return None
    s = 55 + min(15, (c["vr"] - 1.15) * 10) + (10 if c.get("hh") else 0)
    if 1.0 <= c["pct"] <= 7.0:
        s += 8          # 温和新高比涨停新高更可持续
    if c["dev20"] is not None and c["dev20"] > 15:
        s -= 10
    return _res(s, [
        "收盘 %.2f 创 60 日新高（前高 %.2f）" % (c["cl"], c["hi60"]),
        "量比 %.2f、涨 %.2f%%" % (c["vr"], c["pct"]),
        "距 MA20 %+.2f%%，偏离%s" % (c["dev20"] or 0, "偏大" if (c["dev20"] or 0) > 12 else "可控"),
    ], {"hi60": _r(c["hi60"]), "vr": _r(c["vr"]), "dev20": _r(c["dev20"])})


def s03(c):
    """均线回踩：MA20 上行 + 价格回到 MA10~MA20 附近 + 缩量。"""
    ma10, ma20 = c.get("ma10"), c.get("ma20")
    if not ma10 or not ma20 or not c["ma20_up"]:
        return None
    near = min(abs(c["cl"] / ma10 - 1), abs(c["cl"] / ma20 - 1)) * 100.0
    if near > 2.5 or c["cl"] < ma10 * 0.99:
        return None
    if c["vol_ma5"] and c["vol"] > c["vol_ma5"] * 0.9:
        return None
    if (c.get("chg20") or 0) < 0:
        return None
    s = 58 + (12 if near <= 1.5 else 6) + (10 if c["cl"] > ma20 else 0)
    if c["vol_ma5"] and c["vol"] < c["vol_ma5"] * 0.75:
        s += 8
    if c["range_pos"] > 0.6:
        s += 5
    return _res(s, [
        "价格贴近 MA10 %.2f / MA20 %.2f（偏离 %.2f%%）" % (ma10, ma20, near),
        "回踩日缩量：成交量为 5 日均量的 %.0f%%" % (c["vol"] / c["vol_ma5"] * 100 if c["vol_ma5"] else 0),
        "MA20 仍在上行，20 日涨幅 %+.1f%%" % (c.get("chg20") or 0),
    ], {"near": _r(near), "ma20": _r(ma20), "chg20": _r(c.get("chg20"), 1)})


def s04(c):
    """生命线企稳：盘中下探 MA20 附近、收盘收回其上、当日为阳线。"""
    ma20 = c.get("ma20")
    if not ma20:
        return None
    touched = c["l"] <= ma20 * 1.012
    if not touched or c["cl"] <= ma20 or c["cl"] <= c["o"]:
        return None
    if not c["ma60_up"]:
        return None
    s = 60 + (10 if c["dn_sh"] > 0.8 else 0) + (8 if c["vr"] >= 1.0 else 0)
    if c["range_pos"] > 0.7:
        s += 8
    if c["cl"] > ma20 * 1.01:
        s += 5
    return _res(s, [
        "最低 %.2f 下探 MA20 %.2f 后收回" % (c["l"], ma20),
        "收盘 %.2f 高于生命线 %.2f（+%.2f%%）" % (c["cl"], ma20, _pct(c["cl"], ma20) or 0),
        "收盘站在 MA60 %.2f 之上，中期结构未破" % (c.get("ma60") or 0),
    ], {"ma20": _r(ma20), "low": _r(c["l"]), "close": _r(c["cl"])})


def s05(c):
    """下跌拐点（左侧）：前期明显下跌 + 当日放量收阳 + 站上 MA5。"""
    if (c.get("chg20") or 0) > -8:
        return None
    if c["pct"] < 3.0 or c["cl"] <= c["o"]:
        return None
    if c["vr"] < 1.5 or not c.get("ma5"):
        return None
    if c["cl"] < c["ma5"]:
        return None
    s = 50 + min(18, (-(c["chg20"]) - 8) * 0.9) + min(15, (c["vr"] - 1.5) * 8)
    return _res(s, [
        "20 日累计下跌 %.1f%%，处下跌末端" % c["chg20"],
        "当日放量 %.2f 倍收阳、涨 %.2f%%" % (c["vr"], c["pct"]),
        "收盘站上 MA5 %.2f" % c["ma5"],
    ], {"chg20": _r(c["chg20"], 1), "vr": _r(c["vr"]), "pct": _r(c["pct"])},
        caveat="左侧战法：只说明下跌动能衰竭，不代表趋势已反转")


def s06(c):
    """超跌筹码（左侧）：52 周低位 + 60 日跌幅大 + 缩量止跌收阳。"""
    p52 = c.get("pos52")
    if p52 is None or p52 > 30:
        return None
    if (c.get("chg60") or 0) > -18:
        return None
    if c["pct"] <= 0 or c["cl"] <= c["o"]:
        return None
    if c["vr"] < 1.1:
        return None
    s = 48 + min(15, (30 - p52) * 0.5) + min(15, (-(c["chg60"]) - 18) * 0.35)
    return _res(s, [
        "52 周位置仅 %.1f%%，处历史低位区" % p52,
        "60 日跌幅 %.1f%%，超跌明显" % c["chg60"],
        "当日缩量转阳（量比 %.2f），有承接" % c["vr"],
    ], {"pos52": _r(p52, 1), "chg60": _r(c["chg60"], 1), "vr": _r(c["vr"])},
        caveat="左侧战法：低位反弹≠右侧启动，必须过闸门才计分")


def s07(c):
    """妖股筹码（左侧）：高振幅高换手 + 当日回封/大涨 + 量能足。"""
    to = (c["sr"] or {}).get("turnover") or 0.0
    if c["amp"] < 22 or to < 8:
        return None
    if c["pct"] < 4.0:
        return None
    if c["vr"] < 1.2:
        return None
    s = 48 + min(15, (to - 8) * 0.8) + min(12, (c["amp"] - 22) * 0.6) + min(12, c["pct"])
    return _res(s, [
        "20 日振幅 %.1f%%、换手 %.2f%%，情绪票特征" % (c["amp"], to),
        "当日涨 %.2f%%、量比 %.2f" % (c["pct"], c["vr"]),
        "52 周位置 %.0f%%" % (c.get("pos52") or 0),
    ], {"turnover": _r(to), "amp20": _r(c["amp"], 1), "pct": _r(c["pct"])},
        caveat="左侧战法：情绪票波动极大，止损必须硬")


def s08(c):
    """龙头首阴：前期连续大涨（5 日 ≥18%）后首根阴线，未破 MA5，量能收缩。"""
    if (c.get("chg5") or 0) < 18:
        return None
    if c["pct"] >= 0:
        return None
    ma5 = c.get("ma5")
    if not ma5 or c["cl"] < ma5:
        return None
    if c["vol"] >= (c["rows_prev_vol"] if c.get("rows_prev_vol") else c["vol"]):
        return None
    s = 50 + min(14, (c["chg5"] - 18) * 0.5) + (10 if c["dn_sh"] < 2.0 else 0)
    if c["range_pos"] > 0.5:
        s += 6
    return _res(s, [
        "前 5 日累计上涨 %.1f%%，属强势龙头" % c["chg5"],
        "今日首阴 -%.2f%% 但收在 MA5 %.2f 之上" % (abs(c["pct"]), ma5),
        "阴线缩量，抛压不重",
    ], {"chg5": _r(c["chg5"], 1), "pct": _r(c["pct"]), "ma5": _r(ma5)})


def s09(c):
    """逆势爆发（左侧）：大盘走弱（宽度 <45%）而个股放量涨 ≥3% 且强于 MA20。"""
    mk = c["mkt"] or {}
    if (mk.get("breadth") or 50) >= 45:
        return None
    if c["pct"] < 3.0 or c["vr"] < 1.6:
        return None
    if not c["right_ok"]:
        return None
    s = 50 + min(15, (45 - (mk.get("breadth") or 45)) * 0.6) + min(15, (c["vr"] - 1.6) * 7)
    return _res(s, [
        "大盘上涨占比仅 %.1f%%，环境偏弱" % (mk.get("breadth") or 0),
        "个股逆势涨 %.2f%%、量比 %.2f" % (c["pct"], c["vr"]),
        "已站上 MA20，属逆势里的强势股",
    ], {"breadth": _r(mk.get("breadth"), 1), "pct": _r(c["pct"]), "vr": _r(c["vr"])},
        caveat="左侧战法：弱市中逆势股回撤同样快")


def s10(c):
    """缩量错杀：连跌 + 量能枯竭（<20 日均量 60%）+ 长下影或收阳。"""
    if (c.get("chg10") or 0) > -8:
        return None
    if not c["vol_ma20"] or c["vol"] > c["vol_ma20"] * 0.6:
        return None
    if not (c["dn_sh"] > 1.0 or c["cl"] > c["o"]):
        return None
    s = 45 + min(15, (-(c["chg10"]) - 8) * 0.8) + (10 if c["dn_sh"] > 2.0 else 0)
    if c["cl"] > c["o"]:
        s += 5
    return _res(s, [
        "10 日下跌 %.1f%%" % c["chg10"],
        "成交量仅为 20 日均量的 %.0f%%，抛压枯竭" % (c["vol"] / c["vol_ma20"] * 100),
        "下影 %.2f%%，盘中有承接" % c["dn_sh"],
    ], {"chg10": _r(c["chg10"], 1), "vol_ratio20": _r(c["vol"] / c["vol_ma20"] if c["vol_ma20"] else 0),
        "dn_sh": _r(c["dn_sh"])})


def s11(c):
    """放量启动：量比 ≥2 + 涨幅 ≥3% + 站上 MA20 + 处于中低位。"""
    if c["vr"] < 2.0 or c["pct"] < 3.0:
        return None
    if not c["right_ok"]:
        return None
    if (c.get("dev20") or 0) > 12:
        return None
    s = 56 + min(18, (c["vr"] - 2.0) * 8) + min(12, c["pct"] * 0.9)
    if c["range_pos"] > 0.75:
        s += 6
    if (c.get("pos52") or 100) < 70:
        s += 5
    return _res(s, [
        "量比 %.2f（≥2 倍放量）、涨 %.2f%%" % (c["vr"], c["pct"]),
        "收盘 %.2f 站上 MA20 %.2f" % (c["cl"], c.get("ma20") or 0),
        "52 周位置 %.0f%%，未被高位套牢盘压制" % (c.get("pos52") or 0),
    ], {"vr": _r(c["vr"]), "pct": _r(c["pct"]), "pos52": _r(c.get("pos52"), 1)})


def s12(c):
    """缩量回踩：20 日涨幅 >5% 后的回调，量能萎缩、未破 MA10。"""
    if (c.get("chg20") or 0) < 5:
        return None
    if c["pct"] > 1.0:
        return None
    ma10 = c.get("ma10")
    if not ma10 or c["cl"] < ma10 * 0.985:
        return None
    if not c["vol_ma20"] or c["vol"] > c["vol_ma20"] * 0.85:
        return None
    s = 52 + (12 if c["cl"] > ma10 else 4) + min(10, ((c.get("chg20") or 0) - 5) * 0.4)
    return _res(s, [
        "20 日仍涨 %.1f%%，趋势未坏" % c["chg20"],
        "回调日缩量至 20 日均量的 %.0f%%" % (c["vol"] / c["vol_ma20"] * 100),
        "收盘 %.2f 守住 MA10 %.2f" % (c["cl"], ma10),
    ], {"chg20": _r(c["chg20"], 1), "ma10": _r(ma10), "vol_ratio20": _r(c["vol"] / c["vol_ma20"] if c["vol_ma20"] else 0)})


def s13(c):
    """天量换手：量比 ≥3 + 收在当日上半区 + 换手活跃。"""
    if c["vr"] < 3.0 or c["range_pos"] < 0.5:
        return None
    if c["pct"] <= 0:
        return None
    s = 50 + min(20, (c["vr"] - 3.0) * 6) + (10 if c["range_pos"] > 0.75 else 5)
    return _res(s, [
        "量比 %.2f，天量换手" % c["vr"],
        "收盘位于当日区间 %.0f%% 分位（收在上半区）" % (c["range_pos"] * 100),
        "当日涨 %.2f%%" % c["pct"],
    ], {"vr": _r(c["vr"]), "range_pos": _r(c["range_pos"]), "pct": _r(c["pct"])})


def s14(c):
    """缺口不补：向上跳空 >2% 且当日最低未回补，收阳。"""
    gap = _pct(c["o"], c["pc"])
    if gap is None or gap < 2.0:
        return None
    if c["l"] <= c["pc"] * 1.002:
        return None
    if c["cl"] <= c["o"] or c["pct"] <= 0:
        return None
    s = 55 + min(15, (gap - 2.0) * 3) + (10 if c["vr"] >= 1.5 else 0)
    return _res(s, [
        "开盘跳空 +%.2f%%（昨收 %.2f）" % (gap, c["pc"]),
        "最低 %.2f 未回补缺口，缺口成立" % c["l"],
        "收阳、涨 %.2f%%，量比 %.2f" % (c["pct"], c["vr"]),
    ], {"gap": _r(gap), "pct": _r(c["pct"]), "vr": _r(c["vr"])},
        caveat="缺口三日不回补才算有效，此处为当日盘中口径")


def s15(c):
    """回抽颈线：20 日内突破过前高，现价回落到颈线 ±2.5% 且缩量。"""
    if len(c.get("rows") or []) < 30:
        return None
    rows = c["rows"]
    win = rows[-25:-1]
    if not win:
        return None
    neck = max(r[3] for r in win)            # 近 25 日最高价作颈线
    if not neck or c["cl"] > neck * 1.03:
        return None
    near = abs(c["cl"] / neck - 1) * 100.0
    if near > 2.5:
        return None
    if not c["vol_ma20"] or c["vol"] > c["vol_ma20"] * 0.9:
        return None
    if not c["ma20_up"]:
        return None
    s = 58 + (12 if near <= 1.2 else 6) + (8 if c["cl"] > c["o"] else 0)
    return _res(s, [
        "颈线 %.2f，现价 %.2f（偏离 %.2f%%）" % (neck, c["cl"], near),
        "回抽日缩量至 20 日均量 %.0f%%" % (c["vol"] / c["vol_ma20"] * 100),
        "MA20 上行，突破结构未破坏",
    ], {"neck": _r(neck), "near": _r(near), "vo": _r(c["vol"] / c["vol_ma20"] if c["vol_ma20"] else 0)})


def s16(c):
    """双底右侧：近 60 日两个低点接近（差 ≤6%），现价上破两底之间的高点。"""
    rows = c.get("rows") or []
    if len(rows) < 60:
        return None
    win = rows[-60:]
    half = len(win) // 2
    left, right = win[:half], win[half:]
    if not left or not right:
        return None
    i1 = min(range(len(left)), key=lambda i: left[i][4])
    i2 = min(range(len(right)), key=lambda i: right[i][4])
    lo1, lo2 = left[i1][4], right[i2][4]
    if not lo1 or abs(lo1 / lo2 - 1) * 100 > 6:
        return None
    if (len(win) - half - i2) < 3:          # 右底要已经走出来
        return None
    mid = win[i1: half + i2]
    if not mid or len(mid) < 15:
        return None
    neck = max(r[3] for r in mid)
    # 颈线必须真的在两个底之上（否则只是横盘里的相邻低点），且现价是「刚上破」
    # 而不是已经飞出去 —— 后者属于追高，不该算双底右侧买点。
    if neck < lo1 * 1.06:
        return None
    if c["cl"] < neck * 0.995 or c["cl"] > neck * 1.08:
        return None
    if c["vr"] < 1.15:                 # 突破颈线必须有量，缩量上破多为假突破
        return None
    s = 55 + min(12, (neck / lo1 - 1) * 100 * 0.6) + (10 if c["vr"] >= 1.3 else 0)
    return _res(s, [
        "两低点 %.2f / %.2f（相差 %.1f%%）" % (lo1, lo2, abs(lo1 / lo2 - 1) * 100),
        "颈线 %.2f，现价 %.2f 已上破" % (neck, c["cl"]),
        "形态高度 %.1f%%" % ((neck / lo1 - 1) * 100),
    ], {"lo1": _r(lo1), "lo2": _r(lo2), "neck": _r(neck)})


def s17(c):
    """零轴金叉：DIF 在零轴上方且刚上穿 DEA，或红柱由缩转放。"""
    dif, dea = c.get("dif"), c.get("dea")
    dp, ep = c.get("dif_prev"), c.get("dea_prev")
    if dif is None or dea is None or dp is None or ep is None:
        return None
    if dif <= 0:
        return None
    golden = dp <= ep and dif > dea
    # 「红柱放大」必须显著放大（≥1.25 倍）才算一式：只要两根红柱一根比一根长就算命中，
    # 会让全市场 10% 的票都挂着这个标签，标签等于没有信息量。
    expand = (dif > dea and dif > 0 and (c.get("hist_prev") or 0) > 0
              and (c.get("hist") or 0) >= (c.get("hist_prev") or 0) * 1.25)
    if not (golden or expand):
        return None
    s = 60 if golden else 52
    if dif > dea and (c.get("hist") or 0) > 0:
        s += 8
    return _res(s, [
        "%s：DIF %.3f / DEA %.3f（零轴上方）" % ("金叉" if golden else "红柱放大", dif, dea),
        "柱状值 %.3f（前值 %.3f）" % (c.get("hist") or 0, c.get("hist_prev") or 0),
        "动量在零轴上方转强，属确认型信号",
    ], {"dif": _r(dif, 3), "dea": _r(dea, 3), "hist": _r(c.get("hist"), 3)})


def s18(c):
    """加速主升：连续 3 日上涨、累计 ≥10%、均线完全多头、偏离可控。"""
    rows = c.get("rows") or []
    if len(rows) < 4:
        return None
    up3 = all(rows[-i][2] > rows[-i - 1][2] for i in (1, 2, 3))
    if not up3:
        return None
    cum = _pct(c["cl"], rows[-4][2]) or 0
    if cum < 10:
        return None
    ma5, ma10, ma20 = c.get("ma5"), c.get("ma10"), c.get("ma20")
    if not (ma5 and ma10 and ma20 and ma5 > ma10 > ma20):
        return None
    if (c.get("dev20") or 0) > 18:
        return None
    s = 55 + min(18, (cum - 10) * 0.8) + (8 if c["vr"] >= 1.3 else 0)
    return _res(s, [
        "连续 3 日上涨、累计 %+.1f%%" % cum,
        "MA5 %.2f > MA10 %.2f > MA20 %.2f，短中期共振" % (ma5, ma10, ma20),
        "距 MA20 %+.1f%%，尚未过度偏离" % (c.get("dev20") or 0),
    ], {"cum3": _r(cum, 1), "dev20": _r(c.get("dev20"), 1), "vr": _r(c["vr"])},
        caveat="加速段也是风险积累段，追高需按位置1处理")


def r1(c):
    """高位放量滞涨。"""
    if (c.get("pos52") or 0) < 82 or c["vr"] < 2.0 or c["pct"] > 1.0:
        return None
    return _res(60 + min(20, (c["vr"] - 2) * 5),
                ["52 周位置 %.0f%% 的高位出现 %.1f 倍量" % (c.get("pos52") or 0, c["vr"]),
                 "当日仅涨 %.2f%%，量增价滞" % c["pct"]],
                {"pos52": _r(c.get("pos52"), 1), "vr": _r(c["vr"])})


def r2(c):
    """高位长上影。"""
    if (c.get("pos52") or 0) < 75:
        return None
    if c["up_sh"] < 2.0 or c["up_sh"] < c["body"] / c["cl"] * 100 * 1.5:
        return None
    return _res(58 + min(18, c["up_sh"] * 3),
                ["上影线 %.2f%%，上方抛压明显" % c["up_sh"],
                 "52 周位置 %.0f%%，处高位" % (c.get("pos52") or 0)],
                {"up_sh": _r(c["up_sh"]), "pos52": _r(c.get("pos52"), 1)})


def r3(c):
    """断头铡刀：一根阴线同时跌破 MA5/MA10/MA20。"""
    ma5, ma10, ma20 = c.get("ma5"), c.get("ma10"), c.get("ma20")
    if not (ma5 and ma10 and ma20):
        return None
    if c["cl"] >= min(ma5, ma10, ma20) or c["pct"] > -3.0:
        return None
    if c["pc"] < ma20:
        return None
    return _res(70, ["收盘 %.2f 一举跌破 MA5 %.2f / MA10 %.2f / MA20 %.2f" % (c["cl"], ma5, ma10, ma20),
                     "当日跌 %.2f%%，形态转弱" % c["pct"]],
                {"pct": _r(c["pct"]), "ma20": _r(ma20)})


def r4(c):
    """放量破线：放量跌破 MA20。"""
    ma20 = c.get("ma20")
    if not ma20 or c["cl"] >= ma20 or c["pc"] < ma20:
        return None
    if c["vr"] < 1.5:
        return None
    return _res(62 + min(15, (c["vr"] - 1.5) * 6),
                ["放量跌破 MA20 %.2f（量比 %.2f）" % (ma20, c["vr"]),
                 "收盘 %.2f，结构由多转空" % c["cl"]],
                {"ma20": _r(ma20), "vr": _r(c["vr"])})


FUNCS = {"S01": s01, "S02": s02, "S03": s03, "S04": s04, "S05": s05, "S06": s06,
         "S07": s07, "S08": s08, "S09": s09, "S10": s10, "S11": s11, "S12": s12,
         "S13": s13, "S14": s14, "S15": s15, "S16": s16, "S17": s17, "S18": s18,
         "R1": r1, "R2": r2, "R3": r3, "R4": r4}


# ---------------------------------------------------------------------------
# 单票评估
# ---------------------------------------------------------------------------

def _scan_hits(c):
    """跑全部判定器，返回 (命中列表, 风险列表)。左侧四式未过闸门的不计入命中。"""
    hits, risks, gated = [], [], 0
    for sid, ch, name, side, w, _desc in CATALOG:
        fn = FUNCS.get(sid)
        if not fn:
            continue
        try:
            res = fn(c)
        except Exception:
            res = None
        if not res:
            continue
        score, reasons, ev, caveat = res
        if side == "right_gated" and not _gate(c):
            gated += 1
            continue
        hits.append({"id": sid, "chapter": ch, "name": name, "side": side,
                     "score": score, "weight": w, "reasons": reasons,
                     "evidence": ev, "caveat": caveat})
    for sid, ch, name, side, _w, _desc in RISKS:
        fn = FUNCS.get(sid)
        if not fn:
            continue
        try:
            res = fn(c)
        except Exception:
            res = None
        if res:
            score, reasons, ev, caveat = res
            risks.append({"id": sid, "chapter": ch, "name": name, "side": "risk",
                          "score": score, "weight": 0.0, "reasons": reasons,
                          "evidence": ev, "caveat": caveat})
    hits.sort(key=lambda h: -(h["score"] * max(h["weight"], 0.05)))
    return hits, risks, gated


def _tactic_score(hits):
    """最强一式计 70%、其余各计 12%，封顶 96。

    为什么不用简单加权求和：18 个判定器互相高度相关（一个健康的上升趋势会同时触发
    回踩、金叉、双底），求和的结果是「命中越多越接近 100」，于是主榜前 20 名全部
    顶格 100，排序等于没排。压缩后：1 式命中 ≈ 66 分、2 式 ≈ 78、3 式 ≈ 89、
    4 式以上封顶 96 —— 既保留「多因子共振更强」的方向，又不让分数失去分辨力。
    这仍然只是相对强度，不是概率。
    """
    if not hits:
        return 0
    vals = sorted((h["score"] * max(h["weight"], 0.05) for h in hits), reverse=True)
    total = vals[0] * 0.70 + sum(vals[1:]) * 0.12
    return int(max(0, min(96, round(total))))


def _verdict(ts, right_hits, dev20, risks):
    """战法口径的结论。

    阈值是照压缩后的分数标定的：单式命中上限约 66（正好够「观察」），所以要「可介入」
    必须至少两式共振。风险预警只做降级（可介入→观察），不做否决 —— 硬性否决是 12 维
    体检那一把尺子的事，两把尺子各管一段，不越位。
    """
    if (dev20 or 0) > 15:
        return "回避"          # 位置1 追高：离 MA20 超过 15%，右侧交易直接不参与
    if ts >= 66 and right_hits >= 2:
        v = "可介入"
    elif ts >= 48:
        v = "观察"
    else:
        v = "回避"
    if risks and v == "可介入":
        v = "观察"
    return v


def _plan(f, level_key):
    """买点计划：engine.make_plan 的价位 + 盈亏比 + 建议仓位（三位置档位）。"""
    p = dict(engine.make_plan(f, level_key) or {})
    close = f.get("close")
    stop = p.get("stop_loss")
    t1 = p.get("target1")
    try:
        if close and stop and t1 and close > stop:
            p["rr"] = _r((t1 - close) / (close - stop), 2)
    except (TypeError, ZeroDivisionError):
        p["rr"] = None
    p["position_pct"] = {"pos3": 30, "pos2": 20}.get(level_key, 0)
    return p


def _risk_pos(stop_pct, level_key):
    """风险仓位：单笔风险预算 2% 除以止损距离，封顶 30%、位置1 直接 0。"""
    if level_key == "pos1" or not stop_pct:
        return 0
    pct = 2.0 / max(abs(stop_pct), 0.5) * 100.0
    return int(max(2, min(30, round(pct))))


def evaluate(code, name, rows, sr, mkt, deep=True, f=None):
    """单票评估。f 可由调用方预先算好传进来，避免全市场扫描时重复 features()。"""
    if f is None:
        f = engine.features(code, rows, sr)
    if not f:
        return None
    f["rows"] = rows
    c = _ctx(f, rows, sr, mkt)
    # 首阴要跟昨日量比：放在 ctx 里，避免 s08 自己切片
    c["rows_prev_vol"] = rows[-2][5] if len(rows) >= 2 else 0.0

    hits, risks, gated = _scan_hits(c)
    if not hits and not risks:
        return None

    ts = _tactic_score(hits)
    right_hits = sum(1 for h in hits if h["side"] == "right")
    dev20 = f.get("dev20")
    dart = _verdict(ts, right_hits, dev20, risks)

    mt = engine.maturity(f) or {}
    sub = mt.get("sub") or {}
    maturity_view = {
        "score": mt.get("score"),
        # ★ 前端读的是 mt.sub.brk，engine.maturity 里叫 break —— 不改前端就在这里改名
        "sub": {"struct": sub.get("struct"), "brk": sub.get("break"),
                "vol": sub.get("vol")},
        "tags": mt.get("tags") or [],
    }

    ck = {}
    chan_walk = ""
    if deep:
        try:
            ck = engine.checkup(f, sr, mkt.get("_temp")) or {}
            chan_walk = ((ck.get("chan") or {}).get("walk") or "")
        except Exception:
            ck = {}
    ck_v = ck.get("verdict")
    vetoes = _veto_txt(ck.get("vetoes"))

    if ck_v:
        final = ck_v if SEV.get(ck_v, 1) > SEV.get(dart, 1) else dart
        # 含硬性否决的一律按回避处理（体检的否决是纪律，不允许被战法分覆盖）
        if vetoes:
            final = "回避"
    else:
        final = dart

    disagree = bool(ck_v) and final != dart
    if disagree:
        note = "战法口径 %s / 体检口径 %s（%s），按更严的赢" % (
            dart, ck_v, ck.get("verdict_note") or "体检结论更保守")
    else:
        note = ""

    level_key = ck.get("level_key") or ("pos1" if (dev20 or 0) > 15
                                        else ("pos3" if (dev20 or 0) <= 3 else "pos2"))
    plan = _plan(f, level_key)
    risk_pos = _risk_pos(plan.get("stop_pct"), level_key)

    row = {
        "code": code, "name": name or (sr or {}).get("name") or code,
        "secid": engine.secid_of_code(code),
        "price": _r(f.get("close")), "pct": _r(f.get("pct")),
        "dev20": _r(dev20),
        "score": int(round(ts * 0.6 + (mt.get("score") or 0) * 0.4)),
        "tactic_score": ts,
        "verdict": dart,
        "verdict_with_checkup": {"verdict": final, "vetoes": vetoes[:3]},
        "vetoes": vetoes[:3],
        "tactic_hits": hits + risks,
        "right_hits": right_hits,
        "gated_hits": gated,
        "risk_warn": risks[0]["name"] if risks else "",
        "checkup": {"verdict": ck_v, "score": ck.get("score"),
                    "vetoes": len(vetoes)} if ck_v else {},
        "disagree": disagree, "disagree_note": note,
        "maturity": maturity_view,
        "chan_walk": chan_walk,
        "plan": plan,
        "discipline": {"defense": {"price": _r(f.get("ma20")), "date": "MA20"},
                       "risk_pos": {"pct": risk_pos}},
        "_ts": ts,
    }
    return row


# ---------------------------------------------------------------------------
# 全市场扫描（收盘后一次，按交易日缓存）
# ---------------------------------------------------------------------------

def _universe():
    snap = engine.snap_index()
    rows = [r for r in snap.values()
            if engine.is_a_share(r["code"]) and not engine.is_st(r.get("name") or "")
            and (r.get("price") or 0) > 0]
    rows.sort(key=lambda r: -((r.get("amount") or 0) or (r.get("nmc_yi") or 0) * 1e8))
    return rows[:UNIVERSE_N]


def _build(force=False):
    t0 = time.time()
    with _LOCK:
        _ST.update({"building": True, "phase": "准备票池", "done": 0, "total": 0,
                    "started": t0, "error": "", "td": ""})
    try:
        snap_rows = _universe()
        temp, mk = engine.market_temp()
        mk["_temp"] = temp
        with _LOCK:
            _ST.update({"phase": "18 式预筛", "total": len(snap_rows)})

        cand, short = [], 0
        width = {"n": 0, "above": 0, "up": 0, "right": 0}
        for sr in snap_rows:
            code = sr["code"]
            rows = engine.kline_get(code)
            if not rows or len(rows) < MIN_BARS:
                short += 1
                with _LOCK:
                    _ST["done"] += 1
                continue
            f = engine.features(code, rows, sr)
            if f and f.get("ma20"):
                # 右侧结构宽度：站上 MA20 且 MA20 不下行。这是判断「普涨是趋势还是反弹」
                # 的关键 —— 只看上涨家数会把反弹日误读成趋势日。
                width["n"] += 1
                up_ma20 = bool(f.get("ma20_prev") and f["ma20"] >= f["ma20_prev"] * 0.999)
                if f["close"] > f["ma20"]:
                    width["above"] += 1
                if up_ma20:
                    width["up"] += 1
                if f["close"] > f["ma20"] and up_ma20:
                    width["right"] += 1
            row = evaluate(code, sr.get("name") or code, rows, sr, mk, deep=False, f=f)
            if row:
                cand.append((row, sr, rows))
            with _LOCK:
                _ST["done"] += 1

        cand.sort(key=lambda x: (-x[0]["tactic_score"], -(x[0]["score"])))
        deep_set = cand[:DEEP_MAX]
        with _LOCK:
            _ST.update({"phase": "12 维体检复核", "done": 0, "total": len(deep_set)})

        out = []
        for row, sr, rows in deep_set:
            full = evaluate(sr["code"], row["name"], rows, sr, mk, deep=True)
            if full:
                out.append(full)
            with _LOCK:
                _ST["done"] += 1

        out.sort(key=lambda r: (-r["score"], -r["tactic_score"]))
        lst = out[:LIST_MAX]
        for r in lst:
            r.pop("_ts", None)

        # 命中分布：按 18 式统计今日命中数与均分
        dims = []
        for sid, ch, name, side, _w, _d in CATALOG:
            hs = [h for r in out for h in r["tactic_hits"] if h["id"] == sid]
            dims.append({"id": sid, "chapter": ch, "name": name, "side": side,
                         "hits": len(hs),
                         "avg_score": _r(_avg([h["score"] for h in hs]), 1) if hs else None})

        cnt = {"可介入": 0, "观察": 0, "回避": 0}
        for r in lst:
            v = r["verdict_with_checkup"]["verdict"]
            cnt[v] = cnt.get(v, 0) + 1

        breadth = mk.get("breadth") or 0
        wn = max(1, width["n"])
        w_above = width["above"] * 100.0 / wn
        w_up = width["up"] * 100.0 / wn
        w_right = width["right"] * 100.0 / wn
        if breadth >= 65 and w_right >= 45:
            env, env_d = "普涨 · 右侧结构完好", "多数个股上涨且多数站在上行 MA20 之上，趋势环境，可正常按位置3执行"
        elif breadth >= 65:
            env, env_d = "反弹普涨 · 右侧结构未修复", (
                "上涨家数占优，但仅 %.0f%% 的个股站上上行 MA20 —— 这是超跌反弹而非趋势，"
                "追涨容易被套，只做已确认的右侧票、仓位减半" % w_right)
        elif breadth >= 45 and w_right >= 30:
            env, env_d = "结构分化", "涨跌各半，趋势机会集中在少数主线，只做位置3"
        elif breadth >= 45:
            env, env_d = "分化偏弱", "上涨家数与右侧结构都不占优，容错低，仓位收紧"
        elif breadth >= 30:
            env, env_d = "偏弱", "下跌家数占优，右侧信号成功率下降"
        else:
            env, env_d = "普跌", "系统性下跌，右侧战法胜率显著下降，以观望为主"

        payload = {
            "ok": True,
            "empty": False,
            "trade_date": engine.latest_trade_date(),
            "updated": time.strftime("%Y-%m-%d %H:%M:%S"),
            "stats": {
                "universe": len(snap_rows),
                "universe_src": "快照按成交额降序取前 %d" % UNIVERSE_N,
                "quotes": len(engine.snapshot().get("rows") or []),
                "evaluated": len(snap_rows) - short,
                "kline_short": short,
                "candidates": len(cand),
                "deep": len(out),
                "t_total": round(time.time() - t0, 1),
            },
            "market": {
                "name": env, "desc": env_d,
                "approx": "上涨 %d / 下跌 %d（上涨占比 %.1f%%）· 站上 MA20 %.1f%% · "
                          "MA20 上行 %.1f%% · 右侧结构宽度 %.1f%% · 成交 %.0f 亿 · 市场温度 %.0f" % (
                              mk.get("up") or 0, mk.get("down") or 0, breadth,
                              w_above, w_up, w_right, mk.get("amount_yi") or 0, temp),
                "breadth": _r(breadth, 1), "temp": _r(temp, 1),
                "width": {"above_ma20": _r(w_above, 1), "ma20_up": _r(w_up, 1),
                          "right_ok": _r(w_right, 1), "base": width["n"]},
            },
            "catalog": [{"id": s, "chapter": ch, "name": n, "side": side, "desc": d}
                        for s, ch, n, side, _w, d in CATALOG],
            "dims": dims,
            "list": lst,
            "counts": cnt,
            "hint": "战法负责找机会、12 维体检负责管风险；两把尺子打架按更严的赢。"
                    "本页为收盘后扫描结果，盘中实时信号请看「实盘信号」。",
        }
        os.makedirs(STATE_DIR, exist_ok=True)
        tmp = CACHE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False)
        os.replace(tmp, CACHE_FILE)
        with _LOCK:
            _ST.update({"building": False, "phase": "完成", "td": payload["trade_date"]})
        return payload
    except Exception as e:
        with _LOCK:
            _ST.update({"building": False, "phase": "失败",
                        "error": "%s: %s" % (type(e).__name__, e)})
        raise


def _load():
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


def status():
    with _LOCK:
        return dict(_ST)


def api_tactics(params):
    td = engine.latest_trade_date()
    cache = _load()
    if cache and cache.get("trade_date") == td and not (params or {}).get("refresh"):
        return cache

    with _LOCK:
        building = _ST["building"]
    if not building:
        threading.Thread(target=_build, kwargs={"force": True}, daemon=True).start()

    # 首次扫描约 20~40 秒：等 5 秒，赶得上就直接给结果，赶不上返回进度让前端轮询
    deadline = time.time() + 5.0
    while time.time() < deadline:
        with _LOCK:
            if not _ST["building"] and _ST["error"]:
                return {"ok": False, "empty": True, "error": "扫描失败", "hint": _ST["error"]}
        c = _load()
        if c and c.get("trade_date") == td:
            return c
        time.sleep(0.2)

    st = status()
    pct = int(st["done"] * 100.0 / st["total"]) if st["total"] else 0
    return {
        "ok": True, "empty": True, "stale": True,
        "note": "正在扫描全市场 18 式（%s %d%%），首次约 20~40 秒后自动刷新。" % (
            st["phase"] or "排队", pct),
        "hint": "扫描完成后本页自动加载；同一交易日内只需算一次。",
    }


def api_tacticscheck(params):
    code = (params or {}).get("code") or ""
    secid = (params or {}).get("secid") or ""
    if not code and secid:
        code = secid.split(".")[-1]
    if not code:
        return {"error": "缺少 code"}
    rows = engine.kline_get(code)
    if not rows or len(rows) < MIN_BARS:
        return {"error": "日 K 不足（%d 根），无法复核" % (len(rows or []))}
    sr = engine.snap_index().get(code) or {}
    temp, mk = engine.market_temp()
    mk["_temp"] = temp
    f = engine.features(code, rows, sr)
    if not f:
        return {"error": "行情特征计算失败"}
    f["rows"] = rows
    c = _ctx(f, rows, sr, mk)
    c["rows_prev_vol"] = rows[-2][5] if len(rows) >= 2 else 0.0

    hits, risks, gated = _scan_hits(c)
    hit_map = {h["id"]: h for h in hits + risks}
    ts = _tactic_score(hits)
    right_hits = sum(1 for h in hits if h["side"] == "right")
    dart = _verdict(ts, right_hits, f.get("dev20"), risks)

    lst = []
    for sid, ch, name, side, _w, desc in CATALOG:
        h = hit_map.get(sid)
        gated_here = (not h) and side == "right_gated" and FUNCS[sid](c) is not None
        reasons = list(h["reasons"]) if h else ([desc] if not gated_here else
                                               ["信号出现但未过右侧闸门（MA20 未站稳或仍在下行）"])
        lst.append({
            "chapter": ch, "name": name, "side": side,
            "hit": bool(h), "gated": gated_here,
            "score": h["score"] if h else None,
            "reasons": reasons,
        })
    out_risk = [{"chapter": r["chapter"], "name": r["name"], "side": "risk",
                 "hit": True, "score": r["score"], "reasons": r["reasons"]} for r in risks]

    return {
        "ok": True,
        "code": code, "name": sr.get("name") or code,
        "price": _r(f.get("close")), "pct": _r(f.get("pct")),
        "dev20": _r(f.get("dev20")),
        "score": ts, "tactic_score": ts,
        "right_hits": right_hits, "gated_hits": gated,
        "list": out_risk + lst,
        "verdict": {
            "verdict": dart,
            "notes": ["命中 %d 式（右侧 %d 式，闸门后 %d 式）" % (len(hits), right_hits, gated)],
        },
        "checked_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }


if __name__ == "__main__":
    t = time.time()
    p = _build()
    st = p["stats"]
    print("耗时 %.1fs 池 %d 预筛 %d 深算 %d 主榜 %d"
          % (time.time() - t, st["universe"], st["candidates"], st["deep"], len(p["list"])))
    print("市场", p["market"]["name"], p["market"]["approx"])
    print("结论分布", p["counts"])
    for r in p["list"][:8]:
        print("  %-6s %-6s 综合 %3d 战法 %3d %s 命中 %s"
              % (r["code"], r["name"], r["score"], r["tactic_score"],
                 r["verdict_with_checkup"]["verdict"],
                 "/".join(h["name"] for h in r["tactic_hits"][:3])))
