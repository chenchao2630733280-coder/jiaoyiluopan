"""边缘验证（edge.py）—— 检验「策略适配分 80 分线」到底有没有区分度。

为什么单独做这个
----------------
体检给出「可介入 / 观察 / 回避」三档，这是全站所有结论的源头。但「这条线准不准」
从来没有被检验过：如果可介入档的后续收益并不比回避档高，那整套评分就是安慰剂。

这个模块把「体检」倒回历史：在过去的某一天，只喂它那一天及之前的 K 线
（`rows[:i+1]`，物理上不可能看到未来），拿到当时的评分与结论，再对照真实的前向收益。
口径全部写在下面，页面上不展开的部分都以这里为准。

口径
----
1. **无偏池**：只按**代码层面**过滤（非 ST、非退市、非北交所、A 股），
   不用任何行情字段筛（不用市值/涨幅/换手挑样本）—— 用了就是前视偏差。
   排序取市值前 N 只是为了把池子压到可计算规模，不影响口径。

2. **超额收益**：`ex = 个股 T+n 收益 − 当日全样本均值`。
   减掉当日均值等于剔除大盘与样本期整体牛熊的影响 —— 否则熊市里所有分档都是负的，
   看不出「谁比谁好」。这是相对强弱的度量，不是绝对收益。

3. **维度区分度**：某维按得分中位数切高低两组，`spread = 高组 ex5 − 低组 ex5`。
   spread ≈ 0 或为负 = 这一维在与别的维数量同一件事（共线），权重被重复计算。

4. **权重反事实搜索**：穷举若干维度组合（等权、阈值固定 80），按验证日切成
   前一半训练 / 后一半留出：训练段选最优，再看它在留出段是否还为正。
   留出段转负 = 过拟合，不予采纳。

5. **环境分层**：把验证日按「当日全样本均值」的正负分成涨日/跌日，
   看各档超额符号是否随大盘方向翻转。这能区分「策略失效」与「样本期偏熊」。

已知局限（页面上也会显示）
--------------------------
- 盘口维度需要实时数据，历史不可得 → 该维恒缺，区分度表里会标「历史不可得」。
- 市场温度同理，`sentiment` 维在回放中拿不到当日的真实温度。
- 验证日集中在最近一段时间，未覆盖完整牛熊。
- 未计交易成本、涨跌停不可成交。
"""
import json
import os
import threading
import time

import engine

# --- 参数 -------------------------------------------------------------------
POOL_N = 400          # 无偏池大小（按市值降序取，只为压规模，不参与筛选口径）
N_DAYS = 10           # 验证日个数（前端文案写的是 10 个验证日）
DAY_STEP = 6          # 验证日间隔（交易日）
LOOKBACK = 160        # 回看 K 线根数
MIN_BARS = 160        # 单票最少 K 线根数
WINDOWS = (5, 10)     # 前向收益窗口
THRESHOLD = 80        # 权重搜索固定阈值
COMBO_MAX = 2         # 组合最多几维（3 维以上候选爆炸且无信息量）

STATE_DIR = os.path.join(os.environ.get("COMPASS_STORE") or
                         os.path.dirname(os.path.abspath(__file__)), "state")
CACHE_FILE = os.path.join(STATE_DIR, "edge.json")

_ST = {"building": False, "phase": "", "done": 0, "total": 0,
       "started": 0.0, "error": "", "td": ""}
_LOCK = threading.Lock()

DISCLAIMER = ("本页是对历史样本的统计检验，样本期与股票池都会影响结论；"
              "结论不构成投资建议，也不自动改动体检的打分与判定。")


# ---------------------------------------------------------------------------
# 基础
# ---------------------------------------------------------------------------

def _universe():
    """无偏池：代码层面过滤 + 足够 K 线。返回 [(code, name, rows, date->idx)]。"""
    snap = engine.snap_index()
    cand = [r for r in snap.values()
            if engine.is_a_share(r["code"]) and not engine.is_st(r.get("name") or "")]
    cand.sort(key=lambda r: -(r.get("nmc_yi") or r.get("mv_yi") or 0))

    out = []
    for r in cand:
        if len(out) >= POOL_N:
            break
        code = r["code"]
        rows = engine.kline_get(code) or []
        if len(rows) < MIN_BARS:
            continue
        idx = {}
        for i, row in enumerate(rows):
            idx[row[0]] = i
        out.append((code, r.get("name") or code, rows, idx))
    return out


def _calendar(universe):
    dates = set()
    for _, _, rows, _ in universe[:40]:
        for row in rows[-LOOKBACK:]:
            dates.add(row[0])
    return sorted(dates)[-LOOKBACK:]


def _forward(rows, i, days):
    """信号日 i 收盘买入、i+days 收盘卖出，返回百分比收益；越界返回 None。"""
    if i + days >= len(rows):
        return None
    base = rows[i][2]
    if not base:
        return None
    try:
        return (rows[i + days][2] / base - 1.0) * 100.0
    except (TypeError, ZeroDivisionError):
        return None


def _avg(vals):
    v = [x for x in vals if x is not None]
    return round(sum(v) / len(v), 3) if v else None


def _blk(vals):
    """一组收益的统计块：样本、胜率、均值。前端按 {n, win, avg} 取。"""
    v = [x for x in vals if x is not None]
    if not v:
        return None
    return {"n": len(v),
            "win": round(100.0 * sum(1 for x in v if x > 0) / len(v), 1),
            "avg": round(sum(v) / len(v), 3)}


# ---------------------------------------------------------------------------
# 构建
# ---------------------------------------------------------------------------

def _build(force=False):
    t0 = time.time()
    with _LOCK:
        _ST.update({"building": True, "phase": "准备无偏池", "done": 0, "total": 0,
                    "started": t0, "error": "", "td": ""})
    try:
        universe = _universe()
        if len(universe) < 50:
            raise RuntimeError("可用样本不足（只有 %d 只），请先等全市场扫描完成" % len(universe))
        cal = _calendar(universe)

        # 验证日：从末尾往前每 DAY_STEP 天取一个，且必须留出 WINDOWS[-1] 天给前向收益
        last = len(cal) - 1 - max(WINDOWS)
        days = [cal[i] for i in range(last, -1, -DAY_STEP)][:N_DAYS]
        days = sorted(days)
        if len(days) < 3:
            raise RuntimeError("可用验证日不足（%d 个），日 K 缓存可能还没铺满" % len(days))

        with _LOCK:
            _ST.update({"phase": "回放体检", "total": len(days) * len(universe)})

        # 全量 K 线按代码索引一次即可 —— 早先在信号循环里每只票重建一次字典，
        # 400 只 × 10 天就是 4000 次建表，纯属浪费。
        by_code = {c: rows for c, _, rows, _ in universe}

        samples = []
        for d in days:
            day = []
            for code, name, rows, idx in universe:
                i = idx.get(d)
                if i is None or i < MIN_BARS - 1:
                    continue
                # ★ 只喂到信号日为止 —— 物理上看不到未来，这是「无前视」的关键
                f = engine.features(code, rows[:i + 1], None)
                if not f:
                    continue
                try:
                    ck = engine.checkup(f)
                except Exception:
                    continue
                dims = {x["key"]: x["score"] for x in ck.get("dims") or []}
                day.append({
                    "code": code, "name": name, "date": d, "i": i,
                    "score": ck.get("checkup_score") or ck.get("score") or 0,
                    "strategy": ck.get("strategy_score") or 0,
                    "verdict": ck.get("verdict") or "观察",
                    "level": ck.get("level_key") or "",
                    "dims": dims,
                })
                with _LOCK:
                    _ST["done"] += 1

            # 前向收益（用全量 rows，此时信号日之后的 K 线才在视野里）
            for s in day:
                rows = by_code.get(s["code"]) or []
                for w in WINDOWS:
                    s["r%d" % w] = _forward(rows, s["i"], w)
            # 当日全样本均值 → 超额。这是剔除大盘影响的基准
            for w in WINDOWS:
                base = _avg([s.get("r%d" % w) for s in day])
                s_key = "ex%d" % w
                r_key = "r%d" % w
                for s in day:
                    v = s.get(r_key)
                    s[s_key] = (round(v - base, 3) if (v is not None and base is not None) else None)
            # 当日整体涨跌 → 用于环境分层
            day_ret = _avg([s.get("r5") for s in day])
            for s in day:
                s["mkt"] = "up" if (day_ret or 0) > 0 else "down"
            samples.extend(day)

        if not samples:
            raise RuntimeError("没有有效样本（可能是日 K 缓存不完整）")

        payload = _aggregate(samples, days, universe, time.time() - t0)
        os.makedirs(STATE_DIR, exist_ok=True)
        engine.write_json(CACHE_FILE, payload)
        with _LOCK:
            _ST.update({"building": False, "phase": "完成", "error": "",
                        "td": engine.latest_trade_date()})
        return payload
    except Exception as e:
        with _LOCK:
            _ST.update({"building": False, "phase": "失败", "error": str(e)})
        raise


def _aggregate(samples, days, universe, elapsed):
    summary = {}

    # 1) 三档结论的真实表现（绝对 h 与超额 ex 都给）
    by_verdict = {}
    for k in ("可介入", "观察", "回避"):
        rows = [s for s in samples if s["verdict"] == k]
        if not rows:
            continue
        blk = {"n": len(rows)}
        for w in WINDOWS:
            blk["h%d" % w] = _blk([s.get("r%d" % w) for s in rows])
            blk["ex%d" % w] = _blk([s.get("ex%d" % w) for s in rows])
        by_verdict[k] = blk
    summary["by_verdict_excess"] = by_verdict

    # 2) 位置态
    by_level = {}
    for k in ("pos3", "pos2", "pos1"):
        rows = [s for s in samples if s["level"] == k]
        if not rows:
            continue
        by_level[k] = {"n": len(rows),
                       "ex5": _blk([s.get("ex5") for s in rows]),
                       "ex10": _blk([s.get("ex10") for s in rows])}
    summary["by_level"] = by_level

    # 3) 环境分层：各档 × 涨日/跌日
    env_layered = {}
    for k in ("可介入", "观察", "回避"):
        rows = [s for s in samples if s["verdict"] == k]
        if not rows:
            continue
        down = [s for s in rows if s["mkt"] == "down"]
        up = [s for s in rows if s["mkt"] == "up"]
        env_layered[k] = {
            "down": {"n": len(down), "ex5": _blk([s.get("ex5") for s in down])},
            "up": {"n": len(up), "ex5": _blk([s.get("ex5") for s in up])},
        }
    summary["env_layered"] = env_layered
    summary["env"] = {
        "down_days": [s["date"] for s in samples if s["mkt"] == "down"],
        "up_days": [s["date"] for s in samples if s["mkt"] == "up"],
    }
    # 去重（按日，不是按样本）
    summary["env"]["down_days"] = sorted(set(summary["env"]["down_days"]))
    summary["env"]["up_days"] = sorted(set(summary["env"]["up_days"]))

    # 4) 各维区分度：中位数切高低组，比 ex5
    keys = [k for k, _, _ in engine.DIMS]
    names = {k: n for k, n, _ in engine.DIMS}
    disc = []
    for k in keys:
        vals = [(s["dims"].get(k), s.get("ex5")) for s in samples]
        vals = [(v, e) for v, e in vals if v is not None and e is not None]
        if len(vals) < 60:
            disc.append({"key": k, "name": names.get(k, k), "n": len(vals),
                         "note": "样本不足，未统计", "spread": None, "useless": True})
            continue
        distinct = len(set(v for v, _ in vals))
        if distinct <= 2:
            # 该维在历史回放里几乎是常数（盘口/市场温度需要实时数据）
            disc.append({"key": k, "name": names.get(k, k), "n": len(vals),
                         "note": "历史不可得（该维依赖实时数据，回放中恒为缺省值）",
                         "spread": None, "useless": True})
            continue
        vs = sorted(vals, key=lambda t: t[0])
        mid = len(vs) // 2
        lo = _avg([e for _, e in vs[:mid]])
        hi = _avg([e for _, e in vs[mid:]])
        spread = round(hi - lo, 3) if (hi is not None and lo is not None) else None
        disc.append({"key": k, "name": names.get(k, k), "n": len(vals),
                     "spread": spread, "useless": (spread is None or spread <= 0.05)})
    disc.sort(key=lambda x: -(x["spread"] if x["spread"] is not None else -9))
    summary["dim_discrimination"] = disc

    # 5) 权重反事实搜索
    weights = _search_weights(samples, days, [k for k in keys if k != "pankou"])

    summary["n"] = len(samples)
    return {
        "ok": True,
        "validate": {
            "summary": summary,
            "dates": days,
            "generated_at": time.strftime("%Y-%m-%d %H:%M"),
            "elapsed_sec": round(elapsed, 1),
            "disclaimer": DISCLAIMER,
        },
        "weights": weights,
        "universe": {"n": len(universe)},
        "td": engine.latest_trade_date(),
    }


def _search_weights(samples, days, pool_keys):
    """穷举维度组合（等权、阈值 THRESHOLD），训练段选优、留出段验证。

    为什么用等权而不是原权重：这是「反事实」搜索 —— 想知道换一种加权方式会不会更好，
    沿用原权重就测不出差异了。阈值固定 80 是为了和页面上的「80 分线」同口径比较。
    """
    combos = []
    n = len(pool_keys)
    for i in range(n):
        combos.append((pool_keys[i],))
    if COMBO_MAX >= 2:
        for i in range(n):
            for j in range(i + 1, n):
                combos.append((pool_keys[i], pool_keys[j]))
    if COMBO_MAX >= 3:
        for i in range(n):
            for j in range(i + 1, n):
                for k in range(j + 1, n):
                    combos.append((pool_keys[i], pool_keys[j], pool_keys[k]))

    half = len(days) // 2
    train_days = set(days[:half])
    test_days = set(days[half:])

    cands = []
    for ks in combos:
        tr, te, fu = [], [], []
        touched = 0
        for s in samples:
            ds = s["dims"]
            if any(ds.get(k) is None for k in ks):
                continue
            sc = sum(ds[k] for k in ks) / float(len(ks))
            if sc < THRESHOLD:
                continue
            touched += 1
            e = s.get("ex10")
            if e is None:
                continue
            fu.append(e)
            (tr if s["date"] in train_days else te).append(e)
        if len(fu) < 30:
            continue
        cands.append({
            "keys": [names_of(k) for k in ks],
            "key_ids": list(ks),
            "train": {"ex10": _blk(tr)},
            "test": {"ex10": _blk(te)},
            "full": {"n": len(fu), "ex10": _blk(fu)},
            "hit": touched,
        })

    def avg_of(blk):
        v = (blk or {}).get("ex10") or {}
        return v.get("avg")

    best = None
    best_v = None
    for c in cands:
        v = avg_of(c["train"])
        if v is None:
            continue
        if best_v is None or v > best_v:
            best, best_v = c, v

    cands.sort(key=lambda c: -(c["full"]["n"] or 0))
    return {"candidates": cands, "best_on_train": best, "threshold": THRESHOLD,
            "train_days": sorted(train_days), "test_days": sorted(test_days)}


def names_of(key):
    for k, n, _ in engine.DIMS:
        if k == key:
            return n
    return key


# ---------------------------------------------------------------------------
# 对外
# ---------------------------------------------------------------------------

def _load():
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


def status():
    with _LOCK:
        return dict(_ST)


def api_edge(params):
    """前端三种调用：默认读结果、refresh=1 重跑、status=1 查进度。"""
    if params.get("status"):
        st = status()
        return {"ok": True, "running": st["building"], "phase": st["phase"],
                "done": st["done"], "total": st["total"], "error": st["error"]}

    if params.get("refresh"):
        with _LOCK:
            if _ST["building"]:
                return {"ok": True, "running": True}
            _ST.update({"error": ""})
        threading.Thread(target=_build, kwargs={"force": True}, daemon=True).start()
        return {"ok": True, "running": True}

    cache = _load()
    if cache:
        # 缓存存在但数据落后于交易日：附带过期提示，页面照常显示旧结论
        td = engine.latest_trade_date()
        if cache.get("td") != td:
            cache["stale"] = True
        return cache

    st = status()
    if not st["building"]:
        threading.Thread(target=_build, kwargs={"force": True}, daemon=True).start()
    # 没有结果时返回空壳 —— 前端会渲染「尚无验证结果，点重新验证」
    return {"ok": True, "running": True, "validate": {"summary": {}}, "weights": {},
            "universe": {}, "td": ""}


if __name__ == "__main__":
    t = time.time()
    p = _build()
    s = p["validate"]["summary"]
    print("耗时 %.1fs 样本 %d 条 验证日 %d 个"
          % (time.time() - t, s["n"], len(p["validate"]["dates"])))
    for k, v in s["by_verdict_excess"].items():
        print("  %-4s n=%-5d T+5 超额 %s  T+10 超额 %s"
              % (k, v["n"], (v.get("ex5") or {}).get("avg"), (v.get("ex10") or {}).get("avg")))
    print("  维度区分度 Top3:",
          [(d["name"], d["spread"]) for d in s["dim_discrimination"][:3]])
