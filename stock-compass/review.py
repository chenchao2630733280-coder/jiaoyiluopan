"""复盘胜率闭环（review.py）。

口径（与页面「历史 Top10 / 观察席」字样对齐，页面上不解释的部分都写在这里）：

1. **信号从哪来**：本复刻版没有原站的每日榜单存档，所以用引擎自己的体检函数
   `engine.checkup()` 沿时间轴回放 —— 对每个采样交易日 D，把每只候选票的日K
   截断到 D 当天，重算十二维体检，按总分排序取 **前 10 名 = 体检榜 Top10**、
   **第 11~20 名 = 观察席**，共 20 条信号/日。
2. **收益怎么算**：信号日 D 的收盘价买入，第 D+5 / D+10 / D+20 个交易日收盘价卖出，
   不复权、不计手续费与滑点。K 线不足窗口的记入 stale_signals，不进胜率。
3. **为什么要先等一会儿**：全市场×多个历史日是一次 1.4 万次体检的重算，跑一次约
   半分钟。结果按交易日缓存在 state/review.json，同一交易日只算一次。
4. **不能当成策略收益**：没有剔除涨停买不进、没有考虑停牌、没有做行业中性，
   它只能回答「体检这套打分在历史上选出来的票，后面 5/10/20 日整体是不是正收益」。
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
CACHE_FILE = os.path.join(STATE_DIR, "review.json")

# --- 回放参数 ---------------------------------------------------------------
# UNIVERSE_N 是候选池大小：按流通市值降序取前 N 只（盘前成交额为 0，用市值更稳）。
# STEP 是采样间隔：每 STEP 个交易日取一个信号日，够密又不至于跑太久。
# 600 只 × 24 个交易日 ≈ 1.44 万次体检 ≈ 30 秒（单核实测 2.1ms/次）。
UNIVERSE_N = 600
LOOKBACK = 121          # 回看的交易日根数（含信号日与 20 日窗口）
STEP = 5
TOP_N = 10              # 体检榜 Top10
WATCH_N = 10            # 观察席（第 11~20 名）
WINDOWS = (5, 10, 20)
MIN_BARS = 160          # 单票至少要这么多根 K 线才参与回放

_ST = {"building": False, "phase": "", "done": 0, "total": 0,
       "started": 0.0, "error": "", "td": ""}
_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# 回放
# ---------------------------------------------------------------------------

def _universe():
    """候选票：流通市值降序 + 有足够日K + 非 ST。返回 [(code, name, rows, date->idx)]。"""
    snap = engine.snap_index()
    cand = [r for r in snap.values()
            if engine.is_a_share(r["code"]) and not engine.is_st(r.get("name") or "")]
    cand.sort(key=lambda r: -(r.get("nmc_yi") or r.get("mv_yi") or 0))

    out = []
    for r in cand:
        if len(out) >= UNIVERSE_N:
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
    """交易日历：取候选池前 40 只的日期并集（大市值票极少停牌，并集不会缺日）。"""
    dates = set()
    for _, _, rows, _ in universe[:40]:
        for row in rows[-LOOKBACK:]:
            dates.add(row[0])
    return sorted(dates)[-LOOKBACK:]


def _forward(rows, i, days):
    """信号日 i 收盘买入，i+days 收盘卖出，返回百分比收益；不足返回 None。"""
    if i + days >= len(rows):
        return None
    base = rows[i][2]
    if not base:
        return None
    return (rows[i + days][2] / base - 1.0) * 100.0


def _build(force=False):
    t0 = time.time()
    with _LOCK:
        _ST.update({"building": True, "phase": "准备候选池", "done": 0, "total": 0,
                    "started": t0, "error": "", "td": ""})
    try:
        universe = _universe()
        cal = _calendar(universe)
        # 信号日：从最近往前每 STEP 天取一个；去掉末尾不足窗口的日子交给 stale 统计
        days = list(range(len(cal) - 1, -1, -STEP))
        days = sorted(days)
        with _LOCK:
            _ST.update({"phase": "回放体检", "total": len(days) * len(universe)})

        signals = []
        # 基准：同一批信号日、同一候选池的「等权买入并持有」，用来判断信号是跑赢还是跑输大市。
        # 不加这个基准，-2% 的平均收益分不清是打分不行还是这段时间大盘就在跌。
        bench = {w: [] for w in WINDOWS}
        for di in days:
            d = cal[di]
            day_rows = []
            for code, name, rows, idx in universe:
                i = idx.get(d)
                if i is None or i < MIN_BARS - 1:
                    continue
                f = engine.features(code, rows[:i + 1], None)
                if not f:
                    continue
                ck = engine.checkup(f)
                fr = {}
                for w in WINDOWS:
                    fr["r%d" % w] = _forward(rows, i, w)
                    if fr["r%d" % w] is not None:
                        bench[w].append(fr["r%d" % w])
                day_rows.append({
                    "code": code, "name": name, "date": d, "i": i,
                    "score": ck["score"], "strategy": ck["strategy_score"],
                    "verdict": ck["verdict"], "level": ck["level_key"], "fr": fr,
                })
                with _LOCK:
                    _ST["done"] += 1
            day_rows.sort(key=lambda x: (-x["score"], -x["strategy"]))
            top = day_rows[:TOP_N]
            watch = day_rows[TOP_N:TOP_N + WATCH_N]
            for rank, s in enumerate(top + watch):
                s["pool"] = "体检榜 Top10" if rank < TOP_N else "观察席"
                s["rank"] = rank + 1
                s.pop("fr", None)
                signals.append(s)

        # 前向收益：必须用**全量**日K（信号日之后的 K 线才在截断序列之外）
        by_code = {c: rows for c, _, rows, _ in universe}
        for s in signals:
            rows = by_code.get(s["code"]) or []
            for w in WINDOWS:
                s["r%d" % w] = _forward(rows, s["i"], w)

        payload = _aggregate(signals, cal, time.time() - t0, bench)
        os.makedirs(STATE_DIR, exist_ok=True)
        tmp = CACHE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False)
        os.replace(tmp, CACHE_FILE)
        with _LOCK:
            _ST.update({"building": False, "phase": "完成", "td": payload["td"]})
        return payload
    except Exception as e:
        with _LOCK:
            _ST.update({"building": False, "phase": "失败",
                        "error": "%s: %s" % (type(e).__name__, e)})
        raise


# ---------------------------------------------------------------------------
# 统计
# ---------------------------------------------------------------------------

def _stat(vals):
    vals = [v for v in vals if v is not None]
    n = len(vals)
    if not n:
        return {"samples": 0, "win_rate": 0.0, "avg_return": 0.0,
                "avg_win": 0.0, "avg_loss": 0.0, "profit_factor": None}
    wins = [v for v in vals if v > 0]
    loss = [v for v in vals if v <= 0]
    gross_win = sum(wins)
    gross_loss = -sum(loss)
    return {
        "samples": n,
        "win_rate": round(len(wins) / n * 100.0, 1),
        "avg_return": round(sum(vals) / n, 2),
        "avg_win": round(gross_win / len(wins), 2) if wins else 0.0,
        "avg_loss": round(gross_loss / len(loss) * -1, 2) if loss else 0.0,
        "profit_factor": round(gross_win / gross_loss, 2) if gross_loss > 0 else None,
    }


def _aggregate(signals, cal, cost, bench=None):
    bench = bench or {}
    windows = []
    for w in WINDOWS:
        st = _stat([s.get("r%d" % w) for s in signals])
        st["days"] = w
        windows.append({
            "days": w,
            "samples": st["samples"],
            "win_rate": st["win_rate"],
            "avg_return": st["avg_return"],
            "avg_win": st["avg_win"],
            "avg_loss": st["avg_loss"],
            "profit_factor": st["profit_factor"],
        })

    by_verdict = {}
    for vd in ("可介入", "观察", "回避"):
        sub = [s for s in signals if s["verdict"] == vd]
        if not sub:
            continue
        bucket = {}
        for w in WINDOWS:
            st = _stat([s.get("r%d" % w) for s in sub])
            bucket["%d 日" % w] = {"samples": st["samples"], "win_rate": st["win_rate"],
                                   "avg_return": st["avg_return"]}
        by_verdict[vd] = bucket
    # 兜底：出现意料之外的结论也照样分组，不吞数据
    for vd in sorted(set(s["verdict"] for s in signals)):
        if vd in by_verdict:
            continue
        sub = [s for s in signals if s["verdict"] == vd]
        bucket = {}
        for w in WINDOWS:
            st = _stat([s.get("r%d" % w) for s in sub])
            bucket["%d 日" % w] = {"samples": st["samples"], "win_rate": st["win_rate"],
                                   "avg_return": st["avg_return"]}
        by_verdict[vd] = bucket

    total = len(signals)
    usable = sum(1 for s in signals if s.get("r%d" % max(WINDOWS)) is not None)
    dates = sorted(set(s["date"] for s in signals))
    verdicts = {}
    for s in signals:
        verdicts[s["verdict"]] = verdicts.get(s["verdict"], 0) + 1

    bk = {}
    for w in WINDOWS:
        vals = [v for v in (bench.get(w) or []) if v is not None]
        if vals:
            bk[str(w)] = {"samples": len(vals),
                          "up_rate": round(sum(1 for v in vals if v > 0) * 100.0 / len(vals), 1),
                          "avg_return": round(sum(vals) / len(vals), 2)}

    return {
        "ok": True,
        "td": cal[-1] if cal else "",
        "built_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "windows": windows,
        "by_verdict": by_verdict,
        "total_signals": total,
        "usable_signals": usable,
        "stale_signals": total - usable,
        "date_range": [dates[0] if dates else "", dates[-1] if dates else ""],
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "verdict_counts": verdicts,
        "bench": bk,
        "meta": {
            "rule": "每个采样日按十二维体检总分排序，前 10 名记体检榜 Top10、第 11~20 名记观察席；"
                    "信号日收盘买入，第 5/10/20 个交易日收盘卖出，不复权、不计费用。",
            "universe": UNIVERSE_N, "lookback": LOOKBACK, "step": STEP,
            "signal_days": len(set(s["date"] for s in signals)),
            "cost_sec": round(cost, 1),
        },
        "hint": "回放口径：每 %d 个交易日采样一次、市值前 %d 只、每日 20 条信号；"
                "未剔除涨停买不进与停牌，仅用于检验打分方向，不代表可实现收益。" % (STEP, UNIVERSE_N),
    }


# ---------------------------------------------------------------------------
# 对外
# ---------------------------------------------------------------------------

def _fresh(payload, td):
    return bool(payload) and payload.get("td") == td


def _load():
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


def status():
    with _LOCK:
        return dict(_ST)


def api_review(params):
    td = engine.latest_trade_date()
    cache = _load()
    if _fresh(cache, td):
        return cache

    # 缓存过期/不存在：起后台重建，等 6 秒看能不能赶上
    with _LOCK:
        building = _ST["building"]
    if not building:
        threading.Thread(target=_build, kwargs={"force": True}, daemon=True).start()
    deadline = time.time() + 6.0
    while time.time() < deadline:
        with _LOCK:
            if not _ST["building"] and _ST["error"]:
                return {"ok": False, "error": "复盘统计失败", "hint": _ST["error"]}
        c = _load()
        if _fresh(c, td):
            return c
        time.sleep(0.2)

    st = status()
    pct = int(st["done"] * 100.0 / st["total"]) if st["total"] else 0
    return {
        "ok": False,
        "error": "正在回放历史体检（%s %d%%），首次统计约需半分钟。" % (st["phase"] or "排队", pct),
        "hint": "约 30 秒后再点一次「刷新复盘」即可看到结果；同一交易日内只需算一次。",
        "building": True,
    }


if __name__ == "__main__":
    t = time.time()
    p = _build()
    print("耗时 %.1fs  信号 %d 条 %s~%s" % (time.time() - t, p["total_signals"],
                                          p["date_range"][0], p["date_range"][1]))
    for w in p["windows"]:
        print("  %2d 日: 样本 %4d 胜率 %5.1f%% 均值 %+.2f%% 盈亏比 %s"
              % (w["days"], w["samples"], w["win_rate"], w["avg_return"], w["profit_factor"]))
    print("  结论分布", p["verdict_counts"])
