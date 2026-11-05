"""实时雷达（liveradar.py）：盘中每 10 秒一轮的「可介入 / 观察」滚动扫描。

口径（页面上不展开的都写在这里）：

1. **一轮做什么**：刷快照 → 取成交额前 800 只做 18 式浅算 → 按战法分取前 60 只补
   12 维体检测算 → 输出结论为「可介入 / 观察」的票，附带买点计划与缠论买卖点。
   与本页同名的那块表（实盘信号）字段完全复用 tactics.evaluate 的返回值，
   不另造一套 —— 两处口径必须一致，否则同一个票在两个页签给出不同结论。
2. **为什么 10 秒能跑完**：快照本身有 120 秒缓存、日 K 是本地磁盘文件，真正花时间的
   只有 features() 与 checkup() 两次纯计算；800 只浅算约 1~2 秒，60 只深算不到 0.2 秒。
   收盘后自动降频到 60 秒一轮（数据不会变，没必要空转）。
3. **new_signals / exited**：只在「可介入」这一档上做差分。观察席天天进出，做差分会
   把「本轮新出现」刷成一屏噪音，反而淹没真正值得看一眼的信号。
4. **不做的事**：不下单、不做仓位变动建议、不预测。所有价位来自 engine.make_plan 的
   同一套规则（ATR 与结构位取更紧的一侧，8% 封顶）。
"""
import threading
import time

import engine
import tactics

POOL_N = 800        # 浅算池：快照成交额前 N
DEEP_N = 60         # 深算上限：战法分前 N 补 12 维体检
CADENCE_OPEN = 10   # 盘中节奏（秒）
CADENCE_IDLE = 60   # 非交易时段节奏

_LOCK = threading.Lock()
_TH = {"thread": None, "alive": False}
_ST = {"ready": False, "payload": None, "round": 0,
       "prev_ok": set(), "error": ""}


def _session():
    """返回 (是否盘中, 时段文案)。盘中指 9:25~11:30 与 13:00~15:00。"""
    now = time.localtime()
    hm = now.tm_hour * 60 + now.tm_min
    wd = now.tm_wday
    if wd >= 5:
        return False, "周末休市"
    if 9 * 60 + 15 <= hm < 9 * 60 + 25:
        return False, "集合竞价"
    if 9 * 60 + 25 <= hm <= 11 * 60 + 30:
        return True, "交易中"
    if 11 * 60 + 30 < hm < 13 * 60:
        return False, "午间休市"
    if 13 * 60 <= hm <= 15 * 60:
        return True, "交易中"
    if hm < 9 * 60 + 15:
        return False, "盘前"
    return False, "收盘后"


def _bs_type(bs):
    if not bs:
        return "", ""
    if "三买" in bs:
        return "buy3", bs
    if "二买" in bs:
        return "buy2", bs
    if "一买" in bs:
        return "buy1", bs
    if "三卖" in bs:
        return "sell3", bs
    if "二卖" in bs:
        return "sell2", bs
    if "一卖" in bs:
        return "sell1", bs
    return "", bs


def _pool():
    snap = engine.snap_index()
    rows = [r for r in snap.values()
            if engine.is_a_share(r["code"]) and not engine.is_st(r.get("name") or "")
            and (r.get("price") or 0) > 0]
    rows.sort(key=lambda r: -((r.get("amount") or 0) or (r.get("nmc_yi") or 0) * 1e8))
    return rows[:POOL_N], len(snap)


def _round():
    t_all = time.time()
    t0 = time.time()
    snap_rows, quoted = _pool()
    temp, mk = engine.market_temp()
    mk["_temp"] = temp
    t_snap = time.time() - t0

    t0 = time.time()
    shallow = []
    for sr in snap_rows:
        code = sr["code"]
        rows = engine.kline_get(code)
        if not rows or len(rows) < tactics.MIN_BARS:
            continue
        f = engine.features(code, rows, sr)
        if not f:
            continue
        row = tactics.evaluate(code, sr.get("name") or code, rows, sr, mk,
                               deep=False, f=f)
        if row:
            shallow.append((row, sr, rows))
    shallow.sort(key=lambda x: -x[0]["tactic_score"])
    t_shallow = time.time() - t0

    t0 = time.time()
    out = []
    for row, sr, rows in shallow[:DEEP_N]:
        full = tactics.evaluate(sr["code"], row["name"], rows, sr, mk,
                                deep=True)
        if not full:
            continue
        ck = full.get("checkup") or {}
        full["deep"] = bool(ck.get("verdict"))
        bs = ""
        # checkup 里没有透出缠论原文，单独取一次（同一套 features，成本可忽略）
        try:
            f = engine.features(sr["code"], rows, sr)
            ch = (engine.checkup(f, sr, mk.get("_temp")) or {}).get("chan") or {}
            bs = ch.get("bs") or ""
            full["chan_bs_desc"] = "%s · %s" % (ch.get("grade_text") or "", ch.get("divergence") or "")
        except Exception:
            ch = {}
        t, nm = _bs_type(bs)
        full["chan_bs_type"] = t
        full["chan_bs"] = nm
        if full["verdict"] in ("可介入", "观察"):
            # ★ 两档按「战法口径」分表，不按合成口径 —— 实盘页的结论列取的是 r.verdict，
            #   前端也是用 r.verdict 分到「可介入 / 观察」两张表。若这里用合成口径筛行，
            #   会出现「页面显示 5 只可介入、表里只有 4 行」的错账。合成口径只体现在
            #   12 维体检列与 ⚠分歧 标记上，两个口径都看得见。
            full.pop("_ts", None)      # 内部排序用的中间键，不出网
            out.append(full)
    t_deep = time.time() - t0

    out.sort(key=lambda r: (-r["score"], -r["tactic_score"]))

    ok_codes = set(r["code"] for r in out if r["verdict"] == "可介入")
    prev = _ST["prev_ok"]
    new_codes = ok_codes - prev
    gone = prev - ok_codes
    new_signals = [{"code": r["code"], "name": r["name"], "verdict": r["verdict"],
                    "chan_bs_type": r.get("chan_bs_type") or ""}
                   for r in out if r["code"] in new_codes]
    exited = [{"code": c} for c in sorted(gone)]

    opened, label = _session()
    n_ok = sum(1 for r in out if r["verdict"] == "可介入")
    payload = {
        "ok": True,
        "ready": True,
        "alive": bool(_TH["alive"]),
        "session": "open" if opened else "closed",
        "session_label": label,
        "generated_at": time.strftime("%H:%M:%S"),
        "stats": {
            "round": _ST["round"],
            "pool": len(snap_rows), "quoted": quoted,
            "candidates": len(shallow), "deep": len(out),
            "deep_cached": 0,
            "cost": {"total": round(time.time() - t_all, 2),
                     "snapshot": round(t_snap, 2),
                     "shallow": round(t_shallow, 2),
                     "deep": round(t_deep, 2)},
        },
        "opportunities": out[:40],
        "new_signals": new_signals,
        "exited": exited,
        "counts": {"可介入": n_ok, "观察": len(out) - n_ok},
        "message": "本轮扫描完成",
    }
    return payload, ok_codes


def _loop():
    while True:
        opened, _ = _session()
        try:
            _ST["round"] += 1
            payload, ok = _round()
            with _LOCK:
                _ST["payload"] = payload
                _ST["ready"] = True
                _ST["prev_ok"] = ok
                _ST["error"] = ""
        except Exception as e:
            with _LOCK:
                _ST["error"] = "%s: %s" % (type(e).__name__, e)
                if not _ST["payload"]:
                    _ST["payload"] = {"ok": False, "ready": False, "alive": True,
                                      "error": "雷达单轮失败：%s" % _ST["error"]}
        time.sleep(CADENCE_OPEN if opened else CADENCE_IDLE)


def start():
    """启动（或返回已在跑的）雷达线程。"""
    with _LOCK:
        th = _TH["thread"]
        if th and th.is_alive():
            return False
        th = threading.Thread(target=_loop, name="liveradar", daemon=True)
        _TH["thread"] = th
        _TH["alive"] = True
        if not _ST["payload"]:
            _ST["payload"] = {"ok": True, "ready": False, "alive": True,
                              "message": "首轮扫描中（约 3 秒）…",
                              "session_label": _session()[1],
                              "stats": {}, "opportunities": [],
                              "new_signals": [], "exited": []}
        th.start()
        return True


def api_liveradar(params):
    start()                      # 打开页面即自启，不需要用户先点按钮
    with _LOCK:
        p = dict(_ST["payload"] or {})
    p["alive"] = bool(_TH["thread"] and _TH["thread"].is_alive())
    if _ST["error"] and not p.get("ready"):
        p["error"] = _ST["error"]
    return p


def api_liveradarstart(params):
    started = start()
    return {"ok": True, "started": started,
            "alive": bool(_TH["thread"] and _TH["thread"].is_alive()),
            "message": "雷达已启动" if started else "雷达已在运行"}


if __name__ == "__main__":
    start()
    for _ in range(3):
        time.sleep(4)
        d = api_liveradar({})
        st = d.get("stats") or {}
        print("轮 %s 池 %s 候选 %s 深算 %s 成本 %s 可介入 %s 观察 %s" % (
            st.get("round"), st.get("pool"), st.get("candidates"), st.get("deep"),
            st.get("cost"), (d.get("counts") or {}).get("可介入"),
            (d.get("counts") or {}).get("观察")))
        for r in (d.get("opportunities") or [])[:5]:
            print("   %-6s %-6s %s 综合%d 战法%d %s" % (
                r["code"], r["name"], r["verdict_with_checkup"]["verdict"],
                r["score"], r["tactic_score"],
                "/".join(h["name"] for h in r["tactic_hits"][:2])))
