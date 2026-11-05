# A股右侧交易罗盘 · 项目长期备忘

## 仓库与部署约定

- 远程：`git@github.com:chenchao2630733280-coder/jiaoyiluopan.git`（走 **SSH**，
  HTTPS 无凭据会 401）。`push.default=simple`，裸 `git push` / `git pull` 可用。
- **仓库保持 public，`.workbuddy/memory/*.md` 保留在仓库中**（用户 2026-11-05 明确决定，
  以后不要再提议撤掉或改 private）。
- `data/kline`、`data/*.json` 缓存、`state/`、`desktop/dist*`、`node_modules` 不入库（见 .gitignore）。
- 提交与推送**分成两条命令**：同一条 `&&` 链里 push 会静默不执行。
- `data/index.json`（只存 td/checked_ts/prev）已移出版本库 —— 它是运行时状态，
  留在索引里会让工作区永远是脏的（2026-11-05 处理）。

## 「六个池」有两套，别再混

- **罗盘右侧六池**：`engine.pool_hit` 实现，id = `breakout / boll / yearline /
  macd / platform / pullback`，用于 `/api/compass`、`/api/rightside`。
- **短线六池**：`short.py` 的 `POOL_DEFS` 实现，id = `breakout / limit_up /
  leader / gap / pullback / divergence`（①突破 ②涨停接力 ③热点龙头 ④缺口
  ⑤回踩观察 ⑥底背离），用于 `/api/short`。
- 两套都叫「六个池」但只有 `breakout` 与 `pullback` 重名，**条件也不同**。
  上一轮就是因为把罗盘六池当成短线六池，才在 `short.py` 里自造了一套原站没有的池。
- 权威基准存档在 `stock-compass/_short_remote.json`（原站 `/api/short` 全量返回，
  含每池 `scan` 条件文本、个股全部字段形状）。**`scan` 文本就是合同**，改条件必须两处一起改。

## 数据口径陷阱（踩过，别再踩）

- `engine.features` 的 `hi20/hi60/vol_ma20` **含当日**。判「突破 20 日新高」「缩量」
  必须跟**不含当日**的窗口比（`short._hi_ex_today` / `_vol20_prev`），否则要么恒不成立、
  要么把「缩量」判轻。
- 东财 `push2/clist` 单页硬上限 100 条，大板块必须翻页；**并发下会 RemoteDisconnected
  返回「看着完整的半份数据」**。并发只用来省时间，正确性靠「空页串行补抓 + 取不齐整块重抓」兜。
- 模块级 `except: pass` 会把 `NameError` 吞成「今日 0 只命中」，页面上看起来像行情结论。
  逐池异常必须计数并上报（`pools_note` + 前端提示条）。
- **「原站的 scan 文本」≠「原站的实现」**：底背离 scan 写「60日价格新低」，
  按字面卡「当日就是最低那根」实测 4998 只命中 **0** 只（最低点那天 DIF 几乎都还在
  DEA 下方，金叉要等反弹一两天），但原站该池有 6 只。→ 原站允许新低发生在最近几天内，
  本地取 `DIVERGENCE_NEWLOW_WINDOW=5`（近 5 日内新低 = 9 只，同量级）。
  **遇到「按字面实现恒为 0、原站却有票」时，先怀疑窗口口径，别急着自创判据。**

## 别自拟标签：原站有什么就只实现什么

- `_maform` 只有三式：**出水芙蓉 / 多方炮 / 断头铡刀**（+ 无均线形态），
  `grade_text` 必带发生日，取回溯 `MA_FORM_LOOKBACK=10` 根内**最新**一次命中
  （不是优先级最高那次 —— 10-30 断头、11-02 又出水芙蓉，还挂断头方向会反）。
- `ind15` 是原站 **15 项**（6 支柱 + 6 未来 + 3 基础盘），不是自拟的 16 项。
  自拟的「工业母机与高端仪器 / 基础软件与工业软件 / 先进材料 / 人工智能与算力」
  跟原站划分对不上，已整表改回。
- `position_health` 的 `box.level` 是「**最近 6 箱里的序号**」（`len(history)`），
  不是历史总箱数；`gann.retr_pct` 可为负、可 >100。

## 代码手术坑

- **按 `def` 切区间替换会吃掉紧随其后的模块级语句**：`_hit_divergence` 后面紧跟
  `_HIT = {...}` 分派表，整函数替换时被一起删掉 → `/api/short` 直接
  `NameError: name '_HIT' is not defined`。`py_compile` 查不出名字缺失，
  **替换后必须跑一次真实接口**。
- 字典回退表（如 `FALLBACK_NAME`）的 key 种类搞错（产业 id 当股票代码）时，
  `get()` 永远 None，回退等于没写；**快照齐全时完全看不出来**，停牌那天才炸。
  生成后必须断言「所有 key 都是 6 位代码」且「覆盖全部 code」。

