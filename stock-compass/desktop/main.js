/*
 * A股右侧交易罗盘 · 桌面客户端（Electron 主进程）
 *
 * 职责边界：这里**不碰任何业务逻辑**，只做四件事
 *   1. 拉起/复用一个本地 Python 服务（server.py），它才是真正的后端；
 *   2. 开一个无地址栏的原生窗口指向 http://127.0.0.1:PORT ；
 *   3. 关窗时优雅停服务（走 /api/shutdown，不硬杀，避免写坏 JSON）；
 *   4. 提供托盘、菜单、崩溃自愈这些"桌面应用才有的"东西。
 *
 * 为什么后端不搬到 Node：这套系统是 Python 标准库实现的 40 多个接口 + 5000 只票的
 * 本地计算，重写一遍等于重做项目。桌面端只做"外壳"，这是唯一理性的切法。
 */
const { app, BrowserWindow, Menu, Tray, dialog, shell, nativeImage } = require("electron");
const { spawn, spawnSync } = require("child_process");
const http = require("http");
const https = require("https");
const net = require("net");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");

// --- 路径与常量 -------------------------------------------------------------
// 开发态：main.js 在 desktop/ 下，项目根是上一级
// 打包态：Python 代码由 extraResources 复制到 resources/app，根目录不同
const ROOT = app.isPackaged
  ? path.join(process.resourcesPath, "app")
  : path.join(__dirname, "..");
const PORT = Number(process.env.COMPASS_PORT || 8092);
const PAGE = `http://127.0.0.1:${PORT}/index.html`;
const ICON = path.join(__dirname, "icon.ico");

// 可写数据目录（state/ 榜单缓存、data/ 行情、desktop.log 都落这里）
//
// ★ 便携 exe 的坑：electron-builder 的 portable 包每次启动都会把整个程序解压到
// %TEMP% 下的一个新目录，程序自己并不知道这件事。若把 data/state 留在解压目录，
// 等于每次打开都是空缓存 —— 重下 85MB 行情、重扫全市场 3 分钟，而且退出即删。
// PORTABLE_EXECUTABLE_DIR 是 portable 运行时注入的、exe 真正所在的目录，用它做锚点。
const PORTABLE_DIR = process.env.PORTABLE_EXECUTABLE_DIR || "";
const STORE_DIR = PORTABLE_DIR
  ? path.join(PORTABLE_DIR, "罗盘数据")
  : app.isPackaged
    ? app.getPath("userData")     // 安装版：%APPDATA%\A股右侧交易罗盘
    : ROOT;                       // 开发态：还是项目目录，不改变原有习惯

const STATE_DIR = path.join(STORE_DIR, "state");
const LOG_FILE = path.join(STATE_DIR, "desktop.log");

let win = null;
let tray = null;
let pyProc = null;      // 我们拉起的 Python 进程（复用别人的服务时为 null）
let owned = false;      // 服务是不是本实例起的 —— 决定关窗时要不要停它
let quitting = false;   // 主动退出中，避免 close 事件里重复走清理
let ready = false;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function log(...args) {
  const line = `[${new Date().toLocaleString("zh-CN")}] ${args.join(" ")}\n`;
  try {
    fs.mkdirSync(STATE_DIR, { recursive: true });
    fs.appendFileSync(LOG_FILE, line, "utf8");
  } catch (_) {
    // 数据目录写不进去（U 盘只读、权限不足）就退到系统临时目录，
    // 否则「启动即退」这类问题会连一行线索都留不下。
    try { fs.appendFileSync(path.join(require("os").tmpdir(), "compass-desktop.log"), line, "utf8"); } catch (_) {}
  }
  console.log(line.trim());
}

// 尽早留痕：任何后续判断（单实例锁、找 Python、端口占用）失败时，
// 用户只会看到窗口闪一下就没了，日志是唯一能复盘的东西。
log("=== 进程启动 ===", `packaged=${app.isPackaged}`,
    `portable=${PORTABLE_DIR || "(非便携)"}`, `root=${ROOT}`, `store=${STORE_DIR}`);

// --- 后端探测与拉起 ---------------------------------------------------------

/** 端口是否已经有人在监听（说明服务已起，直接复用，避免重复扫描 5000 只票）。 */
function portOpen(port, timeout = 800) {
  return new Promise((resolve) => {
    const sock = net.connect({ host: "127.0.0.1", port });
    let done = false;
    const finish = (ok) => {
      if (done) return;
      done = true;
      sock.destroy();
      resolve(ok);
    };
    sock.setTimeout(timeout);
    sock.once("connect", () => finish(true));
    sock.once("timeout", () => finish(false));
    sock.once("error", () => finish(false));
  });
}

/** GET 一个本地 JSON，失败返回 null。不用系统代理（Python 那边已经踩过这个坑）。 */
function getJson(pathname, timeout = 2000, method = "GET") {
  return new Promise((resolve) => {
    const req = http.request(
      { host: "127.0.0.1", port: PORT, path: pathname, method, timeout },
      (res) => {
        let buf = "";
        res.setEncoding("utf8");
        res.on("data", (d) => (buf += d));
        res.on("end", () => {
          try { resolve(JSON.parse(buf)); } catch { resolve(null); }
        });
      }
    );
    req.on("error", () => resolve(null));
    req.on("timeout", () => { req.destroy(); resolve(null); });
    if (method === "POST") req.end("{}");
    else req.end();
  });
}

/** 找一个能用的 Python。项目自带 runtime 优先，其次 PATH / py 启动器。 */
function findPython() {
  const local = path.join(ROOT, "runtime", "python.exe");
  if (fs.existsSync(local)) {
    log("使用项目内嵌 Python:", local);
    return local;
  }
  const cands = [process.env.PYTHON, "python", "python3", "py"].filter(Boolean);
  for (const exe of cands) {
    try {
      const r = spawnSync(exe, ["-c", "import sys;print(sys.version_info[0])"], {
        windowsHide: true, encoding: "utf8", timeout: 8000,
      });
      if (r.status === 0 && String(r.stdout).trim() === "3") {
        log("使用 Python:", exe);
        return exe;
      }
    } catch (_) { /* 继续试下一个 */ }
  }
  return null;
}

function startBackend(python) {
  fs.mkdirSync(STATE_DIR, { recursive: true });
  const out = fs.openSync(LOG_FILE, "a");
  const args = [path.join(ROOT, "server.py"), String(PORT)];
  log("启动后端:", python, args.join(" "));
  pyProc = spawn(python, args, {
    cwd: ROOT,
    windowsHide: true,                 // 不弹黑框
    stdio: ["ignore", out, out],
    env: {
      ...process.env,
      PYTHONIOENCODING: "utf-8",
      PYTHONUNBUFFERED: "1",
      // 让 Python 侧把 data/ state/ 写到与 Electron 相同的可写目录，
      // 否则便携版每次启动都从零开始（见顶部 STORE_DIR 注释）。
      COMPASS_STORE: STORE_DIR,
    },
  });
  pyProc.on("exit", (code, sig) => {
    log(`后端退出 code=${code} signal=${sig}`);
    // ★ 这里原来写的是 `code !== 0 && !code === null`：`!code` 是布尔值，永远不等于 null，
    // 整个条件恒为假，弹出提示的分支从来没执行过。正常退出是 code === 0，
    // 被信号杀掉时 code 为 null —— 两种情况都不该弹「异常退出」。
    if (!quitting && code !== 0 && code !== null) {
      // 后端意外挂掉：告诉用户，而不是留一个连不上的窗口
      if (win && !win.isDestroyed()) {
        dialog.showMessageBox(win, {
          type: "error",
          title: "后端服务已退出",
          message: `Python 服务异常退出（code=${code}）。`,
          detail: `日志：${LOG_FILE}\n可以关闭本窗口后重新启动应用。`,
          buttons: ["知道了"],
        });
      }
    }
  });
  pyProc.on("error", (e) => log("后端启动失败:", e.message));
  return pyProc;
}

/** 等后端真正能响应 /api/health（不是等端口开，端口开也可能还在 import）。 */
async function waitReady(timeoutMs = 180000) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    const h = await getJson("/api/health", 1500);
    if (h && h.ok) return h;
    if (pyProc && pyProc.exitCode !== null) return null;
    await sleep(400);
  }
  return null;
}

/** 优雅停服务：先发 /api/shutdown，给它 8 秒自己收尾，实在不退才杀。
 *
 *  ★ 必须做成幂等：关窗走 close 事件，随后 window-all-closed → will-quit 还会再兜一次。
 *  第二次进来时 Python 往往还没真正退出（exitCode 仍是 null），于是又发一遍 shutdown、
 *  又等 8 秒。实测日志里「正在停止后端 …」会连着打两行。用 promise 缓存挡住并发。 */
let stopPromise = null;
function stopBackend() {
  if (!owned || !pyProc) return Promise.resolve();
  if (stopPromise) return stopPromise;
  stopPromise = (async () => {
    log("正在停止后端 …");
    await getJson("/api/shutdown", 2000, "POST");
    const t0 = Date.now();
    while (Date.now() - t0 < 8000) {
      if (pyProc.exitCode !== null) { log("后端已优雅退出"); return; }
      await sleep(200);
    }
    log("后端 8 秒未退出，强制结束");
    try { pyProc.kill(); } catch (_) {}
  })();
  return stopPromise;
}

// --- 窗口 -------------------------------------------------------------------

function menuTemplate() {
  return [
    {
      label: "应用",
      submenu: [
        { label: "重建全部榜单", click: () => getJson("/api/rescan?target=all", 8000) },
        { type: "separator" },
        {
          label: "检查更新…",
          click: () => checkUpdate({ silent: false }),
        },
        { label: "打开数据目录", click: () => shell.openPath(STORE_DIR) },
        { type: "separator" },
        { label: "在浏览器中打开", click: () => shell.openExternal(PAGE) },
        { label: "打开程序目录", click: () => shell.openPath(ROOT) },
        { label: "查看运行日志", click: () => shell.openPath(LOG_FILE) },
        { type: "separator" },
        { label: "退出", accelerator: "Alt+F4", click: () => app.quit() },
      ],
    },
    {
      label: "视图",
      submenu: [
        { label: "刷新页面", accelerator: "F5", click: () => win && win.reload() },
        { label: "强制刷新", accelerator: "CmdOrCtrl+F5", click: () => win && win.webContents.reloadIgnoringCache() },
        { type: "separator" },
        { label: "全屏", role: "togglefullscreen" },
        { label: "缩放 +", role: "zoomIn" },
        { label: "缩放 −", role: "zoomOut" },
        { label: "重置缩放", role: "resetZoom" },
        { type: "separator" },
        { label: "开发者工具", accelerator: "F12", click: () => win && win.webContents.toggleDevTools() },
      ],
    },
    {
      label: "帮助",
      submenu: [
        {
          label: "关于",
          click: () =>
            dialog.showMessageBox(win, {
              type: "info",
              title: "关于",
              message: "A股右侧交易罗盘 · 桌面端",
              detail:
                `版本 ${app.getVersion()}\nElectron ${process.versions.electron} / Node ${process.versions.node}\n` +
                `后端 http://127.0.0.1:${PORT}（Python ${ready ? "已就绪" : "未就绪"}）\n\n` +
                "所有数据来自公开行情接口，由本机 Python 实时计算，不构成投资建议。",
              buttons: ["好"],
            }),
        },
      ],
    },
  ];
}

function createWindow() {
  win = new BrowserWindow({
    width: 1680,
    height: 1020,
    minWidth: 1180,
    minHeight: 720,
    title: APP_TITLE,
    backgroundColor: "#0a0a0a",
    icon: fs.existsSync(ICON) ? ICON : undefined,
    show: false,
    autoHideMenuBar: true,          // 按 Alt 才出菜单，界面更干净
    webPreferences: {
      contextIsolation: true,
      nodeIntegration: false,
      spellcheck: false,
      backgroundThrottling: false,  // 最小化时也保持轮询，别让行情停更
    },
  });

  Menu.setApplicationMenu(Menu.buildFromTemplate(menuTemplate()));
  win.loadURL(PAGE);

  win.once("ready-to-show", () => {
    win.show();
    win.focus();
  });

  // 页面里的外链（公告详情、研报详情）走系统浏览器，不在应用里开新窗
  win.webContents.setWindowOpenHandler(({ url }) => {
    if (!url.startsWith(`http://127.0.0.1:${PORT}`)) shell.openExternal(url);
    return { action: "deny" };
  });

  // 渲染进程崩溃自动恢复：行情终端长时间开着，偶发 OOM 别让用户面对白屏
  win.webContents.on("render-process-gone", async (_e, details) => {
    log("渲染进程终止:", details.reason);
    if (details.reason !== "clean-exit" && win && !win.isDestroyed()) {
      await sleep(600);
      win.reload();
    }
  });

  // 页面加载失败（后端还没起来/已挂）时给一张可读的说明页，而不是 Chromium 错误页
  win.webContents.on("did-fail-load", async (_e, code, desc, url, isMainFrame) => {
    if (!isMainFrame) return;
    log("页面加载失败:", code, desc, url);
    const alive = await getJson("/api/health", 1500);
    const body = alive
      ? `<p>后端已就绪，但页面加载失败（${desc}）。</p><p>按 F5 重试。</p>`
      : `<p>本地服务没有响应。</p><p>日志：<code>${LOG_FILE}</code></p><p>按 F5 重试，或关掉本窗口重新启动。</p>`;
    win.loadURL(
      "data:text/html;charset=utf-8," +
        encodeURIComponent(
          `<html><head><meta charset="utf-8"><style>
             body{background:#0a0a0a;color:#ddd;font:14px/1.9 "Microsoft YaHei",sans-serif;padding:48px}
             h2{color:#e8c15a;font-weight:600}code{background:#1a1a1a;padding:2px 6px;border-radius:4px}
           </style></head><body><h2>无法打开界面</h2>${body}</body></html>`
        )
    );
  });

  win.on("close", () => {
    if (quitting) return;
    quitting = true;
    // 先把服务停干净再退出，否则 Windows 上会留一个孤儿 python 进程占着 8092
    stopBackend().finally(() => app.quit());
  });
}

// --- 托盘 -------------------------------------------------------------------

function createTray() {
  if (!fs.existsSync(ICON)) return;
  const img = nativeImage.createFromPath(ICON);
  if (img.isEmpty()) return;
  tray = new Tray(img);
  tray.setToolTip(APP_TITLE);
  tray.setContextMenu(
    Menu.buildFromTemplate([
      { label: "显示主窗口", click: () => { if (win) { win.show(); win.focus(); } } },
      { label: "在浏览器中打开", click: () => shell.openExternal(PAGE) },
      { label: "重建全部榜单", click: () => getJson("/api/rescan?target=all", 8000) },
      { label: "检查更新…", click: () => checkUpdate({ silent: false }) },
      { type: "separator" },
      { label: "退出（同时停止服务）", click: () => app.quit() },
    ])
  );
  tray.on("double-click", () => { if (win) { win.show(); win.focus(); } });
}

// --- 自动更新 ---------------------------------------------------------------
//
// 结构照抄 WorkBuddy / Electron 生态的通用做法，分三层：
//   1. 版本检测：启动时后台静默比对版本号，菜单里也能手动检查；
//   2. 下载校验：下到数据目录，用清单里的 sha256 校验，不匹配就丢弃；
//   3. 独立更新器：真正替换 exe 的活儿交给另一个进程（Windows 不允许替换正在运行的 exe）。
//
// 更新源是 update.json 里的 feedUrl，指向一个放了 latest.json 的目录（http/https）。
// **不配 feedUrl 就等于关闭自动更新** —— 宁可没有，也不要每天弹一次「检查失败」。

const LOCAL_VERSION = app.getVersion();
const APP_TITLE = "A股右侧交易罗盘";
const UPDATE_DEFAULTS = {
  enabled: true,
  feedUrl: "",
  checkOnStart: true,
  checkDelaySec: 10,
};

function loadUpdateConfig() {
  const cands = [
    PORTABLE_DIR ? path.join(PORTABLE_DIR, "update.json") : null,
    path.join(ROOT, "update.json"),
    app.isPackaged ? path.join(process.resourcesPath, "update.json") : null,
  ].filter(Boolean);
  for (const p of cands) {
    try {
      if (fs.existsSync(p)) {
        const cfg = { ...UPDATE_DEFAULTS, ...JSON.parse(fs.readFileSync(p, "utf8")) };
        log("更新配置:", p, "enabled=" + cfg.enabled, "feed=" + (cfg.feedUrl || "(空)"));
        return cfg;
      }
    } catch (e) {
      log("update.json 读取失败:", p, e.message);
    }
  }
  return { ...UPDATE_DEFAULTS };
}

const updateCfg = loadUpdateConfig();

function vparts(v) {
  return String(v || "0").split(/[.\-+]/).map((x) => parseInt(x, 10) || 0);
}
/** 大于返回 1，相等 0，小于 -1。只比数字段，够用且不会误判。 */
function cmpVersion(a, b) {
  const x = vparts(a), y = vparts(b);
  for (let i = 0; i < Math.max(x.length, y.length); i++) {
    const p = x[i] || 0, q = y[i] || 0;
    if (p !== q) return p > q ? 1 : -1;
  }
  return 0;
}

/** GET 一个 JSON。更新检查不走系统代理 —— 本地/内网更新源被代理拦掉是常见坑。 */
function fetchJson(url, timeout = 8000) {
  return new Promise((resolve) => {
    let u;
    try { u = new URL(url); } catch { resolve({ error: "地址不合法" }); return; }
    const mod = u.protocol === "https:" ? https : http;
    const req = mod.get(url, { timeout, headers: { "Cache-Control": "no-cache" } }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        resolve(fetchJson(new URL(res.headers.location, url).toString(), timeout));
        return;
      }
      if (res.statusCode !== 200) { res.resume(); resolve({ error: "HTTP " + res.statusCode }); return; }
      let buf = "";
      res.setEncoding("utf8");
      res.on("data", (d) => (buf += d));
      res.on("end", () => {
        try { resolve(JSON.parse(buf)); } catch { resolve({ error: "清单不是合法 JSON" }); }
      });
    });
    req.on("error", (e) => resolve({ error: e.message }));
    req.on("timeout", () => { req.destroy(); resolve({ error: "超时" }); });
  });
}

function sha256File(file) {
  const h = crypto.createHash("sha256");
  const fd = fs.openSync(file, "r");
  try {
    const buf = Buffer.alloc(1 << 20);
    let n;
    while ((n = fs.readSync(fd, buf, 0, buf.length, null)) > 0) h.update(buf.subarray(0, n));
  } finally {
    fs.closeSync(fd);
  }
  return h.digest("hex");
}

/** 下载并回报进度。几十 MB 的包，标题栏显示百分比比一个转圈窗口诚实。 */
function download(url, dest, onProgress) {
  return new Promise((resolve) => {
    let u;
    try { u = new URL(url); } catch { resolve({ ok: false, error: "地址不合法" }); return; }
    const mod = u.protocol === "https:" ? https : http;
    const req = mod.get(url, { timeout: 60000 }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        resolve(download(new URL(res.headers.location, url).toString(), dest, onProgress));
        return;
      }
      if (res.statusCode !== 200) { res.resume(); resolve({ ok: false, error: "HTTP " + res.statusCode }); return; }
      const total = Number(res.headers["content-length"] || 0);
      let got = 0, lastPct = -1;
      const out = fs.createWriteStream(dest);
      res.on("data", (chunk) => {
        got += chunk.length;
        if (total && onProgress) {
          const pct = Math.floor((got / total) * 100);
          if (pct !== lastPct) { lastPct = pct; onProgress(pct); }
        }
      });
      res.on("error", (e) => resolve({ ok: false, error: e.message }));
      res.pipe(out);
      out.on("finish", () => out.close(() => resolve({ ok: true, size: got })));
      out.on("error", (e) => resolve({ ok: false, error: "写文件失败: " + e.message }));
    });
    req.on("error", (e) => resolve({ ok: false, error: e.message }));
    req.on("timeout", () => { req.destroy(); resolve({ ok: false, error: "超时" }); });
  });
}

/** 「我自己」在哪。便携包必须用 exe 真实目录 —— process.execPath 指向临时解压目录。 */
function selfExePath() {
  if (PORTABLE_DIR) {
    const f = process.env.PORTABLE_EXECUTABLE_FILE;
    if (f && fs.existsSync(f)) return f;
    try {
      const exes = fs.readdirSync(PORTABLE_DIR).filter((n) => n.toLowerCase().endsWith(".exe"));
      if (exes.length) {
        exes.sort((a, b) =>
          fs.statSync(path.join(PORTABLE_DIR, b)).size - fs.statSync(path.join(PORTABLE_DIR, a)).size);
        return path.join(PORTABLE_DIR, exes[0]);
      }
    } catch (_) {}
  }
  return process.execPath;
}

async function doUpdate(manifest) {
  const dir = path.join(STORE_DIR, "updates", String(manifest.version));
  fs.mkdirSync(dir, { recursive: true });
  let name = "update.exe";
  try { name = path.basename(new URL(manifest.url).pathname) || name; } catch (_) {}
  const file = path.join(dir, name);

  // 上次下到一半/已下好：校验通过就直接复用，不重复占带宽
  let reuse = false;
  if (fs.existsSync(file) && manifest.sha256) {
    try { reuse = sha256File(file).toLowerCase() === String(manifest.sha256).toLowerCase(); } catch (_) {}
  }

  if (!reuse) {
    log("开始下载更新:", manifest.url);
    const r = await download(manifest.url, file, (pct) => {
      if (win && !win.isDestroyed()) win.setTitle(`${APP_TITLE} — 正在下载更新 ${pct}%`);
    });
    if (win && !win.isDestroyed()) win.setTitle(APP_TITLE);
    if (!r.ok) {
      log("下载失败:", r.error);
      dialog.showErrorBox("更新下载失败", `地址：${manifest.url}\n原因：${r.error}`);
      return false;
    }
    log("下载完成:", r.size, "bytes");
  } else {
    log("复用已下载的更新包:", file);
  }

  if (manifest.sha256) {
    const actual = sha256File(file);
    if (actual.toLowerCase() !== String(manifest.sha256).toLowerCase()) {
      log("sha256 不匹配:", actual);
      dialog.showErrorBox(
        "更新包校验失败",
        "下载的文件与清单里的 sha256 不一致，已放弃更新。\n\n可能是下载中断，或源站文件被替换过。"
      );
      try { fs.unlinkSync(file); } catch (_) {}
      return false;
    }
    log("sha256 校验通过");
  }

  const py = findPython();
  if (!py) {
    dialog.showErrorBox("找不到 Python", "更新需要 Python 执行 update_helper.py 来完成文件替换。");
    return false;
  }

  // 只走到「下载完成 + 校验通过」就停手，不碰任何文件。
  // 用途：① 自测更新链路时不必真的替换程序；② 用户排查「为什么更新失败」时，
  // 能确认到底是网络、版本号还是文件本身的问题。
  if (process.env.COMPASS_UPDATE_DRY_RUN) {
    log("DRY-RUN：更新包已下载并校验通过，跳过替换 →", file);
    return true;
  }

  const helper = path.join(ROOT, "update_helper.py");
  const target = selfExePath();
  const args = [
    helper,
    "--pid", String(process.pid),
    "--src", file,
    "--dst", target,
    "--log", path.join(STORE_DIR, "update.log"),
  ];
  if (manifest.sha256) args.push("--sha256", manifest.sha256);
  // 便携包文件名带版本号，新版是另一个文件 —— 那是「换一个启动」，不是「覆盖」
  if (PORTABLE_DIR) args.push("--mode", "swap", "--restart-src");
  else args.push("--mode", "replace", "--restart");

  log("移交给更新器:", [py, ...args].join(" "));
  try {
    const child = spawn(py, args, {
      cwd: STORE_DIR,
      detached: true,          // 主进程退出后它必须还活着
      stdio: "ignore",
      windowsHide: true,
    });
    child.unref();
  } catch (e) {
    log("更新器启动失败:", e.message);
    dialog.showErrorBox("无法启动更新器", e.message);
    return false;
  }

  // 走正常退出流程（会停掉自己拉起的后端），更新器随后接手
  quitting = true;
  stopBackend().finally(() => app.quit());
  return true;
}

async function checkUpdate({ silent = true, force = false } = {}) {
  if (!updateCfg.enabled || !updateCfg.feedUrl) {
    if (!silent) {
      dialog.showMessageBox(win, {
        type: "info",
        title: "未配置更新源",
        message: "当前版本没有配置自动更新源。",
        detail:
          `版本 ${LOCAL_VERSION}\n\n` +
          "在程序目录放一个 update.json，写入：\n" +
          '{ "feedUrl": "http://你的更新服务器/" }\n\n' +
          "该目录下需要提供 latest.json 清单（格式见 README）。",
        buttons: ["知道了"],
      });
    }
    return;
  }
  const base = updateCfg.feedUrl.replace(/\/+$/, "") + "/";
  const url = `${base}latest.json?t=${Date.now()}`;   // 防 CDN / 代理把旧清单缓存住
  const m = await fetchJson(url);
  if (m.error) {
    log("检查更新失败:", m.error);
    if (!silent) {
      dialog.showMessageBox(win, {
        type: "warning", title: "检查更新失败", message: m.error, detail: url, buttons: ["好"],
      });
    }
    return;
  }
  log("版本比对: 本地", LOCAL_VERSION, "远端", m.version);
  if (!m.version || cmpVersion(m.version, LOCAL_VERSION) <= 0) {
    if (!silent) {
      dialog.showMessageBox(win, {
        type: "info", title: "已是最新版本",
        message: `当前版本 ${LOCAL_VERSION} 已是最新。`, buttons: ["好"],
      });
    }
    return;
  }
  if (!m.url) {
    log("清单缺少 url 字段");
    if (!silent) {
      dialog.showMessageBox(win, { type: "warning", title: "更新清单不完整", message: "latest.json 里没有 url 字段。", buttons: ["好"] });
    }
    return;
  }
  // 清单里可以写相对文件名（RightsideCompass-1.0.1-portable.exe），相对 feedUrl 解析。
  // 这样同一个 latest.json 能跟着服务器换域名，不用改内容。
  try { m.url = new URL(m.url, base).toString(); } catch (_) {
    log("清单 url 无法解析:", m.url);
    return;
  }

  if (force) {
    log("--self-update --force：跳过确认直接更新");
    await doUpdate(m);
    return;
  }

  const detail = [m.notes || "", m.force ? "\n此版本为强制更新，请尽快完成。" : ""]
    .filter(Boolean).join("\n");
  const { response } = await dialog.showMessageBox(win, {
    type: "info",
    title: "发现新版本",
    message: `发现新版本 ${m.version}（当前 ${LOCAL_VERSION}）`,
    detail: detail || "是否立即下载并安装？",
    buttons: ["立即更新", "稍后"],
    defaultId: 0,
    cancelId: 1,
  });
  if (response !== 0) { log("用户选择稍后更新"); return; }
  await doUpdate(m);
}

// --- 启动流程 ---------------------------------------------------------------

// 单实例：很多人会重复双击图标。第二个实例直接把已有窗口叫到前台，
// 而不是再起一个 Electron + 再连一次后端。
if (!app.requestSingleInstanceLock()) {
  // 拿不到锁 = 已经有一个实例在跑。这条路径**必须留日志**，否则表现就是
  // 「双击图标没反应」，用户完全不知道是第二个实例被自己劝退了。
  log("单实例锁已被占用，本实例退出（把已有窗口叫到前台）");
  app.quit();
} else {
  app.on("second-instance", () => {
    if (win) {
      if (win.isMinimized()) win.restore();
      win.show();
      win.focus();
    }
  });

  app.whenReady().then(async () => {
    log("=== 桌面端启动 ===");
    app.setAppUserModelId("com.aipm.rightsidecompass");

    // 0) 命令行触发更新：`RightsideCompass-*.exe --self-update --force`
    //    这是不用点界面的升级入口（配合脚本/远程运维），和 WorkBuddy 的
    //    `workbuddy-cli --self-update --force`、企业微信 `/upgrade now` 是一类东西。
    //    走这条路**不开窗口**，装完直接退出。
    const argv = process.argv.slice(1);
    if (argv.includes("--self-update")) {
      log("命令行更新：--self-update" + (argv.includes("--force") ? " --force" : ""));
      await checkUpdate({ silent: true, force: true });
      // 走到这里说明没更新可装、或者失败了（checkUpdate 内部会弹错误框）
      log("命令行更新结束，退出");
      app.quit();
      return;
    }

    // 1) 端口已有服务 → 复用（可能是命令行起的，也可能是上一次没关干净）
    if (await portOpen(PORT)) {
      log(`端口 ${PORT} 已被占用，复用现有服务`);
      owned = false;
    } else {
      const py = findPython();
      if (!py) {
        dialog.showErrorBox(
          "找不到 Python",
          "本应用的后端需要 Python 3.9+。\n\n请安装 Python 并勾选 “Add to PATH”，" +
            "或把便携版运行时放到程序目录的 runtime\\python.exe。"
        );
        app.quit();
        return;
      }
      startBackend(py);
      owned = true;
    }

    // 2) 等后端能响应再开窗，避免用户先看到一张白屏或错误页
    const h = await waitReady(owned ? 180000 : 8000);
    ready = !!(h && h.ok);
    log("后端就绪:", ready);

    if (!ready && owned) {
      dialog.showErrorBox(
        "后端启动失败",
        `Python 服务在 180 秒内没有响应。\n\n日志：${LOG_FILE}\n\n` +
          "常见原因：端口被占用 / Python 依赖缺失 / 防火墙拦截。"
      );
      await stopBackend();
      app.quit();
      return;
    }

    createWindow();
    createTray();

    // 后端刚起来时页面会立刻可用，但榜单还在扫（前端自己有进度条），不用额外等待
    if (h && h.scan && h.scan.running) log("全市场扫描进行中：", h.scan.phase);

    // 启动后延迟检查更新：让窗口先画出来、行情先刷一轮，别在最忙的时候抢带宽
    if (updateCfg.checkOnStart && updateCfg.enabled && updateCfg.feedUrl) {
      const delay = Math.max(3, Number(updateCfg.checkDelaySec) || 10) * 1000;
      setTimeout(() => {
        checkUpdate({ silent: true }).catch((e) => log("检查更新异常:", e.message));
      }, delay);
    }
  });

  // 所有窗口关闭 → 退出（Windows 习惯）。close 事件里已负责停服务。
  app.on("window-all-closed", () => {
    if (process.platform !== "darwin") app.quit();
  });

  app.on("before-quit", () => { quitting = true; });

  // 退出前再兜一次：万一 close 事件被绕过（任务栏右键关闭等）
  app.on("will-quit", async (e) => {
    if (owned && pyProc && pyProc.exitCode === null) {
      e.preventDefault();
      await stopBackend();
      app.exit(0);
    }
  });
}
