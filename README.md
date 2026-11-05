# A股右侧交易罗盘（复刻版）

单文件前端 + 纯标准库 Python 后端的 A 股行情终端。不装任何第三方库（无 pip 依赖），
拷到 U 盘双击 `start.bat` 就能跑；也可用 Electron 封装成桌面应用，支持自动更新与
注册为 Windows 后台服务。

> 数据来自腾讯 / 新浪 / 东财的公开行情接口，仅用于个人研究，不构成投资建议。

## 目录

```
stock-compass/
├── server.py        HTTP 服务与全部 /api 路由（仅标准库 http.server）
├── engine.py        强势股扫描 · 十二维体检 · K 线缓存（COMPASS_STORE 指定数据目录）
├── edge.py          坐标系自检：历史时点信号重建、超额收益、环境分层、权重反事实搜索
├── review.py        复盘      tactics.py  战术      gold.py  黄金专区
├── liveradar.py     实盘信号  ind15.py    十五行业  knowledge.py  知识库
├── index.html       单文件前端（原生 JS + 本地 vendor/echarts，无构建步骤）
├── vendor/          ECharts 5.5.0（本地优先，断网也能画图）
├── assets/          PWA 图标与背景图
├── desktop/         Electron 封装
│   ├── main.js        主进程：拉起内嵌 Python、版本检测、下载校验、菜单与托盘
│   ├── package.json   electron-builder 配置（portable 目标）
│   └── _mirror.py     构建期本地镜像转发（绕开代理下载失败）
├── service.py       ctypes 直调 SCM 的服务宿主（不依赖 pywin32 / nssm / srvany）
├── update_helper.py 独立更新器（等主进程退出 → 校验 sha256 → 替换 → 重启）
├── release.py       生成 latest.json 并发布
├── install_service.bat / uninstall_service.bat   注册 / 卸载后台服务
└── update.json      自动更新配置（feedUrl 留空 = 不检查）
```

## 快速开始

```bat
cd stock-compass
start.bat
```

浏览器打开 <http://127.0.0.1:8092>。首次扫描全市场约 2 分钟，之后按交易日缓存。

换端口：`set PORT=8099 && python server.py`

数据目录默认在程序旁；要放到别处（如服务模式下的 `C:\ProgramData`）：

```bat
set COMPASS_STORE=D:\罗盘数据 && python server.py
```

## 桌面端

```bat
cd stock-compass\desktop
npm install
npm start            # 开发态
npm run dist         # 打 portable 包 → dist\
```

便携包每次运行会解压到 `%TEMP%` 新目录，所以数据目录锚定在 **exe 所在目录** 的
`罗盘数据\`（读 `PORTABLE_EXECUTABLE_DIR`），不会随退出丢失。

自动更新的三层结构：

| 层 | 在哪 | 做什么 |
|---|---|---|
| 版本检测 | 主进程 `checkUpdate()` | 拉 `latest.json`，只比数字段 |
| 下载校验 | 主进程 `download()` | 按 `content-length` 报进度，落盘后校验 sha256 |
| 替换文件 | 独立进程 `update_helper.py` | Windows 不允许替换运行中的 exe，必须等主进程退出 |

三种触发方式：启动后延迟自动检测、`workbuddy-cli --self-update --force`、
企业微信 `/upgrade now`；也可手动下载安装包覆盖。
`COMPASS_UPDATE_DRY_RUN=1` 可只走检测与下载、不真正替换。

## 后台服务

```bat
install_service.bat      :: 需管理员，注册为 RightsideCompass.CoreService
python service.py --status
uninstall_service.bat
```

服务以 LocalSystem 运行，数据落 `C:\ProgramData\RightsideCompass`。
注册后关掉窗口同步与扫描仍继续；不注册则关窗即停。
`python service.py --console` 用同一套代码路径前台跑，便于排查。

## 坐标系自检（edge.py）

对历史时点做 point-in-time 信号重建（只喂 `rows[:i+1]`，无未来函数），再看后续涨跌：

- **超额收益** = 个股收益 − 当日全样本均值（剔除大盘影响）
- **环境分层** 按当日全样本均值正负切涨日 / 跌日，看超额符号是否随大盘翻转，
  用来区分「策略失效」与「样本期偏熊」
- **维度区分度** 按中位数切高低组，`spread = 高组 − 低组`；≤ 0 即共线冗余
- **权重反事实搜索** 穷举 1~2 维组合，训练段选优、留出段验证，留出转负 = 过拟合

页面上的每一句结论都由 `summary` 现算，不含写死的判断 —— 换一批验证数据，
文字会跟着变。

## 什么不入库

`data/kline/`（5223 个文件 / 83 MB）、`data/*.json` 行情缓存、`state/` 运行时状态、
`desktop/dist*`、`desktop/node_modules/` 全部由 `.gitignore` 排除，合计约 1.9 GB。
删掉后首次启动会重新拉取。
