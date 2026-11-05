@echo off
chcp 65001 >nul
cd /d "%~dp0"

REM ★ 必须清掉 ELECTRON_RUN_AS_NODE。
REM   某些 Electron 应用（含本机的开发工具）会把这个变量注入到子进程环境里，
REM   带着它启动 electron.exe 会退化成「纯 Node 模式」：不开窗口，
REM   require('electron') 只返回一个 exe 路径字符串，然后报
REM   TypeError: Cannot read properties of undefined (reading 'isPackaged')。
REM   这不是代码 bug，是本机环境变量污染 —— 所以启动脚本里显式清空。
set ELECTRON_RUN_AS_NODE=

REM NODE_OPTIONS 一并清掉：打包后的 Electron 只支持其中一小部分，带着它会刷
REM 「Most NODE_OPTIONs are not supported in packaged apps」并可能影响启动参数。
set NODE_OPTIONS=

if not exist "node_modules\electron\dist\electron.exe" (
  echo [首次运行] 正在安装依赖，约 1 分钟 ...
  call npm install --no-audit --no-fund
  if errorlevel 1 (
    echo [错误] 依赖安装失败，请检查 Node.js 与网络。
    pause
    exit /b 1
  )
)

REM 直接调 electron.exe，绕过 npm 一层，启动更快、也不受 npm 脚本解析差异影响
start "" "%~dp0node_modules\electron\dist\electron.exe" "%~dp0."
