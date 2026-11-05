@echo off
chcp 65001 >nul
cd /d "%~dp0"

REM 安装「A股右侧交易罗盘 · 数据服务」并设为开机自动启动。
REM 必须以管理员身份运行：注册服务、写系统服务表都需要提权。
net session >nul 2>&1
if errorlevel 1 (
  echo 需要管理员权限，正在重新以管理员身份启动 ...
  powershell -NoProfile -Command "Start-Process -FilePath '%~f0' -Verb RunAs"
  exit /b
)

set ELECTRON_RUN_AS_NODE=
python "%~dp0service.py" --install
echo.
pause
