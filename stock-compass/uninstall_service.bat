@echo off
chcp 65001 >nul
cd /d "%~dp0"

REM 停止并删除「A股右侧交易罗盘 · 数据服务」。
REM 数据不会被删：服务模式的数据在 %ProgramData%\RightsideCompass。
net session >nul 2>&1
if errorlevel 1 (
  echo 需要管理员权限，正在重新以管理员身份启动 ...
  powershell -NoProfile -Command "Start-Process -FilePath '%~f0' -Verb RunAs"
  exit /b
)

set ELECTRON_RUN_AS_NODE=
python "%~dp0service.py" --uninstall
echo.
pause
