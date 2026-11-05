@echo off
chcp 65001 >nul
cd /d "%~dp0"
where python >nul 2>nul
if errorlevel 1 (
  echo [ERROR] python not found in PATH. Install Python 3.9+ first.
  pause
  exit /b 1
)
echo Starting replica on http://127.0.0.1:8092 ...
python server.py 8092
pause
