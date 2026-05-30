@echo off
chcp 65001 >nul 2>&1
title Smart Investment Advisor
cd /d "%~dp0"
echo.
echo ============================================
echo   Smart Investment Advisor v2
echo ============================================
echo.
echo [Checking proxy...]
python -X utf8 check_proxy.py 2>nul
echo.
echo [Starting server...]
echo   Open: http://127.0.0.1:5000
echo   Press Ctrl+C to stop
echo ============================================
echo.
python -X utf8 app.py
pause
