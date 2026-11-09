@echo off
cd /d "%~dp0"

echo ==========================================================
echo MEMULAI SISTEM SCADA (AUTO-START)
echo Menulis log ke logs\log_scada.txt...
echo ==========================================================

set PYTHONIOENCODING=utf-8

if not exist "logs" mkdir "logs"
python -u engine\history.py > logs\log_scada.txt 2>&1