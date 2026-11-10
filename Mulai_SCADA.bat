@echo off
cd /d "%~dp0"

if exist ".venv\Scripts\python.exe" (
	set "PYTHON=.venv\Scripts\python.exe"
) else (
	set "PYTHON=python"
)

echo ==========================================================
echo MEMULAI SISTEM SCADA (AUTO-START)
echo Menulis log ke logs\log_scada.txt...
echo ==========================================================

set PYTHONIOENCODING=utf-8

if not exist "logs" mkdir "logs"
%PYTHON% -u engine\history.py > logs\log_scada.txt 2>&1