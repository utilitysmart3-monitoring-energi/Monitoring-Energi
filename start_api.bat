@echo off
cd /d "%~dp0"

if exist ".venv\Scripts\python.exe" (
	set "PYTHON=.venv\Scripts\python.exe"
) else (
	set "PYTHON=python"
)

set PYTHONIOENCODING=utf-8
%PYTHON% -u api\app.py