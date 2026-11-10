@echo off
cd /d "%~dp0"

if exist ".venv\Scripts\python.exe" (
	set "PYTHON=.venv\Scripts\python.exe"
) else (
	set "PYTHON=python"
)

:ulangi
echo [SISTEM] Menjalankan Master Engine SCADA...
%PYTHON% engine\history.py

echo [SISTEM] Program Crash atau Berhenti! Merestart dalam 5 detik...
timeout /t 5 >nul
goto ulangi