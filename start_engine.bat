@echo off
cd /d "%~dp0"

:ulangi
echo [SISTEM] Menjalankan Master Engine SCADA...
python engine\history.py

echo [SISTEM] Program Crash atau Berhenti! Merestart dalam 5 detik...
timeout /t 5 >nul
goto ulangi