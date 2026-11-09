@echo off
cd /d "C:\Mahasiswa ATMI-Monitoring Energi\Monitoring-Energi"

:ulangi
echo [SISTEM] Menjalankan Master Engine SCADA...
python history.py

echo [SISTEM] Program Crash atau Berhenti! Merestart dalam 5 detik...
timeout /t 5 >nul
goto ulangi