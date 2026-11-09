@echo off
cd /d "C:\Mahasiswa ATMI-Monitoring Energi\Monitoring-Energi"

echo ==========================================================
echo MEMULAI SISTEM SCADA (AUTO-START)
echo Menulis log ke log_scada.txt...
echo ==========================================================

set PYTHONIOENCODING=utf-8

python -u history.py > log_scada.txt 2>&1