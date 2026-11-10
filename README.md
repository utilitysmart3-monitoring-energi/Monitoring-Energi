# Monitoring Energi

## What this project does

This is a small monitoring system for up to 18 power meters. It has a Python process that reads the meters and several static HTML pages for live monitoring, history, reports, and downloads.

## Main components

- `engine/history.py` reads Modbus TCP via the gateway at `192.168.7.8:26`, publishes live readings to HiveMQ, and periodically inserts selected readings into PostgreSQL table `energi_db`.
- `pages/realtime.html` subscribes to MQTT topics under `monitoring/data/powermeter/#` for live values. It does not read live values from the database.
- `pages/` history and tariff views read from PostgreSQL through the same-origin API in `api/app.py`.
- `index.html` is the dashboard/navigation page.
- `pages/` contains the secondary dashboard views; navigation links among them stay within that folder.
- `tools/test_modbus.py` is a standalone diagnostic that attempts a real connection/read from the configured Modbus gateway; it is not an automated unit test.
- `archive/history.py.laptop-backup` is a second, older copy of the engine. It is not imported by the application; compare it before changing or deleting either copy.
- `assets/` contains the local images used by the dashboard and live-monitoring page.
- `logs/log_scada.txt` contains the saved output from the SCADA launcher.

## Running

From the project directory on the server, create a virtual environment and install the dependencies:

```powershell
py -3.9 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Edit `.env` and set `DATABASE_PASSWORD` to the password for `monitoring_app`. Keep `.env` private; it is ignored by Git.

Run `start_api.bat` to serve the dashboard and API at `http://127.0.0.1:8080/`. In a second session, run `start_engine.bat` to start the Modbus/MQTT/PostgreSQL worker; it restarts the engine after exit. `Mulai_SCADA.bat` runs the worker and writes output to `logs/log_scada.txt` without restarting it. All three launchers prefer `.venv` when present.

The Modbus gateway and MQTT broker settings are in `engine/history.py`. The API binds to localhost by default. Do not expose it to other machines until access is restricted and protected.

## PostgreSQL status

The migrated database is PostgreSQL 16, database `energi_db`. The table contains 185,390 imported rows; the generated `waktu_jakarta` values and report output were checked against Supabase.

`migrations/001_energi_db.sql` defines the table and `get_energy_report` function. The report preserves the existing UTC boundary behavior; changing to Jakarta-day boundaries later should be treated as a separate behavior change.

The live Supabase project has not been deleted; keep it available until the API and writer are deployed and verified on the server.