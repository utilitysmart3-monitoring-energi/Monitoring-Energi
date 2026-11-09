# Monitoring Energi

## What this project does

This is a small monitoring system for up to 18 power meters. It has a Python process that reads the meters and several static HTML pages for live monitoring, history, reports, and downloads.

## Main components

- `engine/history.py` reads Modbus TCP via the gateway at `192.168.7.8:26`, publishes live readings to the public HiveMQ broker, and periodically inserts selected readings into Supabase table `energi_db`.
- `pages/realtime.html` subscribes to MQTT topics under `monitoring/data/powermeter/#` for live values. It does not read live values from the database.
- `pages/tarif.html`, `pages/grafik_tarif.html`, and `pages/download_tarif.html` use Supabase table reads and the `get_energy_report` RPC to calculate/report energy use.
- `pages/grafik_phasa.html`, `pages/download_kwh_harian.html`, and `pages/riwayat_alarm.html` query historical rows from `energi_db`.
- `index.html` is the dashboard/navigation page.
- `pages/` contains the secondary dashboard views; navigation links among them stay within that folder.
- `tools/test_modbus.py` is a standalone diagnostic that attempts a real connection/read from the configured Modbus gateway; it is not an automated unit test.
- `archive/history.py.laptop-backup` is a second, older copy of the engine. It is not imported by the application; compare it before changing or deleting either copy.
- `assets/` contains the local images used by the dashboard and live-monitoring page.
- `logs/log_scada.txt` contains the saved output from the SCADA launcher.

## Running

Install the packages listed in `requirements.txt`, then run `python engine/history.py` from this folder. `start_engine.bat` loops and restarts the engine after it exits. `Mulai_SCADA.bat` writes the engine output to `logs/log_scada.txt` but does not restart it. Both launchers now resolve the project folder relative to their own location.

The Python process has the Modbus gateway and MQTT broker settings in `engine/history.py`.

## Database migration notes

Supabase is used by both the Python writer and six HTML pages. The pages connect directly to Supabase from the browser; a PostgreSQL connection string or password must not be put in HTML/JavaScript. The pages need a server-side API between the browser and PostgreSQL.

The repository does not include the `energi_db` table definition or the SQL definition of `get_energy_report`. Obtain those from the current Supabase project before replacing it. The writer sends `meter_id`, `partial_ea`, `tarif_t1`, `tarif_t2`, `voltage`, and `current_i1` through `current_i3`; pages additionally expect columns including `waktu_jakarta`. The RPC returns per-meter `delta_t1` and `delta_t2` for a time range, so its exact boundary and reset-handling behavior must be preserved.

Suggested migration order:

1. Export and document the current table schema, indexes, timestamp/timezone behavior, row-level security policies, and `get_energy_report` implementation.
2. Back up and migrate the existing rows; verify counts, date ranges, and representative report results.
3. Add a server-side API and PostgreSQL writer configured through environment variables. Keep database credentials out of the frontend and source control.
4. Move each HTML page from Supabase queries/RPC calls to the API, then test live collection, historical pages, date boundaries, and reports against PostgreSQL.
5. Only after both writer and readers have moved, remove the Supabase Python package, JavaScript CDN references, and Supabase project settings.

The live MQTT connection is separate from Supabase/PostgreSQL and can remain as-is during the database migration.

## Dependencies

`requirements.txt` currently lists Paho MQTT, PyModbus, and Supabase. The HTML pages also load their UI/chart/export libraries from external CDNs.