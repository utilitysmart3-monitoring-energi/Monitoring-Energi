import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory
from psycopg.rows import dict_row
from werkzeug.exceptions import HTTPException
from waitress import serve


PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

app = Flask(__name__)


def get_datetime(name: str, require_timezone: bool = False) -> datetime:
    value = request.args.get(name)
    if not value:
        raise ValueError(f"Missing required parameter: {name}")

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"Invalid datetime for {name}") from error

    if require_timezone and parsed.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")

    if not require_timezone and parsed.utcoffset() is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def database_connection() -> psycopg.Connection:
    password = os.getenv("DATABASE_PASSWORD")
    if not password:
        raise RuntimeError("DATABASE_PASSWORD is not configured")

    return psycopg.connect(
        host=os.getenv("DATABASE_HOST", "127.0.0.1"),
        port=int(os.getenv("DATABASE_PORT", "5432")),
        dbname=os.getenv("DATABASE_NAME", "energi_db"),
        user=os.getenv("DATABASE_USER", "monitoring_app"),
        password=password,
        sslmode=os.getenv("DATABASE_SSLMODE", "prefer"),
        row_factory=dict_row,
        connect_timeout=5,
    )


def fetch_rows(query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with database_connection() as connection:
        rows = connection.execute(query, params).fetchall()

    return [
        {
            key: value.isoformat() if isinstance(value, datetime) else value
            for key, value in row.items()
        }
        for row in rows
    ]


@app.errorhandler(ValueError)
def invalid_request(error: ValueError) -> tuple[Any, int]:
    return jsonify(error=str(error)), 400


@app.errorhandler(RuntimeError)
def service_unavailable(error: RuntimeError) -> tuple[Any, int]:
    app.logger.error("API configuration error: %s", error)
    return jsonify(error="Database service is not configured"), 503


@app.errorhandler(psycopg.Error)
def database_error(error: psycopg.Error) -> tuple[Any, int]:
    app.logger.error("Database request failed: %s", error)
    return jsonify(error="Database request failed"), 503


@app.errorhandler(HTTPException)
def http_error(error: HTTPException) -> tuple[Any, int]:
    return jsonify(error=error.description), error.code or 500


@app.get("/")
def dashboard():
    return send_from_directory(PROJECT_ROOT, "index.html")


@app.get("/realtime.html")
def root_realtime():
    return send_from_directory(PROJECT_ROOT, "realtime.html")


@app.get("/pages/<path:page_name>")
def dashboard_page(page_name: str):
    return send_from_directory(PROJECT_ROOT / "pages", page_name)


@app.get("/assets/<path:asset_name>")
def asset(asset_name: str):
    return send_from_directory(PROJECT_ROOT / "assets", asset_name)


@app.get("/api/health")
def health():
    rows = fetch_rows("SELECT current_database() AS database, current_user AS role")
    return jsonify(status="ok", **rows[0])


@app.get("/api/energy/report")
def energy_report():
    start_time = get_datetime("start_time", require_timezone=True)
    end_time = get_datetime("end_time", require_timezone=True)
    if start_time > end_time:
        raise ValueError("start_time must not be after end_time")

    rows = fetch_rows(
        """
        SELECT meter_id, delta_partial_ea, delta_t1, delta_t2,
               biaya_lwbp, biaya_wbp, total_biaya
        FROM public.get_energy_report(%s, %s)
        ORDER BY meter_id
        """,
        (start_time, end_time),
    )
    return jsonify(rows)


@app.get("/api/energy/latest-partials")
def latest_partials():
    rows = fetch_rows(
        """
        SELECT DISTINCT ON (meter_id) meter_id, partial_ea
        FROM public.energi_db
        WHERE partial_ea IS NOT NULL
        ORDER BY meter_id, waktu_jakarta DESC
        """
    )
    return jsonify(rows)


@app.get("/api/energy/history")
def energy_history():
    try:
        meter_id = int(request.args.get("meter_id", ""))
    except ValueError as error:
        raise ValueError("meter_id must be an integer") from error
    if not 1 <= meter_id <= 18:
        raise ValueError("meter_id must be between 1 and 18")

    start_time = get_datetime("start_time")
    end_time = get_datetime("end_time")
    if start_time > end_time:
        raise ValueError("start_time must not be after end_time")

    rows = fetch_rows(
        """
        SELECT waktu_jakarta, voltage, current_i1, current_i2, current_i3
        FROM public.energi_db
        WHERE meter_id = %s AND waktu_jakarta >= %s AND waktu_jakarta <= %s
        ORDER BY waktu_jakarta ASC
        """,
        (meter_id, start_time, end_time),
    )
    return jsonify(rows)


@app.get("/api/energy/alarms")
def voltage_alarms():
    start_time = get_datetime("start_time")
    end_time = get_datetime("end_time")
    if start_time > end_time:
        raise ValueError("start_time must not be after end_time")

    try:
        over_voltage = float(request.args.get("over_voltage", "418"))
        under_voltage = float(request.args.get("under_voltage", "340"))
        limit = int(request.args.get("limit", "1000"))
    except ValueError as error:
        raise ValueError("Voltage thresholds and limit must be numeric") from error
    if not math.isfinite(over_voltage) or not math.isfinite(under_voltage):
        raise ValueError("Voltage thresholds must be finite numbers")
    limit = min(max(limit, 1), 1000)

    rows = fetch_rows(
        """
        SELECT waktu_jakarta, meter_id, voltage
        FROM public.energi_db
        WHERE waktu_jakarta >= %s AND waktu_jakarta <= %s
          AND (voltage > %s OR voltage < %s)
        ORDER BY waktu_jakarta DESC
        LIMIT %s
        """,
        (start_time, end_time, over_voltage, under_voltage, limit),
    )
    return jsonify(rows)


@app.get("/api/energy/window-readings")
def window_readings():
    raw_meter_ids = request.args.get("meter_ids", "")
    try:
        meter_ids = sorted({int(value) for value in raw_meter_ids.split(",") if value})
    except ValueError as error:
        raise ValueError("meter_ids must be comma-separated integers") from error
    if not meter_ids or any(not 1 <= meter_id <= 18 for meter_id in meter_ids):
        raise ValueError("meter_ids must contain IDs between 1 and 18")

    start_time = get_datetime("start_time")
    end_time = get_datetime("end_time")
    if start_time > end_time:
        raise ValueError("start_time must not be after end_time")

    rows = fetch_rows(
        """
        WITH first_readings AS (
            SELECT DISTINCT ON (meter_id) meter_id, tarif_t1, tarif_t2
            FROM public.energi_db
            WHERE meter_id = ANY(%s) AND waktu_jakarta >= %s AND waktu_jakarta <= %s
            ORDER BY meter_id, waktu_jakarta ASC
        ),
        last_readings AS (
            SELECT DISTINCT ON (meter_id) meter_id, tarif_t1, tarif_t2
            FROM public.energi_db
            WHERE meter_id = ANY(%s) AND waktu_jakarta >= %s AND waktu_jakarta <= %s
            ORDER BY meter_id, waktu_jakarta DESC
        )
        SELECT f.meter_id,
               f.tarif_t1 AS first_t1, f.tarif_t2 AS first_t2,
               l.tarif_t1 AS last_t1, l.tarif_t2 AS last_t2
        FROM first_readings f
        JOIN last_readings l USING (meter_id)
        ORDER BY f.meter_id
        """,
        (meter_ids, start_time, end_time, meter_ids, start_time, end_time),
    )
    return jsonify(rows)


if __name__ == "__main__":
    serve(
        app,
        host=os.getenv("WEB_HOST", "127.0.0.1"),
        port=int(os.getenv("WEB_PORT", "8080")),
    )