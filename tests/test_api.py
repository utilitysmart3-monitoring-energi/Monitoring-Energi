import unittest
from datetime import datetime
from unittest.mock import patch

from api import app as api


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.client = api.app.test_client()

    def test_health_returns_database_identity(self):
        with patch.object(api, "fetch_rows", return_value=[{"database": "energi_db", "role": "monitoring_app"}]):
            response = self.client.get("/api/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["database"], "energi_db")
        self.assertEqual(response.json["status"], "ok")

    def test_report_forwards_timezone_aware_bounds(self):
        result = [{"meter_id": 6, "delta_t1": 51.059, "delta_t2": 212.726}]
        with patch.object(api, "fetch_rows", return_value=result) as fetch_rows:
            response = self.client.get(
                "/api/energy/report",
                query_string={
                    "start_time": "2026-10-15T08:00:00+07:00",
                    "end_time": "2026-10-16T08:00:00+07:00",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json[0]["meter_id"], 6)
        start_time, end_time = fetch_rows.call_args.args[1]
        self.assertEqual(start_time.utcoffset().total_seconds(), 7 * 60 * 60)
        self.assertGreater(end_time, start_time)

    def test_latest_partials_returns_rows(self):
        result = [{"meter_id": 6, "partial_ea": 123.0}]
        with patch.object(api, "fetch_rows", return_value=result):
            response = self.client.get("/api/energy/latest-partials")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, result)

    def test_history_rejects_out_of_range_meter(self):
        response = self.client.get(
            "/api/energy/history",
            query_string={
                "meter_id": 19,
                "start_time": "2026-10-15T00:00:00",
                "end_time": "2026-10-15T23:59:59",
            },
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("meter_id", response.json["error"])

    def test_history_forwards_meter_and_naive_local_bounds(self):
        with patch.object(api, "fetch_rows", return_value=[]) as fetch_rows:
            response = self.client.get(
                "/api/energy/history",
                query_string={
                    "meter_id": 6,
                    "start_time": "2026-10-15T00:00:00",
                    "end_time": "2026-10-15T23:59:59",
                },
            )

        self.assertEqual(response.status_code, 200)
        params = fetch_rows.call_args.args[1]
        self.assertEqual(params[0], 6)
        self.assertIsInstance(params[1], datetime)
        self.assertIsNone(params[1].tzinfo)

    def test_alarm_thresholds_and_limit_are_forwarded(self):
        with patch.object(api, "fetch_rows", return_value=[]) as fetch_rows:
            response = self.client.get(
                "/api/energy/alarms",
                query_string={
                    "start_time": "2026-10-15T00:00:00+07:00",
                    "end_time": "2026-10-15T23:59:59+07:00",
                    "over_voltage": "418",
                    "under_voltage": "340",
                    "limit": "1000",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fetch_rows.call_args.args[1][2:], (418.0, 340.0, 1000))

    def test_window_readings_sorts_meter_ids(self):
        with patch.object(api, "fetch_rows", return_value=[]) as fetch_rows:
            response = self.client.get(
                "/api/energy/window-readings",
                query_string={
                    "meter_ids": "16,6",
                    "start_time": "2026-10-15T08:00:00+07:00",
                    "end_time": "2026-10-16T08:00:00+07:00",
                },
            )

        self.assertEqual(response.status_code, 200)
        params = fetch_rows.call_args.args[1]
        self.assertEqual(params[0], [6, 16])
        self.assertEqual(params[3], [6, 16])

    def test_dashboard_and_shared_helper_are_served(self):
        for path in ("/", "/pages/tarif.html", "/assets/api.js"):
            response = self.client.get(path)
            self.assertEqual(response.status_code, 200)
            response.close()


if __name__ == "__main__":
    unittest.main()
