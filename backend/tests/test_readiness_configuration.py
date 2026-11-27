import os
import unittest
from unittest.mock import patch


class TestReadinessConfiguration(unittest.TestCase):
    def test_pipeline_trigger_reports_unconfigured_airflow_as_unavailable(self):
        from fastapi import HTTPException

        from main import queue_pipeline

        with patch.dict(os.environ, {"AIRFLOW_USERNAME": "", "AIRFLOW_PASSWORD": ""}):
            with self.assertRaises(HTTPException) as raised:
                queue_pipeline(incremental=True, source="csv", engine="pandas")

        self.assertEqual(raised.exception.status_code, 503)
        self.assertIn("Airflow integration is not configured", raised.exception.detail)
