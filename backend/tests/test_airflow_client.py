import json
import os
import unittest
from unittest.mock import patch

from airflow_client import get_dag_run, trigger_dag


class FakeResponse:
    def __init__(self, value):
        self.value = value

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def read(self):
        return json.dumps(self.value).encode()


class TestAirflowClient(unittest.TestCase):
    @patch.dict(os.environ, {"AIRFLOW_USERNAME": "test-user", "AIRFLOW_PASSWORD": "test-password"})
    @patch("airflow_client.urlopen")
    def test_trigger_posts_configuration_and_returns_real_run_id(self, urlopen):
        urlopen.return_value = FakeResponse({"dag_run_id": "manual__test", "state": "queued"})
        result = trigger_dag("csv", True)
        self.assertEqual(result["dag_run_id"], "manual__test")
        request = urlopen.call_args.args[0]
        self.assertTrue(request.full_url.endswith("/dagRuns"))
        self.assertIn("Basic ", request.get_header("Authorization"))
        self.assertEqual(json.loads(request.data)["conf"], {"source": "csv", "incremental": True})

    @patch.dict(os.environ, {"AIRFLOW_USERNAME": "test-user", "AIRFLOW_PASSWORD": "test-password"})
    @patch("airflow_client.urlopen")
    def test_status_reads_dag_and_task_states(self, urlopen):
        urlopen.side_effect = [
            FakeResponse({"state": "running", "start_date": "2026-01-01"}),
            FakeResponse({"task_instances": [{"task_id": "etl", "state": "running"}]}),
        ]
        result = get_dag_run("manual__test")
        self.assertEqual(result["state"], "running")
        self.assertEqual(result["tasks"], [{"task_id": "etl", "state": "running"}])
        self.assertEqual(urlopen.call_count, 2)
