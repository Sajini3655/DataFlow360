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
        self.assertEqual(
            json.loads(request.data)["conf"],
            {"source": "csv", "incremental": True, "engine": "pandas"},
        )

    @patch.dict(os.environ, {"AIRFLOW_USERNAME": "test-user", "AIRFLOW_PASSWORD": "test-password"})
    @patch("airflow_client.urlopen")
    def test_trigger_preserves_selected_processing_engine(self, urlopen):
        urlopen.return_value = FakeResponse({"dag_run_id": "manual__spark", "state": "queued"})
        result = trigger_dag("csv", True, "pyspark")
        self.assertEqual(result["dag_run_id"], "manual__spark")
        request = urlopen.call_args.args[0]
        self.assertEqual(json.loads(request.data)["conf"]["engine"], "pyspark")

    @patch.dict(os.environ, {"AIRFLOW_USERNAME": "test-user", "AIRFLOW_PASSWORD": "test-password"})
    @patch("airflow_client.urlopen")
    def test_status_reads_dag_and_task_states(self, urlopen):
        urlopen.side_effect = [
            FakeResponse({"state": "running", "start_date": "2026-01-01"}),
            FakeResponse({
                "task_instances": [
                    {
                        "task_id": "etl",
                        "state": "running",
                        "try_number": 2,
                        "max_tries": 3,
                    }
                ]
            }),
            FakeResponse({"timetable_description": "Every hour"}),
        ]
        result = get_dag_run("manual__test")
        self.assertEqual(result["state"], "running")
        self.assertEqual(result["schedule"], "Every hour")
        self.assertEqual(result["tasks"][0]["task_id"], "etl")
        self.assertEqual(result["tasks"][0]["try_number"], 2)
        self.assertEqual(urlopen.call_count, 3)

    @patch.dict(os.environ, {"AIRFLOW_USERNAME": "test-user", "AIRFLOW_PASSWORD": "test-password"})
    @patch("airflow_client.urlopen")
    def test_retry_clears_only_the_requested_failed_task(self, urlopen):
        from airflow_client import retry_failed_task

        urlopen.return_value = FakeResponse({"task_instances": []})
        retry_failed_task("manual__test", "dbt_build")
        request = urlopen.call_args.args[0]
        self.assertTrue(request.full_url.endswith("/dags/dataflow360_daily_pipeline/clearTaskInstances"))
        self.assertEqual(json.loads(request.data), {
            "dry_run": False,
            "task_ids": ["dbt_build"],
            "dag_run_id": "manual__test",
            "only_failed": True,
        })
