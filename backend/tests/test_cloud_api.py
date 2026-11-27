import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

try:
    import psycopg
except ModuleNotFoundError as exc:
    raise unittest.SkipTest("Install backend/requirements.txt to run cloud API handler tests") from exc


_original_database_url = os.environ.get("DATABASE_URL")
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
_fake_connection = MagicMock()
_fake_connection.__enter__.return_value = _fake_connection
_fake_connection.cursor.return_value.__enter__.return_value = MagicMock()
with patch.object(psycopg, "connect", return_value=_fake_connection):
    import main
if _original_database_url is None:
    os.environ.pop("DATABASE_URL", None)
else:
    os.environ["DATABASE_URL"] = _original_database_url


class FakeCursor:
    def __init__(self, rows=None):
        self.rows = rows or []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, *_args, **_kwargs):
        return None

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, rows=None):
        self.cursor_instance = FakeCursor(rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def cursor(self):
        return self.cursor_instance


class TestCloudApiHandlers(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        root = Path(self.temporary_directory.name)
        self.environment = patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://test:test@localhost/test",
                "CLOUD_STORAGE_MODE": "local",
                "CLOUD_LOCAL_ROOT": str(root / "cloud"),
                "PARQUET_DIR": str(root / "parquet"),
                "FABRIC_WORKSPACE_NAME": "",
                "FABRIC_LAKEHOUSE_NAME": "",
            },
            clear=False,
        )
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def test_cloud_status_reports_unverified_fabric_and_local_mode(self):
        with patch.object(main, "connect", return_value=FakeConnection([])):
            result = main.cloud_status()

        self.assertEqual(result["storage"]["mode"], "local")
        self.assertTrue(result["storage"]["configured"])
        self.assertEqual(result["storage"]["connection"]["state"], "not_tested")
        self.assertEqual(result["fabric"]["state"], "not_configured")
        self.assertEqual(result["fabric"]["execution"], "not_run")

    def test_local_connection_test_persists_operation_and_returns_real_adapter_result(self):
        with patch.object(main, "_record_cloud_event") as record_event:
            result = main.cloud_storage_test()

        self.assertTrue(result["success"])
        self.assertEqual(result["mode"], "local")
        record_event.assert_called_once()
        self.assertEqual(record_event.call_args.args[:3], ("connection_test", "local", "succeeded"))

    def test_sync_uploads_three_existing_parquet_snapshots_and_records_aggregate(self):
        source_root = Path(os.environ["PARQUET_DIR"])
        source_root.mkdir(parents=True, exist_ok=True)
        for layer in ("bronze", "silver", "gold"):
            (source_root / f"{layer}_sales.parquet").write_bytes(f"{layer}-bytes".encode())

        with patch.object(main, "_record_cloud_event") as record_event:
            result = main.cloud_sync()

        self.assertTrue(result["success"])
        self.assertIn("no cloud service was contacted", result["message"])
        self.assertEqual(len(result["files"]), 3)
        self.assertEqual([call.args[0] for call in record_event.call_args_list].count("dataset_sync"), 1)
        self.assertEqual([call.args[0] for call in record_event.call_args_list].count("dataset_sync_file"), 3)

    def test_storage_listing_uses_selected_local_layer(self):
        root = Path(os.environ["CLOUD_LOCAL_ROOT"])
        stored = root / "bronze" / "sample.json"
        stored.parent.mkdir(parents=True)
        stored.write_text('[{"order_id":"S1"}]', encoding="utf-8")

        result = main.cloud_storage_files("bronze")

        self.assertEqual(result["mode"], "local")
        self.assertEqual(result["files"][0]["name"], "sample.json")

    def test_sync_failure_returns_service_error_and_records_failed_operation(self):
        with patch.object(main, "_record_cloud_event") as record_event:
            with self.assertRaises(main.HTTPException) as raised:
                main.cloud_sync()

        self.assertEqual(raised.exception.status_code, 502)
        self.assertEqual(record_event.call_args.args[:3], ("dataset_sync", "local", "failed"))


if __name__ == "__main__":
    unittest.main()
