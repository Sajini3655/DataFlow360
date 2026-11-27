import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from cloud_storage import (
    AzureDataLakeStorageAdapter,
    CloudStorageConfigurationError,
    CloudStorageConfig,
    CloudStorageError,
    LocalStorageAdapter,
    create_storage_adapter,
)


class FakeAzureError(Exception):
    def __init__(self, status_code):
        super().__init__("provider response must not be returned")
        self.status_code = status_code


class FakeDownloader:
    def __init__(self, content):
        self.content = content

    def readinto(self, stream):
        stream.write(self.content)


class FakeFileClient:
    def __init__(self):
        self.content = None
        self.metadata = {}

    def get_file_properties(self):
        if self.content is None:
            raise FakeAzureError(404)
        return SimpleNamespace(
            metadata=self.metadata,
            size=len(self.content),
            etag="fake-etag",
        )

    def upload_data(self, stream, overwrite):
        if not overwrite and self.content is not None:
            raise AssertionError("Expected idempotent overwrite behavior")
        self.content = stream.read()

    def set_metadata(self, metadata):
        self.metadata = dict(metadata)

    def download_file(self):
        return FakeDownloader(self.content)


class FakeFileSystem:
    def __init__(self):
        self.files = {}

    def get_file_system_properties(self):
        return {"name": "ready"}

    def get_file_client(self, name):
        return self.files.setdefault(name, FakeFileClient())

    def get_paths(self, recursive):
        return [
            SimpleNamespace(
                name=name,
                content_length=len(client.content),
                last_modified=None,
                etag="fake-etag",
                is_directory=False,
            )
            for name, client in self.files.items()
            if client.content is not None
        ]

    def get_directory_client(self, name):
        return SimpleNamespace(create_directory=lambda: None)


class FakeServiceClient:
    def __init__(self):
        self.filesystems = {}

    def get_file_system_client(self, file_system):
        return self.filesystems.setdefault(file_system, FakeFileSystem())


class TestCloudStorage(unittest.TestCase):
    def config(self, root, **overrides):
        values = {
            "CLOUD_STORAGE_MODE": "local",
            "CLOUD_LOCAL_ROOT": str(root),
        }
        values.update(overrides)
        return CloudStorageConfig.from_env(values)

    def test_local_mode_is_default_and_factory_remains_local(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(directory)
            adapter = create_storage_adapter(config)
            self.assertEqual(config.mode, "local")
            self.assertIsInstance(adapter, LocalStorageAdapter)

    def test_azure_configuration_requires_a_valid_https_adls_endpoint(self):
        for endpoint in ("", "http://account.dfs.core.windows.net", "https://example.com"):
            with self.subTest(endpoint=endpoint):
                with self.assertRaises(CloudStorageConfigurationError):
                    self.config(
                        ".",
                        CLOUD_STORAGE_MODE="azure",
                        AZURE_STORAGE_ACCOUNT_URL=endpoint,
                    )

    def test_azure_configuration_rejects_embedded_credentials_and_invalid_container(self):
        with self.assertRaises(CloudStorageConfigurationError):
            self.config(
                ".",
                CLOUD_STORAGE_MODE="azure",
                AZURE_STORAGE_ACCOUNT_URL="https://user:password@account.dfs.core.windows.net",
            )
        with self.assertRaises(CloudStorageConfigurationError):
            self.config(
                ".",
                CLOUD_STORAGE_MODE="azure",
                AZURE_STORAGE_ACCOUNT_URL="https://account.dfs.core.windows.net",
                AZURE_STORAGE_BRONZE_CONTAINER="Bad_Name",
            )

    def test_local_upload_is_incremental_and_file_can_be_retrieved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "sales.parquet"
            source.write_bytes(b"first")
            adapter = LocalStorageAdapter(root / "storage")

            first = adapter.upload_file(source, "bronze", "2026/sales.parquet")
            unchanged = adapter.upload_file(source, "bronze", "2026/sales.parquet")
            source.write_bytes(b"updated")
            updated = adapter.upload_file(source, "bronze", "2026/sales.parquet")
            target = root / "download" / "sales.parquet"
            retrieved = adapter.download_file("bronze", "2026/sales.parquet", target)

            self.assertEqual(first["status"], "uploaded")
            self.assertEqual(unchanged["status"], "skipped")
            self.assertEqual(updated["status"], "uploaded")
            self.assertEqual(retrieved["bytes"], len(b"updated"))
            self.assertEqual(target.read_bytes(), b"updated")
            self.assertEqual(adapter.list_files("bronze")[0]["name"], "2026/sales.parquet")

    def test_local_adapter_rejects_path_traversal(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "sales.csv"
            source.write_text("order_id\n1\n", encoding="utf-8")
            adapter = LocalStorageAdapter(Path(directory) / "storage")
            with self.assertRaises(CloudStorageConfigurationError):
                adapter.upload_file(source, "bronze", "../outside.csv")

    def test_azure_adapter_uploads_skips_unchanged_files_and_downloads(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "sales.csv"
            source.write_bytes(b"order_id\n1\n")
            config = self.config(
                root,
                CLOUD_STORAGE_MODE="azure",
                AZURE_STORAGE_ACCOUNT_URL="https://account.dfs.core.windows.net",
            )
            adapter = AzureDataLakeStorageAdapter(config, FakeServiceClient())

            uploaded = adapter.upload_file(source, "bronze", "sales.csv")
            unchanged = adapter.upload_file(source, "bronze", "sales.csv")
            target = root / "retrieved.csv"
            retrieved = adapter.download_file("bronze", "sales.csv", target)
            listed = adapter.list_files("bronze")

            self.assertEqual(uploaded["status"], "uploaded")
            self.assertEqual(unchanged["status"], "skipped")
            self.assertEqual(retrieved["bytes"], source.stat().st_size)
            self.assertEqual(target.read_bytes(), source.read_bytes())
            self.assertEqual(listed[0]["name"], "sales.csv")

    def test_azure_storage_errors_are_wrapped_without_exposing_provider_details(self):
        class FailingServiceClient:
            def get_file_system_client(self, file_system):
                raise RuntimeError("sensitive provider response")

        with tempfile.TemporaryDirectory() as directory:
            config = self.config(
                directory,
                CLOUD_STORAGE_MODE="azure",
                AZURE_STORAGE_ACCOUNT_URL="https://account.dfs.core.windows.net",
            )
            adapter = AzureDataLakeStorageAdapter(config, FailingServiceClient())
            with self.assertRaises(CloudStorageError) as raised:
                adapter.test_connection()
            self.assertNotIn("sensitive provider response", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
