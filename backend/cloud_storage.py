import hashlib
import logging
import os
import re
import shutil
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Protocol
from urllib.parse import urlparse


LOGGER = logging.getLogger(__name__)
LAYERS = ("bronze", "silver", "gold")
CONTAINER_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])$")
PROJECT_ROOT = Path(__file__).resolve().parents[1]


class CloudStorageError(RuntimeError):
    def __init__(self, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.retryable = retryable


class CloudStorageConfigurationError(ValueError):
    pass


class StorageAdapter(Protocol):
    mode: str

    def test_connection(self) -> dict[str, Any]: ...

    def list_files(self, layer: str) -> list[dict[str, Any]]: ...

    def upload_file(self, source: Path, layer: str, object_name: str) -> dict[str, Any]: ...

    def download_file(self, layer: str, object_name: str, destination: Path) -> dict[str, Any]: ...


@dataclass(frozen=True)
class CloudStorageConfig:
    mode: str
    local_root: Path
    account_url: str | None
    auth_mode: str
    containers: dict[str, str]
    retry_total: int
    fabric_workspace: str | None
    fabric_lakehouse: str | None

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> "CloudStorageConfig":
        env = os.environ if environ is None else environ
        local_root_value = env.get("CLOUD_LOCAL_ROOT", "data/cloud")
        local_root = Path(local_root_value).expanduser()
        if not local_root.is_absolute():
            local_root = PROJECT_ROOT / local_root
        retry_value = env.get("AZURE_STORAGE_RETRY_TOTAL", "3")
        try:
            retry_total = int(retry_value)
        except ValueError as exc:
            raise CloudStorageConfigurationError(
                "AZURE_STORAGE_RETRY_TOTAL must be an integer from 0 to 10"
            ) from exc
        config = cls(
            mode=env.get("CLOUD_STORAGE_MODE", "local").strip().lower(),
            local_root=local_root,
            account_url=env.get("AZURE_STORAGE_ACCOUNT_URL", "").strip() or None,
            auth_mode=env.get("AZURE_STORAGE_AUTH_MODE", "entra").strip().lower(),
            containers={
                layer: env.get(f"AZURE_STORAGE_{layer.upper()}_CONTAINER", f"dataflow-{layer}")
                .strip()
                .lower()
                for layer in LAYERS
            },
            retry_total=retry_total,
            fabric_workspace=env.get("FABRIC_WORKSPACE_NAME", "").strip() or None,
            fabric_lakehouse=env.get("FABRIC_LAKEHOUSE_NAME", "").strip() or None,
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.mode not in {"local", "azure"}:
            raise CloudStorageConfigurationError(
                "CLOUD_STORAGE_MODE must be either 'local' or 'azure'"
            )
        if not 0 <= self.retry_total <= 10:
            raise CloudStorageConfigurationError(
                "AZURE_STORAGE_RETRY_TOTAL must be an integer from 0 to 10"
            )
        for layer, container in self.containers.items():
            if layer not in LAYERS or not CONTAINER_PATTERN.fullmatch(container):
                raise CloudStorageConfigurationError(
                    f"Invalid ADLS Gen2 file-system name configured for {layer}"
                )
        if self.mode == "azure":
            if self.auth_mode != "entra":
                raise CloudStorageConfigurationError(
                    "Azure storage supports Microsoft Entra authentication only"
                )
            if not self.account_url:
                raise CloudStorageConfigurationError(
                    "AZURE_STORAGE_ACCOUNT_URL is required in Azure mode"
                )
            parsed = urlparse(self.account_url)
            if (
                parsed.scheme != "https"
                or not parsed.hostname
                or not parsed.hostname.endswith(".dfs.core.windows.net")
                or parsed.username
                or parsed.password
                or parsed.query
                or parsed.fragment
                or parsed.path not in {"", "/"}
            ):
                raise CloudStorageConfigurationError(
                    "AZURE_STORAGE_ACCOUNT_URL must be an HTTPS ADLS Gen2 account endpoint"
                )


def _validate_layer(layer: str) -> str:
    if layer not in LAYERS:
        raise CloudStorageConfigurationError("Layer must be bronze, silver, or gold")
    return layer


def _safe_object_name(object_name: str) -> str:
    if not object_name or "\\" in object_name:
        raise CloudStorageConfigurationError("A safe relative object path is required")
    path = PurePosixPath(object_name)
    if not path.parts or path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise CloudStorageConfigurationError("A safe relative object path is required")
    return path.as_posix()


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_metadata(path: Path) -> dict[str, str]:
    stat = path.stat()
    return {
        "source_sha256": _file_digest(path),
        "source_size": str(stat.st_size),
        "source_mtime_ns": str(stat.st_mtime_ns),
    }


class LocalStorageAdapter:
    mode = "local"

    def __init__(self, root: Path):
        self.root = root.expanduser().resolve()

    def _layer_root(self, layer: str) -> Path:
        return self.root / _validate_layer(layer)

    def _target(self, layer: str, object_name: str) -> Path:
        base = self._layer_root(layer).resolve()
        target = (base / _safe_object_name(object_name)).resolve()
        if not target.is_relative_to(base):
            raise CloudStorageConfigurationError("Object path escapes the configured storage root")
        return target

    def test_connection(self) -> dict[str, Any]:
        self.root.mkdir(parents=True, exist_ok=True)
        probe = self.root / f".dataflow360-probe-{uuid.uuid4().hex}"
        try:
            probe.write_text("ok", encoding="ascii")
            probe.unlink()
        except OSError as exc:
            LOGGER.exception(
                "cloud_storage_connection_test_failed",
                extra={"storage_mode": self.mode, "operation": "connection_test"},
            )
            raise CloudStorageError("Local storage read/write validation failed") from exc
        return {
            "success": True,
            "mode": self.mode,
            "message": "Local storage read/write validation succeeded",
            "containers": list(LAYERS),
        }

    def list_files(self, layer: str) -> list[dict[str, Any]]:
        base = self._layer_root(layer)
        if not base.exists():
            return []
        files = []
        for path in sorted(item for item in base.rglob("*") if item.is_file()):
            relative = path.relative_to(base).as_posix()
            stat = path.stat()
            files.append(
                {
                    "name": relative,
                    "size": stat.st_size,
                    "last_modified": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
                    "etag": _file_digest(path),
                }
            )
        return files

    def upload_file(self, source: Path, layer: str, object_name: str) -> dict[str, Any]:
        source = Path(source)
        name = _safe_object_name(object_name)
        target = self._target(layer, name)
        try:
            if not source.is_file():
                raise CloudStorageConfigurationError("The source file does not exist")
            metadata = _source_metadata(source)
            if target.is_file() and _file_digest(target) == metadata["source_sha256"]:
                return {
                    "status": "skipped",
                    "layer": layer,
                    "name": name,
                    "bytes": target.stat().st_size,
                    "etag": metadata["source_sha256"],
                }
            target.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                dir=target.parent, prefix=".dataflow360-", delete=False
            ) as temporary:
                temporary_path = Path(temporary.name)
            try:
                shutil.copyfile(source, temporary_path)
                os.replace(temporary_path, target)
            finally:
                temporary_path.unlink(missing_ok=True)
            return {
                "status": "uploaded",
                "layer": layer,
                "name": name,
                "bytes": target.stat().st_size,
                "etag": metadata["source_sha256"],
            }
        except CloudStorageConfigurationError:
            raise
        except OSError as exc:
            LOGGER.exception(
                "cloud_storage_upload_failed",
                extra={"storage_mode": self.mode, "operation": "upload", "layer": layer, "object_name": name},
            )
            raise CloudStorageError("Local file synchronization failed") from exc

    def download_file(self, layer: str, object_name: str, destination: Path) -> dict[str, Any]:
        name = _safe_object_name(object_name)
        source = self._target(layer, name)
        destination = Path(destination)
        try:
            if not source.is_file():
                raise CloudStorageError("The requested stored file does not exist")
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            return {
                "layer": layer,
                "name": name,
                "bytes": destination.stat().st_size,
            }
        except CloudStorageError:
            raise
        except OSError as exc:
            LOGGER.exception(
                "cloud_storage_download_failed",
                extra={"storage_mode": self.mode, "operation": "download", "layer": layer, "object_name": name},
            )
            raise CloudStorageError("Local file retrieval failed") from exc


class AzureDataLakeStorageAdapter:
    mode = "azure"

    def __init__(self, config: CloudStorageConfig, service_client: Any | None = None):
        self.config = config
        if service_client is not None:
            self.service_client = service_client
            return
        try:
            from azure.core.pipeline.policies import RetryPolicy
            from azure.identity import DefaultAzureCredential
            from azure.storage.filedatalake import DataLakeServiceClient
        except ImportError as exc:
            raise CloudStorageConfigurationError(
                "Azure mode requires the optional packages in backend/requirements-azure.txt"
            ) from exc
        self.service_client = DataLakeServiceClient(
            account_url=config.account_url,
            credential=DefaultAzureCredential(),
            retry_policy=RetryPolicy(retry_total=config.retry_total),
        )

    def _file_system(self, layer: str):
        return self.service_client.get_file_system_client(
            file_system=self.config.containers[_validate_layer(layer)]
        )

    @staticmethod
    def _log_error(operation: str, layer: str | None, object_name: str | None, exc: Exception):
        status_code = getattr(exc, "status_code", None)
        retryable = status_code == 429 or (isinstance(status_code, int) and status_code >= 500)
        LOGGER.exception(
            "azure_storage_operation_failed",
            extra={
                "storage_mode": "azure",
                "operation": operation,
                "layer": layer,
                "object_name": object_name,
                "error_type": type(exc).__name__,
                "retryable": retryable,
            },
        )
        return CloudStorageError(
            f"Azure storage {operation} failed ({type(exc).__name__})",
            retryable=retryable,
        )

    def test_connection(self) -> dict[str, Any]:
        checked = []
        for layer in LAYERS:
            container = self.config.containers[layer]
            try:
                self.service_client.get_file_system_client(
                    file_system=container
                ).get_file_system_properties()
                checked.append(container)
            except Exception as exc:
                raise self._log_error("connection_test", layer, None, exc) from exc
        return {
            "success": True,
            "mode": self.mode,
            "message": "Microsoft Entra authentication and ADLS Gen2 file-system access succeeded",
            "containers": checked,
        }

    def list_files(self, layer: str) -> list[dict[str, Any]]:
        try:
            paths = self._file_system(layer).get_paths(recursive=True)
            return [
                {
                    "name": path.name,
                    "size": path.content_length or 0,
                    "last_modified": path.last_modified.isoformat() if path.last_modified else None,
                    "etag": path.etag,
                }
                for path in paths
                if not path.is_directory
            ]
        except Exception as exc:
            raise self._log_error("list", layer, None, exc) from exc

    def upload_file(self, source: Path, layer: str, object_name: str) -> dict[str, Any]:
        name = _safe_object_name(object_name)
        source = Path(source)
        try:
            _validate_layer(layer)
            if not source.is_file():
                raise CloudStorageConfigurationError("The source file does not exist")
            metadata = _source_metadata(source)
            file_client = self._file_system(layer).get_file_client(name)
            try:
                props = file_client.get_file_properties()
            except Exception as exc:
                if getattr(exc, "status_code", None) != 404:
                    raise
                props = None
            stored = getattr(props, "metadata", None) or {}
            if all(stored.get(key) == value for key, value in metadata.items()):
                return {
                    "status": "skipped",
                    "layer": layer,
                    "name": name,
                    "bytes": getattr(props, "size", source.stat().st_size),
                    "etag": getattr(props, "etag", None),
                }

            parent = PurePosixPath(name).parent
            if str(parent) != ".":
                filesystem = self._file_system(layer)
                current = []
                for segment in parent.parts:
                    current.append(segment)
                    try:
                        filesystem.get_directory_client("/".join(current)).create_directory()
                    except Exception as exc:
                        if getattr(exc, "status_code", None) != 409:
                            raise
            with source.open("rb") as stream:
                file_client.upload_data(stream, overwrite=True)
            file_client.set_metadata(metadata)
            props = file_client.get_file_properties()
            return {
                "status": "uploaded",
                "layer": layer,
                "name": name,
                "bytes": getattr(props, "size", source.stat().st_size),
                "etag": getattr(props, "etag", None),
            }
        except CloudStorageConfigurationError:
            raise
        except Exception as exc:
            raise self._log_error("upload", layer, name, exc) from exc

    def download_file(self, layer: str, object_name: str, destination: Path) -> dict[str, Any]:
        name = _safe_object_name(object_name)
        destination = Path(destination)
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            downloader = self._file_system(layer).get_file_client(name).download_file()
            with destination.open("wb") as stream:
                downloader.readinto(stream)
            return {"layer": layer, "name": name, "bytes": destination.stat().st_size}
        except Exception as exc:
            raise self._log_error("download", layer, name, exc) from exc


def create_storage_adapter(config: CloudStorageConfig | None = None) -> StorageAdapter:
    config = config or CloudStorageConfig.from_env()
    if config.mode == "local":
        return LocalStorageAdapter(config.local_root)
    return AzureDataLakeStorageAdapter(config)
