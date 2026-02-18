"""Storage abstraction for local and ADLS backends."""

from __future__ import annotations

import io
import logging
import os
from typing import Literal
from urllib.parse import urlparse

import polars as pl

from .paths import DATA_DIR

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

StorageBackend = Literal["local", "adls"]


def _load_env(name: str, default: str | None = None) -> str | None:
    """Load environment variable with optional default."""
    value = os.getenv(name, default)
    return value


def get_storage_backend() -> StorageBackend:
    """Get the configured storage backend."""
    backend = (_load_env("STORAGE_BACKEND", "local") or "local").lower()
    if backend not in {"local", "adls"}:
        logger.error(f"Unsupported STORAGE_BACKEND: {backend}")
        raise ValueError(
            f"Unsupported STORAGE_BACKEND: {backend}. Must be 'local' or 'adls'"
        )
    logger.debug(f"Using storage backend: {backend}")
    return backend  # type: ignore[return-value]


def is_adls_configured() -> bool:
    """Check if ADLS is properly configured."""
    required_vars = ["ADLS_ACCOUNT_NAME", "ADLS_ACCOUNT_KEY", "ADLS_CONTAINER"]
    missing_vars = [var for var in required_vars if not _load_env(var)]

    if missing_vars:
        logger.debug(f"ADLS configuration incomplete. Missing: {missing_vars}")
        return False

    logger.debug("ADLS configuration is complete")
    return True


def _adls_base_path() -> str:
    """Get ADLS base path."""
    return (
        _load_env("ADLS_BASE_PATH", "dna-platform/data") or "dna-platform/data"
    ).strip("/")


def get_adls_base_path() -> str:
    """Get ADLS base path (public interface)."""
    return _adls_base_path()


def resolve_path(layer: str, filename: str) -> str:
    """Resolve file path for the configured storage backend."""
    if not layer or not isinstance(layer, str):
        raise ValueError(f"Layer must be a non-empty string, got: {layer}")
    if not filename or not isinstance(filename, str):
        raise ValueError(f"Filename must be a non-empty string, got: {filename}")

    backend = get_storage_backend()

    try:
        if backend == "local":
            path = os.path.join(DATA_DIR, layer, filename)
            logger.debug(f"Resolved local path: {path}")
            return path
        elif backend == "adls":
            account_name = _load_env("ADLS_ACCOUNT_NAME")
            container = _load_env("ADLS_CONTAINER")
            if not account_name or not container:
                raise ValueError(
                    "ADLS_ACCOUNT_NAME and ADLS_CONTAINER are required for ADLS storage. "
                    "Please configure ADLS credentials or switch to local storage."
                )
            base_path = _adls_base_path()
            path = f"abfss://{container}@{account_name}.dfs.core.windows.net/{base_path}/{layer}/{filename}"
            logger.debug(f"Resolved ADLS path: {path}")
            return path
        else:
            # This should never happen due to get_storage_backend validation
            raise ValueError(f"Unsupported backend: {backend}")
    except Exception as e:
        logger.error(
            f"Failed to resolve path for layer='{layer}', filename='{filename}': {e}"
        )
        raise


def write_azure_data_lake(df: pl.DataFrame, layer: str, filename: str) -> str:
    """Write a DataFrame to Azure Data Lake Storage."""
    if df is None:
        raise ValueError("DataFrame cannot be None")
    if df.is_empty():
        logger.warning("Writing empty DataFrame to ADLS")

    output_uri = resolve_path(layer, filename)
    parsed = urlparse(output_uri)

    account_name = _load_env("ADLS_ACCOUNT_NAME")
    account_key = _load_env("ADLS_ACCOUNT_KEY")
    container = _load_env("ADLS_CONTAINER")

    if not account_name or not account_key or not container:
        raise ValueError(
            "ADLS credentials not configured. Required: ADLS_ACCOUNT_NAME, ADLS_ACCOUNT_KEY, ADLS_CONTAINER"
        )

    output_path = parsed.path.lstrip("/")
    logger.info(
        f"Writing DataFrame ({df.shape[0]} rows, {df.shape[1]} columns) to ADLS: {output_uri}"
    )

    try:
        from azure.storage.filedatalake import DataLakeServiceClient
    except ModuleNotFoundError:
        raise ModuleNotFoundError(
            "azure-storage-filedatalake not installed. Install with: pip install azure-storage-filedatalake"
        ) from None

    try:
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        buffer.seek(0)
        data_size = buffer.tell()
        buffer.seek(0)  # Reset for upload

        logger.debug(f"Prepared parquet data: {data_size} bytes")

        data_lake_service_client = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=account_key,
        )

        # Test connection and container access
        file_system_client = data_lake_service_client.get_file_system_client(container)

        # Create container if it doesn't exist
        try:
            file_system_client.get_file_system_properties()
            logger.debug("Container exists and is accessible")
        except Exception:
            logger.info(f"Creating container: {container}")
            file_system_client.create_file_system()

        # Upload the file
        file_client = file_system_client.get_file_client(output_path)

        # Create directory structure if needed
        dir_path = os.path.dirname(output_path)
        if dir_path and dir_path != "/":
            try:
                file_system_client.create_directory(dir_path)
                logger.debug(f"Created directory structure: {dir_path}")
            except Exception as e:
                logger.debug(f"Directory creation failed (may already exist): {e}")

        file_client.upload_data(buffer.read(), overwrite=True)
        logger.info(f"Successfully uploaded {data_size} bytes to ADLS: {output_uri}")

        return output_uri

    except Exception as e:
        logger.error(f"Failed to write to ADLS {output_uri}: {e}")
        raise RuntimeError(f"ADLS upload failed: {e}") from e


def write_parquet(df: pl.DataFrame, layer: str, filename: str) -> str:
    """Write a DataFrame to parquet storage with dual writing support."""
    if df is None:
        raise ValueError("DataFrame cannot be None")
    if not layer or not filename:
        raise ValueError("Layer and filename must be non-empty strings")

    backend = get_storage_backend()
    logger.info(f"Writing DataFrame to {layer}/{filename} using {backend} backend")

    # Check if ADLS fallback to local is enabled
    adls_fallback = (
        _load_env("ADLS_FALLBACK_TO_LOCAL", "false") or "false"
    ).lower() == "true"

    # Write full data to ADLS if configured
    adls_path = None
    adls_configured = backend == "adls" or is_adls_configured()

    if adls_configured:
        try:
            logger.debug("Attempting ADLS write")
            adls_path = write_azure_data_lake(df, layer, filename)
            logger.info(f"ADLS write successful: {adls_path}")
        except Exception as e:
            if adls_fallback:
                logger.warning(f"ADLS write failed, falling back to local storage: {e}")
                adls_path = None
            else:
                logger.error(f"ADLS write failed and fallback is disabled: {e}")
                raise RuntimeError(f"ADLS write failed (fallback disabled): {e}") from e

    # Write sample to local unless disabled
    try:
        disable_local = (
            _load_env("DISABLE_LOCAL_SAMPLE", "false") or "false"
        ).lower() == "true"

        if not disable_local:
            sample_size_str = _load_env("DUAL_WRITE_SAMPLE_SIZE", "50") or "50"
            try:
                sample_size = int(sample_size_str)
                if sample_size < 0:
                    raise ValueError("Sample size cannot be negative")
                sample_size = min(sample_size, len(df))  # Don't exceed DataFrame size
            except (ValueError, TypeError):
                logger.warning(
                    f"Invalid DUAL_WRITE_SAMPLE_SIZE '{sample_size_str}', using default of 50"
                )
                sample_size = 50

            df_sample = df.limit(sample_size)
            sample_filename = f"sample_{filename}"
            local_sample_path = _write_parquet_local_fallback(
                df_sample, layer, sample_filename
            )
            logger.debug(f"Local sample written: {local_sample_path}")
    except Exception as e:
        logger.error(f"Failed to write local sample: {e}")
        # Don't raise here - continue with main write

    # Return the primary path (ADLS if successful)
    if adls_path:
        # If ADLS succeeded and fallback is enabled, also write full data locally
        if adls_fallback:
            try:
                _write_parquet_local_fallback(df, layer, filename)
                logger.debug("Local fallback write completed (ADLS primary)")
            except Exception as e:
                logger.warning(f"Local fallback write failed (non-critical): {e}")
        return adls_path
    else:
        # This should only be reached if ADLS is not configured
        try:
            return _write_parquet_local_fallback(df, layer, filename)
        except Exception as e:
            logger.error(f"Failed to write to local storage: {e}")
            raise RuntimeError(
                f"Storage write failed for {layer}/{filename}: {e}"
            ) from e


def _write_parquet_local_fallback(df: pl.DataFrame, layer: str, filename: str) -> str:
    """Fallback to local storage if ADLS is not configured."""
    if df is None:
        raise ValueError("DataFrame cannot be None")

    output_path = os.path.join(DATA_DIR, layer, filename)
    logger.debug(f"Writing to local path: {output_path}")

    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Write with error handling
        df.write_parquet(output_path)
        logger.info(
            f"Successfully wrote {len(df)} rows to local storage: {output_path}"
        )

        return output_path
    except Exception as e:
        logger.error(f"Failed to write parquet file to {output_path}: {e}")
        raise RuntimeError(f"Local storage write failed: {e}") from e


def read_parquet_latest(
    layer: str, prefix: str, use_memory_map: bool = True
) -> pl.DataFrame:
    """Read the latest parquet file for a given prefix.

    Args:
        layer: Data layer (bronze, silver, gold)
        prefix: Filename prefix to match
        use_memory_map: Use memory mapping for better performance with large files

    Returns:
        Polars DataFrame

    Raises:
        ValueError: If layer or prefix are invalid
        FileNotFoundError: If no matching files are found
        RuntimeError: If reading fails
    """
    if not layer or not isinstance(layer, str):
        raise ValueError(f"Layer must be a non-empty string, got: {layer}")
    if not prefix or not isinstance(prefix, str):
        raise ValueError(f"Prefix must be a non-empty string, got: {prefix}")

    backend = get_storage_backend()
    logger.info(
        f"Reading latest parquet file from {layer} layer with prefix '{prefix}' using {backend} backend"
    )

    try:
        if backend == "local":
            return _read_latest_local(layer, prefix, use_memory_map)
        elif backend == "adls":
            account_name = _load_env("ADLS_ACCOUNT_NAME")
            account_key = _load_env("ADLS_ACCOUNT_KEY")
            container = _load_env("ADLS_CONTAINER")
            base_path = _adls_base_path()

            if not account_name or not account_key or not container:
                logger.warning(
                    "ADLS credentials not configured, falling back to local storage"
                )
                return _read_latest_local(layer, prefix, use_memory_map)

            try:
                from azure.storage.filedatalake import DataLakeServiceClient
            except ModuleNotFoundError:
                logger.warning(
                    "azure-storage-filedatalake not installed, falling back to local storage"
                )
                return _read_latest_local(layer, prefix, use_memory_map)

            try:
                data_lake_service_client = DataLakeServiceClient(
                    account_url=f"https://{account_name}.dfs.core.windows.net",
                    credential=account_key,
                )
                file_system_client = data_lake_service_client.get_file_system_client(
                    container
                )

                directory_path = f"{base_path}/{layer}".strip("/")
                logger.debug(f"Scanning ADLS directory: {directory_path}")

                candidates = []
                try:
                    for path_item in file_system_client.get_paths(path=directory_path):
                        if path_item.is_directory:
                            continue
                        name = path_item.name.split("/")[-1]
                        if name.startswith(prefix) and name.endswith(".parquet"):
                            candidates.append(path_item.name)
                except Exception as e:
                    logger.error(
                        f"Failed to list files in ADLS directory {directory_path}: {e}"
                    )
                    raise FileNotFoundError(
                        f"Cannot access ADLS directory {directory_path}: {e}"
                    ) from e

                if not candidates:
                    raise FileNotFoundError(
                        f"No parquet files found in {directory_path} with prefix {prefix}"
                    )

                latest_path = max(candidates)
                logger.debug(f"Found latest file: {latest_path}")

                file_client = file_system_client.get_file_client(latest_path)
                data = file_client.download_file().readall()
                buffer = io.BytesIO(data)
                df = pl.read_parquet(
                    buffer, memory_map=False
                )  # Memory mapping not available for BytesIO

                logger.info(
                    f"Successfully read {len(df)} rows from ADLS: {latest_path}"
                )
                return df

            except Exception as e:
                logger.warning(f"ADLS read failed, falling back to local storage: {e}")
                return _read_latest_local(layer, prefix, use_memory_map)
        else:
            raise ValueError(f"Unsupported backend: {backend}")
    except Exception as e:
        logger.error(
            f"Failed to read parquet file from {layer} layer with prefix '{prefix}': {e}"
        )
        if isinstance(e, (ValueError, FileNotFoundError)):
            raise
        raise RuntimeError(f"Storage read failed: {e}") from e


def _read_latest_local(
    layer: str, prefix: str, use_memory_map: bool = True
) -> pl.DataFrame:
    """Read the latest parquet file from local storage.

    Args:
        layer: Data layer (bronze, silver, gold)
        prefix: Filename prefix to match
        use_memory_map: Use memory mapping for better performance with large files

    Returns:
        Polars DataFrame

    Raises:
        FileNotFoundError: If no matching files are found
        RuntimeError: If reading fails
    """
    directory = os.path.join(DATA_DIR, layer)
    logger.debug(f"Scanning local directory: {directory}")

    try:
        if not os.path.exists(directory):
            raise FileNotFoundError(f"Data directory does not exist: {directory}")

        candidates = [
            filename
            for filename in os.listdir(directory)
            if filename.startswith(prefix) and filename.endswith(".parquet")
        ]

        if not candidates:
            raise FileNotFoundError(
                f"No parquet files found in {directory} with prefix {prefix}"
            )

        latest = max(candidates)
        file_path = os.path.join(directory, latest)
        logger.debug(f"Reading latest file: {file_path}")

        df = pl.read_parquet(file_path, memory_map=use_memory_map)
        logger.info(f"Successfully read {len(df)} rows from local storage: {file_path}")

        return df

    except Exception as e:
        logger.error(f"Failed to read parquet file from {directory}: {e}")
        if isinstance(e, FileNotFoundError):
            raise
        raise RuntimeError(f"Local storage read failed: {e}") from e
