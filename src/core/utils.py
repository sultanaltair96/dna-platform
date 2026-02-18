"""Utility functions for Polster core operations."""

from __future__ import annotations

import logging
import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

logger = logging.getLogger(__name__)


@contextmanager
def time_operation(operation_name: str) -> Generator[None, None, None]:
    """Context manager to time operations and log execution duration.

    Args:
        operation_name: Descriptive name of the operation being timed

    Example:
        with time_operation("data transformation"):
            # Your code here
            pass
    """
    start_time = time.time()
    logger.info(f"Starting operation: {operation_name}")

    try:
        yield
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Completed operation '{operation_name}' in {duration:.2f} seconds")
    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        logger.error(
            f"Operation '{operation_name}' failed after {duration:.2f} seconds: {e}"
        )
        raise


def log_dataframe_info(df: Any, name: str = "DataFrame") -> None:
    """Log information about a DataFrame.

    Args:
        df: DataFrame object (Polars or Pandas)
        name: Name to use in log messages
    """
    try:
        # Try Polars DataFrame
        if hasattr(df, "shape"):
            rows, cols = df.shape
            logger.info(f"{name}: {rows} rows, {cols} columns")
        elif hasattr(df, "height") and hasattr(df, "width"):
            # Polars DataFrame
            logger.info(f"{name}: {df.height} rows, {df.width} columns")
        else:
            logger.info(f"{name}: {type(df)} (unable to determine size)")

        # Log memory usage if available
        if hasattr(df, "estimated_size"):
            size_mb = df.estimated_size() / (1024 * 1024)
            logger.debug(f"{name} estimated memory usage: {size_mb:.2f} MB")
    except Exception as e:
        logger.debug(f"Unable to log {name} info: {e}")


def validate_dataframe(df: Any, required_columns: list[str] | None = None) -> None:
    """Validate DataFrame has expected structure.

    Args:
        df: DataFrame to validate
        required_columns: List of column names that must be present

    Raises:
        ValueError: If validation fails
    """
    if df is None:
        raise ValueError("DataFrame is None")

    # Check if empty
    if hasattr(df, "is_empty") and df.is_empty():
        raise ValueError("DataFrame is empty")
    elif hasattr(df, "empty") and df.empty:
        raise ValueError("DataFrame is empty")
    elif hasattr(df, "shape") and df.shape[0] == 0:
        raise ValueError("DataFrame is empty")

    # Check required columns
    if required_columns:
        missing_columns = []
        for col in required_columns:
            if hasattr(df, "columns"):
                if col not in df.columns:
                    missing_columns.append(col)
            elif hasattr(df, "column_names"):
                if col not in df.column_names():
                    missing_columns.append(col)

        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")


class PolsterTimer:
    """Timer utility for tracking execution times across operations."""

    def __init__(self):
        self.operations: dict[str, float] = {}
        self.start_times: dict[str, float] = {}

    def start(self, operation: str) -> None:
        """Start timing an operation."""
        self.start_times[operation] = time.time()
        logger.debug(f"Started timing: {operation}")

    def stop(self, operation: str) -> float:
        """Stop timing an operation and return duration."""
        if operation not in self.start_times:
            logger.warning(f"Stop called for unstarted operation: {operation}")
            return 0.0

        duration = time.time() - self.start_times[operation]
        self.operations[operation] = duration
        logger.info(f"Operation '{operation}' completed in {duration:.2f} seconds")
        del self.start_times[operation]
        return duration

    def get_duration(self, operation: str) -> float | None:
        """Get the duration of a completed operation."""
        return self.operations.get(operation)

    def log_summary(self) -> None:
        """Log a summary of all timed operations."""
        if not self.operations:
            logger.info("No operations timed")
            return

        logger.info("Operation timing summary:")
        total_time = 0.0
        for op, duration in self.operations.items():
            logger.info(f"  {op}: {duration:.2f}s")
            total_time += duration
        logger.info(f"  Total: {total_time:.2f}s")


# Global timer instance
timer = PolsterTimer()
