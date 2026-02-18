"""Silver layer example - Data transformation.

This file demonstrates how to transform bronze data and write it to the silver layer.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import polars as pl

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

try:
    # Try relative imports (when run as module through Dagster)
    from .storage import read_parquet_latest, write_parquet
    from .utils import log_dataframe_info, time_operation, validate_dataframe
except ImportError:
    # Fall back to absolute imports (when run directly)
    import os
    import sys

    # Add src directory to path for absolute imports
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from core.storage import read_parquet_latest, write_parquet
    from core.utils import log_dataframe_info, time_operation, validate_dataframe


def transform() -> tuple[str, pl.DataFrame, dict]:
    """Transform bronze data and write to silver layer.

    Returns:
        tuple[str, pl.DataFrame, dict]: Tuple containing:
            - Path to the written parquet file
            - DataFrame with transformed data
            - Metadata dictionary with row count, columns, etc.

    Raises:
        RuntimeError: If transformation or writing fails
        ValueError: If input data is invalid
    """
    with time_operation("silver layer data transformation"):
        logger.info("Starting silver layer data transformation")

        try:
            # Read latest bronze orders data
            logger.debug("Reading latest bronze layer data")
            df = read_parquet_latest("bronze", "bronze_orders_")

            # Validate input data
            validate_dataframe(
                df, ["order_id", "customer_id", "order_date", "status", "total_amount"]
            )
            log_dataframe_info(df, "Bronze input data")

            logger.info(f"Processing {len(df)} bronze records")

            # Simple transformation: filter out cancelled orders and standardize data types
            # Use lazy evaluation for better performance with large datasets
            cleaned = (
                df.lazy()
                .filter(pl.col("status") != "cancelled")
                .with_columns(
                    [
                        pl.col("order_date").cast(pl.Datetime),
                        pl.col("total_amount").cast(pl.Float64).round(2),
                    ]
                )
                .with_columns(pl.col("order_date").dt.date().alias("order_day"))
                .collect()  # Execute the lazy plan
            )

            if cleaned.is_empty():
                logger.warning(
                    "All bronze records were filtered out (cancelled orders)"
                )
                # Create empty DataFrame with correct schema
                cleaned = pl.DataFrame(
                    schema={
                        "order_id": pl.Int64,
                        "customer_id": pl.Int64,
                        "order_date": pl.Datetime,
                        "status": pl.Utf8,
                        "total_amount": pl.Float64,
                        "fetched_at": pl.Utf8,
                        "order_day": pl.Date,
                    }
                )

            log_dataframe_info(cleaned, "Transformed silver data")

            # Add transformation timestamp
            transform_time = (
                datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            )
            cleaned = cleaned.with_columns(
                pl.lit(transform_time).alias("transformed_at")
            )

            validate_dataframe(cleaned, ["transformed_at"])

            # Write to storage
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            output_path = write_parquet(
                cleaned, "silver", f"silver_orders_{timestamp}.parquet"
            )

            # Prepare metadata for Dagster
            metadata = {
                "row_count": len(cleaned),
                "columns": list(cleaned.columns),
                "dtypes": {
                    col: str(dtype)
                    for col, dtype in zip(cleaned.columns, cleaned.dtypes, strict=False)
                },
            }

            logger.info(
                f"Silver layer transformation completed successfully: {output_path}"
            )
            return output_path, cleaned, metadata

        except Exception as e:
            logger.error(f"Silver layer transformation failed: {e}")
            raise RuntimeError(f"Data transformation failed: {e}") from e


if __name__ == "__main__":
    try:
        result = transform()
        print(f"Transformation completed: {result}")
    except Exception as e:
        print(f"Transformation failed: {e}")
        import sys

        sys.exit(1)
