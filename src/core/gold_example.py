"""Gold layer example - Data aggregation.

This file demonstrates how to aggregate silver data and write it to the gold layer.
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


def aggregate() -> tuple[str, pl.DataFrame, dict]:
    """Aggregate silver data and write to gold layer.

    Returns:
        tuple[str, pl.DataFrame, dict]: Tuple containing:
            - Path to the written parquet file
            - DataFrame with aggregated data
            - Metadata dictionary with row count, columns, etc.

    Raises:
        RuntimeError: If aggregation or writing fails
        ValueError: If input data is invalid
    """
    with time_operation("gold layer data aggregation"):
        logger.info("Starting gold layer data aggregation")

        try:
            # Read latest silver orders data
            logger.debug("Reading latest silver layer data")
            df = read_parquet_latest("silver", "silver_orders_")

            # Validate input data
            validate_dataframe(df, ["status", "total_amount"])
            log_dataframe_info(df, "Silver input data")

            logger.info(f"Processing {len(df)} silver records")

            # Simple aggregation: total sales and order count by status
            result = (
                df.group_by("status")
                .agg(
                    total_sales=pl.col("total_amount").sum().round(2),
                    order_count=pl.len(),
                    avg_order_value=(pl.col("total_amount").sum() / pl.len()).round(2),
                )
                .sort("total_sales", descending=True)
            )

            # Validate aggregation results
            validate_dataframe(
                result, ["total_sales", "order_count", "avg_order_value"]
            )
            log_dataframe_info(result, "Aggregated gold data")

            logger.info(f"Aggregated data: {len(result)} status groups")

            # Add aggregation timestamp
            aggregate_time = (
                datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            )
            result = result.with_columns(pl.lit(aggregate_time).alias("aggregated_at"))

            validate_dataframe(result, ["aggregated_at"])

            # Write to storage
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            output_path = write_parquet(
                result, "gold", f"gold_order_summary_{timestamp}.parquet"
            )

            # Prepare metadata for Dagster
            metadata = {
                "row_count": len(result),
                "columns": list(result.columns),
                "dtypes": {
                    col: str(dtype)
                    for col, dtype in zip(result.columns, result.dtypes, strict=False)
                },
            }

            logger.info(f"Gold layer aggregation completed successfully: {output_path}")
            return output_path, result, metadata

        except Exception as e:
            logger.error(f"Gold layer aggregation failed: {e}")
            raise RuntimeError(f"Data aggregation failed: {e}") from e


if __name__ == "__main__":
    try:
        result = aggregate()
        print(f"Aggregation completed: {result}")
    except Exception as e:
        print(f"Aggregation failed: {e}")
        import sys

        sys.exit(1)
