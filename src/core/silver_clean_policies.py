"""Silver layer - Clean and transform policies data."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Tuple

import polars as pl

try:
    from .storage import read_parquet_latest, write_parquet
    from .utils import log_dataframe_info, time_operation, validate_dataframe
except ImportError:
    import os
    import sys

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from core.storage import read_parquet_latest, write_parquet
    from core.utils import log_dataframe_info, time_operation, validate_dataframe

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def transform() -> Tuple[str, pl.DataFrame, dict]:
    """Transform bronze policy data to silver layer."""
    with time_operation("silver policies transformation"):
        logger.info("Starting silver policies transformation")

        df = read_parquet_latest("bronze", "bronze_policies_")

        validate_dataframe(
            df, ["policy_id", "customer_id", "policy_type", "premium", "status"]
        )
        log_dataframe_info(df, "Bronze policies input")

        cleaned = (
            df.lazy()
            .with_columns(
                [
                    pl.col("policy_number").str.strip_chars().str.to_uppercase(),
                    pl.col("policy_type").str.strip_chars().str.to_titlecase(),
                    pl.col("coverage_type").str.strip_chars(),
                ]
            )
            .with_columns(
                [
                    pl.col("premium").cast(pl.Float64).round(2),
                    pl.col("coverage_amount").cast(pl.Float64),
                    pl.col("deductible").cast(pl.Int32),
                    pl.col("start_date").cast(pl.Datetime),
                    pl.col("end_date").cast(pl.Datetime),
                ]
            )
            .filter(pl.col("premium") > 0)
            .filter(pl.col("coverage_amount") > 0)
            .filter(pl.col("customer_id").is_not_null())
            .filter(pl.col("agent_id").is_not_null())
            .collect()
        )

        log_dataframe_info(cleaned, "Transformed silver policies")

        transform_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        cleaned = cleaned.with_columns(pl.lit(transform_time).alias("transformed_at"))

        validate_dataframe(cleaned, ["transformed_at"])

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = write_parquet(
            cleaned, "silver", f"silver_clean_policies_{timestamp}.parquet"
        )

        metadata = {
            "row_count": len(cleaned),
            "columns": list(cleaned.columns),
            "dtypes": {
                col: str(dtype) for col, dtype in zip(cleaned.columns, cleaned.dtypes)
            },
        }

        logger.info(f"Silver policies transformation completed: {output_path}")
        return output_path, cleaned, metadata


if __name__ == "__main__":
    result = transform()
    print(f"Transformation completed: {result[0]}")
