"""Silver layer - Clean and transform payments data."""

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
    """Transform bronze payments data to silver layer."""
    with time_operation("silver payments transformation"):
        logger.info("Starting silver payments transformation")

        df = read_parquet_latest("bronze", "bronze_payments_")

        validate_dataframe(
            df, ["payment_id", "policy_id", "amount", "payment_date", "status"]
        )
        log_dataframe_info(df, "Bronze payments input")

        cleaned = (
            df.lazy()
            .with_columns(
                [
                    pl.col("payment_number").str.strip_chars().str.to_uppercase(),
                    pl.col("payment_method").str.strip_chars().str.to_titlecase(),
                    pl.col("transaction_id").str.strip_chars().str.to_uppercase(),
                    pl.col("reference_number").str.strip_chars().str.to_uppercase(),
                ]
            )
            .with_columns(
                [
                    pl.col("amount").cast(pl.Float64).round(2),
                    pl.col("payment_date").cast(pl.Datetime),
                ]
            )
            .filter(pl.col("policy_id").is_not_null())
            .filter(pl.col("amount") != 0)
            .collect()
        )

        log_dataframe_info(cleaned, "Transformed silver payments")

        transform_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        cleaned = cleaned.with_columns(pl.lit(transform_time).alias("transformed_at"))

        validate_dataframe(cleaned, ["transformed_at"])

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = write_parquet(
            cleaned, "silver", f"silver_clean_payments_{timestamp}.parquet"
        )

        metadata = {
            "row_count": len(cleaned),
            "columns": list(cleaned.columns),
            "dtypes": {
                col: str(dtype) for col, dtype in zip(cleaned.columns, cleaned.dtypes)
            },
        }

        logger.info(f"Silver payments transformation completed: {output_path}")
        return output_path, cleaned, metadata


if __name__ == "__main__":
    result = transform()
    print(f"Transformation completed: {result[0]}")
