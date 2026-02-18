"""Silver layer - Clean and transform customers data."""

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
    """Transform bronze customer data to silver layer."""
    with time_operation("silver customers transformation"):
        logger.info("Starting silver customers transformation")

        df = read_parquet_latest("bronze", "bronze_customers_")

        validate_dataframe(
            df, ["customer_id", "first_name", "last_name", "email", "join_date"]
        )
        log_dataframe_info(df, "Bronze customers input")

        cleaned = (
            df.lazy()
            .with_columns(
                [
                    pl.col("first_name").str.strip_chars().str.to_titlecase(),
                    pl.col("last_name").str.strip_chars().str.to_titlecase(),
                    pl.col("email").str.to_lowercase().str.strip_chars(),
                    pl.col("phone").str.replace_all(r"[^\d]", "").str.strip_chars(),
                    pl.col("city").str.strip_chars().str.to_titlecase(),
                    pl.col("state").str.strip_chars().str.to_uppercase(),
                    pl.col("zip_code").str.strip_chars(),
                    pl.col("occupation").str.strip_chars().str.to_titlecase(),
                ]
            )
            .with_columns(
                [
                    pl.col("date_of_birth").cast(pl.Date),
                    pl.col("join_date").cast(pl.Datetime),
                ]
            )
            .with_columns(
                [
                    pl.col("annual_income").cast(pl.Float64),
                    pl.col("credit_score").cast(pl.Int32),
                ]
            )
            .filter(pl.col("email").str.contains("@"))
            .filter(pl.col("credit_score") >= 300)
            .filter(pl.col("credit_score") <= 850)
            .collect()
        )

        log_dataframe_info(cleaned, "Transformed silver customers")

        transform_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        cleaned = cleaned.with_columns(pl.lit(transform_time).alias("transformed_at"))

        validate_dataframe(cleaned, ["transformed_at"])

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = write_parquet(
            cleaned, "silver", f"silver_clean_customers_{timestamp}.parquet"
        )

        metadata = {
            "row_count": len(cleaned),
            "columns": list(cleaned.columns),
            "dtypes": {
                col: str(dtype) for col, dtype in zip(cleaned.columns, cleaned.dtypes)
            },
        }

        logger.info(f"Silver customers transformation completed: {output_path}")
        return output_path, cleaned, metadata


if __name__ == "__main__":
    result = transform()
    print(f"Transformation completed: {result[0]}")
