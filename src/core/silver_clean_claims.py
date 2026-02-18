"""Silver layer - Clean and transform claims data."""

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
    """Transform bronze claims data to silver layer."""
    with time_operation("silver claims transformation"):
        logger.info("Starting silver claims transformation")

        df = read_parquet_latest("bronze", "bronze_claims_")

        validate_dataframe(
            df, ["claim_id", "policy_id", "claim_type", "claimed_amount", "status"]
        )
        log_dataframe_info(df, "Bronze claims input")

        cleaned = (
            df.lazy()
            .with_columns(
                [
                    pl.col("claim_number").str.strip_chars().str.to_uppercase(),
                    pl.col("claim_type").str.strip_chars().str.to_titlecase(),
                ]
            )
            .with_columns(
                [
                    pl.col("claimed_amount").cast(pl.Float64).round(2),
                    pl.col("approved_amount").cast(pl.Float64),
                    pl.col("incident_date").cast(pl.Datetime),
                    pl.col("reported_date").cast(pl.Datetime),
                ]
            )
            .filter(pl.col("claimed_amount") > 0)
            .filter(pl.col("policy_id").is_not_null())
            .collect()
        )

        cleaned = cleaned.with_columns(
            pl.when(pl.col("approved_amount").is_null())
            .then(pl.lit(0.0))
            .otherwise(pl.col("approved_amount"))
            .alias("approved_amount")
        )

        log_dataframe_info(cleaned, "Transformed silver claims")

        transform_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        cleaned = cleaned.with_columns(pl.lit(transform_time).alias("transformed_at"))

        validate_dataframe(cleaned, ["transformed_at"])

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = write_parquet(
            cleaned, "silver", f"silver_clean_claims_{timestamp}.parquet"
        )

        metadata = {
            "row_count": len(cleaned),
            "columns": list(cleaned.columns),
            "dtypes": {
                col: str(dtype) for col, dtype in zip(cleaned.columns, cleaned.dtypes)
            },
        }

        logger.info(f"Silver claims transformation completed: {output_path}")
        return output_path, cleaned, metadata


if __name__ == "__main__":
    result = transform()
    print(f"Transformation completed: {result[0]}")
