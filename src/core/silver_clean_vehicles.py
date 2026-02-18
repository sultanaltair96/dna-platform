"""Silver layer - Clean and transform vehicles data."""

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
    """Transform bronze vehicle data to silver layer."""
    with time_operation("silver vehicles transformation"):
        logger.info("Starting silver vehicles transformation")

        df = read_parquet_latest("bronze", "bronze_vehicles_")

        validate_dataframe(
            df, ["vehicle_id", "policy_id", "vin", "make", "model", "year"]
        )
        log_dataframe_info(df, "Bronze vehicles input")

        cleaned = (
            df.lazy()
            .with_columns(
                [
                    pl.col("vin").str.strip_chars().str.to_uppercase(),
                    pl.col("make").str.strip_chars().str.to_titlecase(),
                    pl.col("model").str.strip_chars().str.to_titlecase(),
                    pl.col("color").str.strip_chars().str.to_titlecase(),
                    pl.col("vehicle_type").str.strip_chars().str.to_titlecase(),
                    pl.col("engine_type").str.strip_chars().str.to_titlecase(),
                    pl.col("registration_state").str.strip_chars().str.to_uppercase(),
                ]
            )
            .with_columns(
                [
                    pl.col("year").cast(pl.Int32),
                    pl.col("mileage").cast(pl.Int32),
                    pl.col("registration_expiry").cast(pl.Date),
                ]
            )
            .filter(pl.col("year") >= 1990)
            .filter(pl.col("year") <= 2025)
            .collect()
        )

        log_dataframe_info(cleaned, "Transformed silver vehicles")

        transform_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        cleaned = cleaned.with_columns(pl.lit(transform_time).alias("transformed_at"))

        validate_dataframe(cleaned, ["transformed_at"])

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = write_parquet(
            cleaned, "silver", f"silver_clean_vehicles_{timestamp}.parquet"
        )

        metadata = {
            "row_count": len(cleaned),
            "columns": list(cleaned.columns),
            "dtypes": {
                col: str(dtype) for col, dtype in zip(cleaned.columns, cleaned.dtypes)
            },
        }

        logger.info(f"Silver vehicles transformation completed: {output_path}")
        return output_path, cleaned, metadata


if __name__ == "__main__":
    result = transform()
    print(f"Transformation completed: {result[0]}")
