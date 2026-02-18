"""Silver layer - Clean and transform agents data."""

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
    """Transform bronze agent data to silver layer."""
    with time_operation("silver agents transformation"):
        logger.info("Starting silver agents transformation")

        df = read_parquet_latest("bronze", "bronze_agents_")

        validate_dataframe(
            df, ["agent_id", "first_name", "last_name", "email", "region"]
        )
        log_dataframe_info(df, "Bronze agents input")

        cleaned = (
            df.lazy()
            .with_columns(
                [
                    pl.col("first_name").str.strip_chars().str.to_titlecase(),
                    pl.col("last_name").str.strip_chars().str.to_titlecase(),
                    pl.col("email").str.to_lowercase().str.strip_chars(),
                    pl.col("phone").str.replace_all(r"[^\d]", "").str.strip_chars(),
                    pl.col("region").str.strip_chars().str.to_titlecase(),
                    pl.col("specialty").str.strip_chars().str.to_titlecase(),
                    pl.col("license_number").str.strip_chars().str.to_uppercase(),
                ]
            )
            .with_columns(
                [
                    pl.col("commission_rate").cast(pl.Float64),
                    pl.col("hire_date").cast(pl.Date),
                ]
            )
            .filter(pl.col("agent_id").is_not_null())
            .collect()
        )

        log_dataframe_info(cleaned, "Transformed silver agents")

        transform_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        cleaned = cleaned.with_columns(pl.lit(transform_time).alias("transformed_at"))

        validate_dataframe(cleaned, ["transformed_at"])

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = write_parquet(
            cleaned, "silver", f"silver_clean_agents_{timestamp}.parquet"
        )

        metadata = {
            "row_count": len(cleaned),
            "columns": list(cleaned.columns),
            "dtypes": {
                col: str(dtype) for col, dtype in zip(cleaned.columns, cleaned.dtypes)
            },
        }

        logger.info(f"Silver agents transformation completed: {output_path}")
        return output_path, cleaned, metadata


if __name__ == "__main__":
    result = transform()
    print(f"Transformation completed: {result[0]}")
