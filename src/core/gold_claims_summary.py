"""Gold layer - Claims summary aggregation."""

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


def aggregate() -> Tuple[str, pl.DataFrame, dict]:
    """Aggregate claims data by status and type."""
    with time_operation("gold claims summary aggregation"):
        logger.info("Starting gold claims summary aggregation")

        df = read_parquet_latest("silver", "silver_clean_claims_")

        validate_dataframe(
            df, ["claim_type", "status", "claimed_amount", "approved_amount"]
        )
        log_dataframe_info(df, "Silver claims input")

        result = (
            df.group_by(["claim_type", "status"])
            .agg(
                total_claims=pl.len(),
                total_claimed=pl.col("claimed_amount").sum().round(2),
                total_approved=pl.col("approved_amount").sum().round(2),
                avg_claimed=pl.col("claimed_amount").mean().round(2),
                avg_approved=pl.col("approved_amount").mean().round(2),
            )
            .with_columns(
                ((pl.col("total_approved") / pl.col("total_claimed")) * 100)
                .round(2)
                .alias("approval_rate_pct")
            )
            .sort(["claim_type", "status"])
        )

        validate_dataframe(result, ["total_claims", "total_claimed"])
        log_dataframe_info(result, "Gold claims summary output")

        aggregate_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        result = result.with_columns(pl.lit(aggregate_time).alias("aggregated_at"))

        validate_dataframe(result, ["aggregated_at"])

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = write_parquet(
            result, "gold", f"gold_claims_summary_{timestamp}.parquet"
        )

        metadata = {
            "row_count": len(result),
            "columns": list(result.columns),
            "dtypes": {
                col: str(dtype) for col, dtype in zip(result.columns, result.dtypes)
            },
        }

        logger.info(f"Gold claims summary aggregation completed: {output_path}")
        return output_path, result, metadata


if __name__ == "__main__":
    result = aggregate()
    print(f"Aggregation completed: {result[0]}")
