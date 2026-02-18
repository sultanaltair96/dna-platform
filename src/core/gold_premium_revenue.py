"""Gold layer - Premium revenue aggregation."""

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
    """Aggregate premium revenue by policy type."""
    with time_operation("gold premium revenue aggregation"):
        logger.info("Starting gold premium revenue aggregation")

        policies_df = read_parquet_latest("silver", "silver_clean_policies_")
        payments_df = read_parquet_latest("silver", "silver_clean_payments_")

        validate_dataframe(policies_df, ["policy_id", "policy_type", "premium"])
        validate_dataframe(payments_df, ["policy_id", "amount", "status"])

        log_dataframe_info(policies_df, "Silver policies input")
        log_dataframe_info(payments_df, "Silver payments input")

        completed_payments = payments_df.filter(pl.col("status") == "Completed")

        policy_payments = policies_df.join(
            completed_payments, left_on="policy_id", right_on="policy_id", how="inner"
        )

        result = (
            policy_payments.group_by("policy_type")
            .agg(
                total_policies=pl.col("policy_id").n_unique(),
                total_premium=pl.col("premium").sum().round(2),
                avg_premium=pl.col("premium").mean().round(2),
                total_collected=pl.col("amount").sum().round(2),
                avg_collection=pl.col("amount").mean().round(2),
            )
            .with_columns(
                ((pl.col("total_collected") / pl.col("total_premium")) * 100)
                .round(2)
                .alias("collection_rate_pct")
            )
            .sort("total_premium", descending=True)
        )

        validate_dataframe(result, ["total_policies", "total_premium"])
        log_dataframe_info(result, "Gold premium revenue output")

        aggregate_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        result = result.with_columns(pl.lit(aggregate_time).alias("aggregated_at"))

        validate_dataframe(result, ["aggregated_at"])

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = write_parquet(
            result, "gold", f"gold_premium_revenue_{timestamp}.parquet"
        )

        metadata = {
            "row_count": len(result),
            "columns": list(result.columns),
            "dtypes": {
                col: str(dtype) for col, dtype in zip(result.columns, result.dtypes)
            },
        }

        logger.info(f"Gold premium revenue aggregation completed: {output_path}")
        return output_path, result, metadata


if __name__ == "__main__":
    result = aggregate()
    print(f"Aggregation completed: {result[0]}")
