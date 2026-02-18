"""Gold layer - Agent performance aggregation."""

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
    """Aggregate agent performance metrics."""
    with time_operation("gold agent performance aggregation"):
        logger.info("Starting gold agent performance aggregation")

        agents_df = read_parquet_latest("silver", "silver_clean_agents_")
        policies_df = read_parquet_latest("silver", "silver_clean_policies_")
        claims_df = read_parquet_latest("silver", "silver_clean_claims_")

        validate_dataframe(agents_df, ["agent_id", "first_name", "last_name", "region"])
        validate_dataframe(policies_df, ["agent_id", "policy_id", "premium"])
        validate_dataframe(claims_df, ["claim_id", "policy_id", "approved_amount"])

        log_dataframe_info(agents_df, "Silver agents input")
        log_dataframe_info(policies_df, "Silver policies input")
        log_dataframe_info(claims_df, "Silver claims input")

        policy_claims = policies_df.join(
            claims_df, left_on="policy_id", right_on="policy_id", how="left"
        )

        agent_metrics = (
            policy_claims.with_columns(
                pl.col("claim_id").is_not_null().alias("has_claim")
            )
            .group_by("agent_id")
            .agg(
                total_policies=pl.len(),
                policies_with_claims=pl.col("has_claim").sum(),
                total_premium=pl.col("premium").sum().round(2),
                total_claims_amount=pl.col("approved_amount").sum().round(2),
            )
        )

        result = agents_df.join(
            agent_metrics, left_on="agent_id", right_on="agent_id", how="left"
        ).fill_null(0)

        result = result.with_columns(
            (pl.col("total_premium") * pl.col("commission_rate"))
            .round(2)
            .alias("commission_earned"),
            (pl.col("policies_with_claims") / pl.col("total_policies") * 100)
            .round(2)
            .alias("claim_rate_pct"),
        ).sort("total_premium", descending=True)

        validate_dataframe(result, ["total_policies", "total_premium"])
        log_dataframe_info(result, "Gold agent performance output")

        aggregate_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        result = result.with_columns(pl.lit(aggregate_time).alias("aggregated_at"))

        validate_dataframe(result, ["aggregated_at"])

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = write_parquet(
            result, "gold", f"gold_agent_performance_{timestamp}.parquet"
        )

        metadata = {
            "row_count": len(result),
            "columns": list(result.columns),
            "dtypes": {
                col: str(dtype) for col, dtype in zip(result.columns, result.dtypes)
            },
        }

        logger.info(f"Gold agent performance aggregation completed: {output_path}")
        return output_path, result, metadata


if __name__ == "__main__":
    result = aggregate()
    print(f"Aggregation completed: {result[0]}")
