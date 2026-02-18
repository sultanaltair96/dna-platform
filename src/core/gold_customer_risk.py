"""Gold layer - Customer risk scoring aggregation."""

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
    """Calculate customer risk scoring."""
    with time_operation("gold customer risk aggregation"):
        logger.info("Starting gold customer risk aggregation")

        customer_policies = read_parquet_latest("silver", "silver_customer_policies_")
        policy_claims = read_parquet_latest("silver", "silver_policy_claims_")

        validate_dataframe(
            customer_policies, ["customer_id", "policy_id", "premium", "credit_score"]
        )
        validate_dataframe(policy_claims, ["policy_id", "has_claim", "claimed_amount"])

        log_dataframe_info(customer_policies, "Silver customer-policies input")
        log_dataframe_info(policy_claims, "Silver policy-claims input")

        customer_claims = policy_claims.group_by("policy_id").agg(
            claim_count=pl.col("has_claim").sum(),
            total_claimed=pl.col("claimed_amount").sum().fill_null(0),
        )

        customer_risk = customer_policies.join(
            customer_claims, left_on="policy_id", right_on="policy_id", how="left"
        ).fill_null(0)

        result = customer_risk.group_by("customer_id").agg(
            total_policies=pl.len(),
            total_premium=pl.col("premium").sum().round(2),
            avg_credit_score=pl.col("credit_score").mean().round(0),
            total_claims=pl.col("claim_count").sum(),
            total_claimed_amount=pl.col("total_claimed").sum().round(2),
        )

        result = result.with_columns(
            [
                (pl.col("total_claims") / pl.col("total_policies") * 100)
                .round(2)
                .alias("claim_frequency_pct"),
                (pl.col("total_claimed_amount") / pl.col("total_premium") * 100)
                .round(2)
                .alias("loss_ratio_pct"),
            ]
        )

        result = result.with_columns(
            pl.when(pl.col("loss_ratio_pct") > 50)
            .then(pl.lit("High"))
            .when(pl.col("loss_ratio_pct") > 20)
            .then(pl.lit("Medium"))
            .otherwise(pl.lit("Low"))
            .alias("risk_category")
        ).sort("loss_ratio_pct", descending=True)

        validate_dataframe(result, ["total_policies", "total_premium", "risk_category"])
        log_dataframe_info(result, "Gold customer risk output")

        aggregate_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        result = result.with_columns(pl.lit(aggregate_time).alias("aggregated_at"))

        validate_dataframe(result, ["aggregated_at"])

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = write_parquet(
            result, "gold", f"gold_customer_risk_{timestamp}.parquet"
        )

        metadata = {
            "row_count": len(result),
            "columns": list(result.columns),
            "dtypes": {
                col: str(dtype) for col, dtype in zip(result.columns, result.dtypes)
            },
        }

        logger.info(f"Gold customer risk aggregation completed: {output_path}")
        return output_path, result, metadata


if __name__ == "__main__":
    result = aggregate()
    print(f"Aggregation completed: {result[0]}")
