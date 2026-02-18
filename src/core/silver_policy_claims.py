"""Silver layer - Join policies with claims data."""

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
    """Join silver policies with silver claims data."""
    with time_operation("silver policy-claims join"):
        logger.info("Starting silver policy-claims join")

        policies_df = read_parquet_latest("silver", "silver_clean_policies_")
        claims_df = read_parquet_latest("silver", "silver_clean_claims_")

        validate_dataframe(policies_df, ["policy_id", "customer_id", "policy_type"])
        validate_dataframe(claims_df, ["claim_id", "policy_id", "claimed_amount"])

        log_dataframe_info(policies_df, "Silver policies input")
        log_dataframe_info(claims_df, "Silver claims input")

        joined = policies_df.join(
            claims_df, left_on="policy_id", right_on="policy_id", how="left"
        ).sort("policy_id", "incident_date")

        joined = joined.with_columns(
            [
                pl.when(pl.col("claim_id").is_null())
                .then(pl.lit(False))
                .otherwise(pl.lit(True))
                .alias("has_claim")
            ]
        )

        log_dataframe_info(joined, "Joined silver policy-claims")

        transform_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        joined = joined.with_columns(pl.lit(transform_time).alias("transformed_at"))

        validate_dataframe(joined, ["transformed_at"])

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = write_parquet(
            joined, "silver", f"silver_policy_claims_{timestamp}.parquet"
        )

        metadata = {
            "row_count": len(joined),
            "columns": list(joined.columns),
            "dtypes": {
                col: str(dtype) for col, dtype in zip(joined.columns, joined.dtypes)
            },
        }

        logger.info(f"Silver policy-claims join completed: {output_path}")
        return output_path, joined, metadata


if __name__ == "__main__":
    result = transform()
    print(f"Join completed: {result[0]}")
