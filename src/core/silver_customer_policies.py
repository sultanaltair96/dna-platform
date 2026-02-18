"""Silver layer - Join customers with policies data."""

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
    """Join silver customers with silver policies data."""
    with time_operation("silver customer-policies join"):
        logger.info("Starting silver customer-policies join")

        customers_df = read_parquet_latest("silver", "silver_clean_customers_")
        policies_df = read_parquet_latest("silver", "silver_clean_policies_")

        validate_dataframe(customers_df, ["customer_id", "first_name", "last_name"])
        validate_dataframe(
            policies_df, ["policy_id", "customer_id", "policy_type", "premium"]
        )

        log_dataframe_info(customers_df, "Silver customers input")
        log_dataframe_info(policies_df, "Silver policies input")

        joined = customers_df.join(
            policies_df, left_on="customer_id", right_on="customer_id", how="inner"
        ).sort("customer_id", "start_date")

        log_dataframe_info(joined, "Joined silver customer-policies")

        transform_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        joined = joined.with_columns(pl.lit(transform_time).alias("transformed_at"))

        validate_dataframe(joined, ["transformed_at"])

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = write_parquet(
            joined, "silver", f"silver_customer_policies_{timestamp}.parquet"
        )

        metadata = {
            "row_count": len(joined),
            "columns": list(joined.columns),
            "dtypes": {
                col: str(dtype) for col, dtype in zip(joined.columns, joined.dtypes)
            },
        }

        logger.info(f"Silver customer-policies join completed: {output_path}")
        return output_path, joined, metadata


if __name__ == "__main__":
    result = transform()
    print(f"Join completed: {result[0]}")
