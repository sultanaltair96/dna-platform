"""Bronze layer - Policies data extraction."""

from __future__ import annotations

import logging
import random
from datetime import datetime, timezone
from typing import Tuple

import polars as pl
from faker import Faker

try:
    from .storage import get_storage_backend, write_parquet
    from .utils import log_dataframe_info, time_operation, validate_dataframe
except ImportError:
    import os
    import sys

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from core.storage import get_storage_backend, write_parquet
    from core.utils import log_dataframe_info, time_operation, validate_dataframe

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

fake = Faker()


def extract() -> Tuple[str, pl.DataFrame, dict]:
    """Extract policy data and write to bronze layer."""
    with time_operation("bronze policies extraction"):
        logger.info("Starting bronze policies extraction")

        backend = get_storage_backend()
        num_policies = 80 if backend == "adls" else 800

        policy_types = ["Auto", "Home", "Life", "Health", "Business"]
        statuses = ["Active", "Active", "Active", "Pending", "Expired", "Cancelled"]

        policies = []
        customer_ids = list(range(10000, 10500))
        agent_ids = list(range(1, 21))

        for idx in range(num_policies):
            start_date = fake.date_time_between(start_date="-3y", end_date="now")
            policy_type = random.choice(policy_types)

            if policy_type == "Auto":
                coverage = random.choice(
                    ["Liability", "Collision", "Comprehensive", "Full Coverage"]
                )
                premium = round(random.uniform(500, 3000), 2)
            elif policy_type == "Home":
                coverage = random.choice(["HO-3", "HO-5", "HO-6", "DP-3"])
                premium = round(random.uniform(800, 5000), 2)
            elif policy_type == "Life":
                coverage = random.choice(
                    ["Term 10", "Term 20", "Term 30", "Whole Life", "Universal"]
                )
                premium = round(random.uniform(200, 2000), 2)
            elif policy_type == "Health":
                coverage = random.choice(["Bronze", "Silver", "Gold", "Platinum"])
                premium = round(random.uniform(300, 1500), 2)
            else:
                coverage = random.choice(
                    ["General Liability", "Professional", "Property", "混合"]
                )
                premium = round(random.uniform(1000, 10000), 2)

            policies.append(
                {
                    "policy_id": 20000 + idx,
                    "policy_number": f"POL-{fake.bothify(text='????-######')}",
                    "customer_id": random.choice(customer_ids),
                    "agent_id": random.choice(agent_ids),
                    "policy_type": policy_type,
                    "coverage_type": coverage,
                    "premium": premium,
                    "coverage_amount": round(random.uniform(50000, 1000000), 2),
                    "deductible": random.choice([250, 500, 1000, 2500, 5000]),
                    "start_date": start_date,
                    "end_date": fake.date_time_between(
                        start_date="now", end_date="+2y"
                    ),
                    "status": random.choice(statuses),
                }
            )

        df = pl.DataFrame(policies)
        fetch_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        df = df.with_columns(pl.lit(fetch_time).alias("fetched_at"))

        log_dataframe_info(df, "Bronze policies output")
        validate_dataframe(
            df, ["policy_id", "customer_id", "policy_type", "premium", "status"]
        )

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = write_parquet(
            df, "bronze", f"bronze_policies_{timestamp}.parquet"
        )

        metadata = {
            "row_count": len(df),
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
            "storage_backend": backend,
        }

        logger.info(f"Bronze policies extraction completed: {output_path}")
        return output_path, df, metadata


if __name__ == "__main__":
    result = extract()
    print(f"Extraction completed: {result[0]}")
