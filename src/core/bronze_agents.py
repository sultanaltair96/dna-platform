"""Bronze layer - Agents data extraction."""

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
    """Extract agent data and write to bronze layer."""
    with time_operation("bronze agents extraction"):
        logger.info("Starting bronze agents extraction")

        backend = get_storage_backend()
        num_agents = 20 if backend == "adls" else 20

        regions = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
        specialties = ["Auto", "Home", "Life", "Commercial", "Multi-line"]

        agents = []
        for idx in range(num_agents):
            hire_date = fake.date_time_between(start_date="-10y", end_date="-1y")

            agents.append(
                {
                    "agent_id": idx + 1,
                    "first_name": fake.first_name(),
                    "last_name": fake.last_name(),
                    "email": f"{fake.first_name().lower()}.{fake.last_name().lower()}@insurance.com",
                    "phone": fake.phone_number()[:15],
                    "region": random.choice(regions),
                    "specialty": random.choice(specialties),
                    "license_number": fake.bothify(text="???-######"),
                    "commission_rate": round(random.uniform(0.05, 0.15), 4),
                    "hire_date": hire_date,
                    "status": random.choice(
                        ["Active", "Active", "Active", "On Leave", "Retired"]
                    ),
                }
            )

        df = pl.DataFrame(agents)
        fetch_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        df = df.with_columns(pl.lit(fetch_time).alias("fetched_at"))

        log_dataframe_info(df, "Bronze agents output")
        validate_dataframe(
            df, ["agent_id", "first_name", "last_name", "email", "region"]
        )

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = write_parquet(df, "bronze", f"bronze_agents_{timestamp}.parquet")

        metadata = {
            "row_count": len(df),
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
            "storage_backend": backend,
        }

        logger.info(f"Bronze agents extraction completed: {output_path}")
        return output_path, df, metadata


if __name__ == "__main__":
    result = extract()
    print(f"Extraction completed: {result[0]}")
