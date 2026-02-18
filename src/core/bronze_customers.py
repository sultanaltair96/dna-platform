"""Bronze layer - Customers data extraction."""

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
    """Extract customer data and write to bronze layer."""
    with time_operation("bronze customers extraction"):
        logger.info("Starting bronze customers extraction")

        backend = get_storage_backend()
        num_customers = 50 if backend == "adls" else 500

        states = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
        occupations = [
            "Engineer",
            "Teacher",
            "Doctor",
            "Nurse",
            "Lawyer",
            "Accountant",
            "Manager",
            "Sales",
            "Retired",
            "Self-employed",
        ]

        customers = []
        for idx in range(num_customers):
            dob = fake.date_of_birth(minimum_age=18, maximum_age=75)
            join_date = fake.date_time_between(start_date="-5y", end_date="now")

            customers.append(
                {
                    "customer_id": 10000 + idx,
                    "first_name": fake.first_name(),
                    "last_name": fake.last_name(),
                    "date_of_birth": dob,
                    "email": fake.email(),
                    "phone": fake.phone_number()[:15],
                    "address": fake.street_address(),
                    "city": fake.city(),
                    "state": random.choice(states),
                    "zip_code": fake.zipcode(),
                    "occupation": random.choice(occupations),
                    "annual_income": round(random.uniform(30000, 250000), 2),
                    "credit_score": random.randint(500, 850),
                    "join_date": join_date,
                }
            )

        df = pl.DataFrame(customers)
        fetch_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        df = df.with_columns(pl.lit(fetch_time).alias("fetched_at"))

        log_dataframe_info(df, "Bronze customers output")
        validate_dataframe(
            df, ["customer_id", "first_name", "last_name", "email", "join_date"]
        )

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = write_parquet(
            df, "bronze", f"bronze_customers_{timestamp}.parquet"
        )

        metadata = {
            "row_count": len(df),
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
            "storage_backend": backend,
        }

        logger.info(f"Bronze customers extraction completed: {output_path}")
        return output_path, df, metadata


if __name__ == "__main__":
    result = extract()
    print(f"Extraction completed: {result[0]}")
