"""Bronze layer - Payments data extraction."""

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
    """Extract payment data and write to bronze layer."""
    with time_operation("bronze payments extraction"):
        logger.info("Starting bronze payments extraction")

        backend = get_storage_backend()
        num_payments = 100 if backend == "adls" else 1000

        payment_methods = [
            "Credit Card",
            "Debit Card",
            "Bank Transfer",
            "Check",
            "Cash",
        ]
        payment_statuses = [
            "Completed",
            "Completed",
            "Completed",
            "Pending",
            "Failed",
            "Refunded",
        ]

        payments = []
        policy_ids = list(range(20000, 20800))

        for idx in range(num_payments):
            payment_date = fake.date_time_between(start_date="-2y", end_date="now")
            status = random.choice(payment_statuses)

            if status == "Completed":
                amount = round(random.uniform(100, 5000), 2)
            elif status == "Refunded":
                amount = round(random.uniform(-100, -500), 2) * -1
            else:
                amount = round(random.uniform(100, 5000), 2)

            payments.append(
                {
                    "payment_id": 50000 + idx,
                    "policy_id": random.choice(policy_ids),
                    "payment_number": f"PAY-{fake.bothify(text='######')}",
                    "payment_date": payment_date,
                    "amount": amount,
                    "payment_method": random.choice(payment_methods),
                    "status": status,
                    "transaction_id": fake.bothify(text="TXN-???????????????"),
                    "reference_number": fake.bothify(text="REF-######"),
                }
            )

        df = pl.DataFrame(payments)
        fetch_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        df = df.with_columns(pl.lit(fetch_time).alias("fetched_at"))

        log_dataframe_info(df, "Bronze payments output")
        validate_dataframe(
            df, ["payment_id", "policy_id", "amount", "payment_date", "status"]
        )

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = write_parquet(
            df, "bronze", f"bronze_payments_{timestamp}.parquet"
        )

        metadata = {
            "row_count": len(df),
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
            "storage_backend": backend,
        }

        logger.info(f"Bronze payments extraction completed: {output_path}")
        return output_path, df, metadata


if __name__ == "__main__":
    result = extract()
    print(f"Extraction completed: {result[0]}")
