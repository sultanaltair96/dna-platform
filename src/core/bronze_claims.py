"""Bronze layer - Claims data extraction."""

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
    """Extract claims data and write to bronze layer."""
    with time_operation("bronze claims extraction"):
        logger.info("Starting bronze claims extraction")

        backend = get_storage_backend()
        num_claims = 30 if backend == "adls" else 300

        claim_types = [
            "Accident",
            "Theft",
            "Fire",
            "Water Damage",
            "Natural Disaster",
            "Liability",
            "Medical",
        ]
        statuses = [
            "Pending",
            "Pending",
            "Approved",
            "Approved",
            "Denied",
            "Under Review",
            "Settled",
        ]

        claims = []
        policy_ids = list(range(20000, 20800))

        for idx in range(num_claims):
            incident_date = fake.date_time_between(start_date="-2y", end_date="now")
            claim_type = random.choice(claim_types)

            if claim_type == "Accident":
                claimed_amount = round(random.uniform(1000, 50000), 2)
            elif claim_type == "Theft":
                claimed_amount = round(random.uniform(500, 25000), 2)
            elif claim_type == "Fire":
                claimed_amount = round(random.uniform(5000, 100000), 2)
            elif claim_type == "Natural Disaster":
                claimed_amount = round(random.uniform(10000, 200000), 2)
            else:
                claimed_amount = round(random.uniform(500, 30000), 2)

            status = random.choice(statuses)
            if status in ["Approved", "Settled"]:
                approved_amount = round(claimed_amount * random.uniform(0.5, 1.0), 2)
            elif status == "Denied":
                approved_amount = 0.0
            else:
                approved_amount = None

            claims.append(
                {
                    "claim_id": 30000 + idx,
                    "policy_id": random.choice(policy_ids),
                    "claim_number": f"CLM-{fake.bothify(text='######')}",
                    "claim_type": claim_type,
                    "incident_date": incident_date,
                    "reported_date": fake.date_time_between(
                        start_date=incident_date, end_date="+30d"
                    ),
                    "claimed_amount": claimed_amount,
                    "approved_amount": approved_amount,
                    "status": status,
                    "description": fake.sentence(nb_words=10),
                }
            )

        df = pl.DataFrame(claims)
        fetch_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        df = df.with_columns(pl.lit(fetch_time).alias("fetched_at"))

        log_dataframe_info(df, "Bronze claims output")
        validate_dataframe(
            df, ["claim_id", "policy_id", "claim_type", "claimed_amount", "status"]
        )

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = write_parquet(df, "bronze", f"bronze_claims_{timestamp}.parquet")

        metadata = {
            "row_count": len(df),
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
            "storage_backend": backend,
        }

        logger.info(f"Bronze claims extraction completed: {output_path}")
        return output_path, df, metadata


if __name__ == "__main__":
    result = extract()
    print(f"Extraction completed: {result[0]}")
