"""Bronze layer - Vehicles data extraction."""

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
    """Extract vehicle data and write to bronze layer."""
    with time_operation("bronze vehicles extraction"):
        logger.info("Starting bronze vehicles extraction")

        backend = get_storage_backend()
        num_vehicles = 40 if backend == "adls" else 400

        makes_models = [
            ("Toyota", "Camry"),
            ("Toyota", "Corolla"),
            ("Toyota", "RAV4"),
            ("Honda", "Civic"),
            ("Honda", "Accord"),
            ("Honda", "CR-V"),
            ("Ford", "F-150"),
            ("Ford", "Mustang"),
            ("Ford", "Explorer"),
            ("Chevrolet", "Silverado"),
            ("Chevrolet", "Malibu"),
            ("Chevrolet", "Equinox"),
            ("BMW", "3 Series"),
            ("BMW", "5 Series"),
            ("BMW", "X5"),
            ("Mercedes", "C-Class"),
            ("Mercedes", "E-Class"),
            ("Mercedes", "GLE"),
            ("Tesla", "Model 3"),
            ("Tesla", "Model Y"),
        ]
        colors = ["Black", "White", "Silver", "Gray", "Red", "Blue", "Navy", "Green"]
        vehicle_types = ["Sedan", "SUV", "Truck", "Van", "Sports Car", "Luxury"]

        vehicles = []
        policy_ids = list(range(20000, 20800))

        for idx in range(num_vehicles):
            make, model = random.choice(makes_models)
            year = random.randint(2015, 2024)

            vehicles.append(
                {
                    "vehicle_id": 40000 + idx,
                    "policy_id": random.choice(policy_ids),
                    "vin": fake.bothify(text="?????????????????").upper(),
                    "make": make,
                    "model": model,
                    "year": year,
                    "vehicle_type": random.choice(vehicle_types),
                    "color": random.choice(colors),
                    "mileage": random.randint(5000, 150000),
                    "engine_type": random.choice(
                        ["Gasoline", "Diesel", "Electric", "Hybrid"]
                    ),
                    "registration_state": random.choice(
                        ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA"]
                    ),
                    "registration_expiry": fake.date_between(
                        start_date="now", end_date="+2y"
                    ),
                }
            )

        df = pl.DataFrame(vehicles)
        fetch_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        df = df.with_columns(pl.lit(fetch_time).alias("fetched_at"))

        log_dataframe_info(df, "Bronze vehicles output")
        validate_dataframe(
            df, ["vehicle_id", "policy_id", "vin", "make", "model", "year"]
        )

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = write_parquet(
            df, "bronze", f"bronze_vehicles_{timestamp}.parquet"
        )

        metadata = {
            "row_count": len(df),
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
            "storage_backend": backend,
        }

        logger.info(f"Bronze vehicles extraction completed: {output_path}")
        return output_path, df, metadata


if __name__ == "__main__":
    result = extract()
    print(f"Extraction completed: {result[0]}")
