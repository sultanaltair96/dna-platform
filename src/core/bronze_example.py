"""Bronze layer example - Data extraction.

This file demonstrates how to extract data and write it to the bronze layer.
"""

from __future__ import annotations

import logging
import random
from datetime import datetime, timezone

import polars as pl
from faker import Faker

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

try:
    # Try relative imports (when run as module through Dagster)
    from .storage import get_storage_backend, write_parquet
    from .utils import log_dataframe_info, time_operation, validate_dataframe
except ImportError:
    # Fall back to absolute imports (when run directly)
    import os
    import sys

    # Add src directory to path for absolute imports
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from core.storage import get_storage_backend, write_parquet
    from core.utils import log_dataframe_info, time_operation, validate_dataframe


fake = Faker()


def extract() -> tuple[str, pl.DataFrame, dict]:
    """Extract data and write to bronze layer.

    Returns:
        tuple[str, pl.DataFrame, dict]: Tuple containing:
            - Path to the written parquet file
            - DataFrame with extracted data
            - Metadata dictionary with row count, columns, etc.

    Raises:
        RuntimeError: If data extraction or writing fails
        ValueError: If generated data is invalid
    """
    with time_operation("bronze layer data extraction"):
        logger.info("Starting bronze layer data extraction")

        try:
            # Determine sample size based on storage backend
            backend = get_storage_backend()
            num_orders = 50 if backend == "adls" else 500
            logger.info(f"Generating {num_orders} sample orders for {backend} storage")

            # Validate configuration
            customer_ids = list(range(1, 201))
            if len(customer_ids) == 0:
                raise ValueError("Customer IDs list cannot be empty")

            fetch_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            orders = []

            # Generate sample orders with validation
            for idx in range(num_orders):
                try:
                    order_date = fake.date_time_between(
                        start_date="-120d", end_date="now"
                    )
                    total_amount = round(random.uniform(25.0, 520.0), 2)

                    # Validate generated data
                    if total_amount <= 0:
                        raise ValueError(
                            f"Invalid total_amount generated: {total_amount}"
                        )
                    if order_date > datetime.now():
                        raise ValueError(f"Future order date generated: {order_date}")

                    orders.append(
                        {
                            "order_id": 10000 + idx,
                            "customer_id": random.choice(customer_ids),
                            "order_date": order_date,
                            "status": random.choice(
                                [
                                    "placed",
                                    "shipped",
                                    "delivered",
                                    "cancelled",
                                    "returned",
                                ]
                            ),
                            "total_amount": total_amount,
                        }
                    )
                except Exception as e:
                    logger.error(f"Failed to generate order {idx}: {e}")
                    raise RuntimeError(
                        f"Data generation failed at order {idx}: {e}"
                    ) from e

            if not orders:
                raise ValueError("No orders were generated")

            # Create DataFrame and validate
            df = pl.DataFrame(orders)
            df = df.with_columns(pl.lit(fetch_time).alias("fetched_at"))

            log_dataframe_info(df, "Generated bronze data")
            validate_dataframe(
                df,
                [
                    "order_id",
                    "customer_id",
                    "order_date",
                    "status",
                    "total_amount",
                    "fetched_at",
                ],
            )

            # Write to storage
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            output_path = write_parquet(
                df, "bronze", f"bronze_orders_{timestamp}.parquet"
            )

            # Prepare metadata for Dagster
            metadata = {
                "row_count": len(df),
                "columns": list(df.columns),
                "dtypes": {
                    col: str(dtype)
                    for col, dtype in zip(df.columns, df.dtypes, strict=False)
                },
                "storage_backend": backend,
            }

            logger.info(
                f"Bronze layer extraction completed successfully: {output_path}"
            )
            return output_path, df, metadata

        except Exception as e:
            logger.error(f"Bronze layer extraction failed: {e}")
            raise RuntimeError(f"Data extraction failed: {e}") from e


if __name__ == "__main__":
    try:
        result = extract()
        print(f"Extraction completed: {result}")
    except Exception as e:
        print(f"Extraction failed: {e}")
        sys.exit(1)
