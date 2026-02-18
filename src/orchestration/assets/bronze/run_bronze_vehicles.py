"""Bronze vehicles asset."""

from dagster import asset

from src.core.bronze_vehicles import extract
from src.orchestration.utils import create_output_with_metadata


@asset(
    group_name="bronze",
    description="Extract vehicle data linked to auto policies",
    compute_kind="polars",
)
def run_bronze_vehicles():
    """Run bronze vehicles extraction."""
    bronze_path, df, metadata = extract()
    return create_output_with_metadata(bronze_path, df, metadata)
