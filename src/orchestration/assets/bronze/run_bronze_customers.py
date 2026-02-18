"""Bronze customers asset."""

from dagster import asset

from src.core.bronze_customers import extract
from src.orchestration.utils import create_output_with_metadata


@asset(
    group_name="bronze",
    description="Extract customer demographics data",
    compute_kind="polars",
)
def run_bronze_customers():
    """Run bronze customers extraction."""
    bronze_path, df, metadata = extract()
    return create_output_with_metadata(bronze_path, df, metadata)
