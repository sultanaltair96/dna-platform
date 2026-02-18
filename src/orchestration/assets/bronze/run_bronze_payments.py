"""Bronze payments asset."""

from dagster import asset

from src.core.bronze_payments import extract
from src.orchestration.utils import create_output_with_metadata


@asset(
    group_name="bronze",
    description="Extract payment transactions data",
    compute_kind="polars",
)
def run_bronze_payments():
    """Run bronze payments extraction."""
    bronze_path, df, metadata = extract()
    return create_output_with_metadata(bronze_path, df, metadata)
