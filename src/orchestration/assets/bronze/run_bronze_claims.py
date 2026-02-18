"""Bronze claims asset."""

from dagster import asset

from src.core.bronze_claims import extract
from src.orchestration.utils import create_output_with_metadata


@asset(
    group_name="bronze",
    description="Extract claims data",
    compute_kind="polars",
)
def run_bronze_claims():
    """Run bronze claims extraction."""
    bronze_path, df, metadata = extract()
    return create_output_with_metadata(bronze_path, df, metadata)
