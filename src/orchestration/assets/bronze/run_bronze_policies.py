"""Bronze policies asset."""

from dagster import asset

from src.core.bronze_policies import extract
from src.orchestration.utils import create_output_with_metadata


@asset(
    group_name="bronze",
    description="Extract insurance policies data",
    compute_kind="polars",
)
def run_bronze_policies():
    """Run bronze policies extraction."""
    bronze_path, df, metadata = extract()
    return create_output_with_metadata(bronze_path, df, metadata)
