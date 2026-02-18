"""Bronze agents asset."""

from dagster import asset

from src.core.bronze_agents import extract
from src.orchestration.utils import create_output_with_metadata


@asset(
    group_name="bronze",
    description="Extract insurance agents data",
    compute_kind="polars",
)
def run_bronze_agents():
    """Run bronze agents extraction."""
    bronze_path, df, metadata = extract()
    return create_output_with_metadata(bronze_path, df, metadata)
