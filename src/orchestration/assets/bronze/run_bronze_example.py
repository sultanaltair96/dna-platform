"""Bronze example asset."""

from dagster import asset

from src.core.bronze_example import extract
from src.orchestration.utils import create_output_with_metadata


@asset(
    group_name="bronze",
    description="Bronze example asset - data extraction",
    compute_kind="polars",
)
def run_bronze_example():
    """Run bronze example extraction."""
    bronze_path, df, metadata = extract()
    return create_output_with_metadata(bronze_path, df, metadata)
