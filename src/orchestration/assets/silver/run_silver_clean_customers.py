"""Silver clean customers asset."""

from dagster import AutomationCondition, asset

from src.core.silver_clean_customers import transform
from src.orchestration.utils import create_output_with_metadata


@asset(
    group_name="silver",
    description="Clean and validate customer data",
    compute_kind="polars",
    automation_condition=AutomationCondition.eager(),
    deps=["run_bronze_customers"],
)
def run_silver_clean_customers(run_bronze_customers):
    """Run silver customers transformation."""
    silver_path, df, metadata = transform()
    return create_output_with_metadata(silver_path, df, metadata)
