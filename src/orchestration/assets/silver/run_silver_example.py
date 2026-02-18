"""Silver example asset."""

from dagster import AutomationCondition, asset

from src.core.silver_example import transform
from src.orchestration.utils import create_output_with_metadata


@asset(
    group_name="silver",
    description="Silver example asset - data transformation",
    compute_kind="polars",
    automation_condition=AutomationCondition.eager(),
    deps=["run_bronze_example"],
)
def run_silver_example():
    """Run silver example transformation."""
    silver_path, df, metadata = transform()
    return create_output_with_metadata(silver_path, df, metadata)
