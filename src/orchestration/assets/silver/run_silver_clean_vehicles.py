"""Silver clean vehicles asset."""

from dagster import AutomationCondition, asset

from src.core.silver_clean_vehicles import transform
from src.orchestration.utils import create_output_with_metadata


@asset(
    group_name="silver",
    description="Clean and validate vehicle data",
    compute_kind="polars",
    automation_condition=AutomationCondition.eager(),
    deps=["run_bronze_vehicles"],
)
def run_silver_clean_vehicles(run_bronze_vehicles):
    """Run silver vehicles transformation."""
    silver_path, df, metadata = transform()
    return create_output_with_metadata(silver_path, df, metadata)
