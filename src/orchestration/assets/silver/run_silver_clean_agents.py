"""Silver clean agents asset."""

from dagster import AutomationCondition, asset

from src.core.silver_clean_agents import transform
from src.orchestration.utils import create_output_with_metadata


@asset(
    group_name="silver",
    description="Clean and validate agent data",
    compute_kind="polars",
    automation_condition=AutomationCondition.eager(),
    deps=["run_bronze_agents"],
)
def run_silver_clean_agents(run_bronze_agents):
    """Run silver agents transformation."""
    silver_path, df, metadata = transform()
    return create_output_with_metadata(silver_path, df, metadata)
