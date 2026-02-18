"""Silver clean payments asset."""

from dagster import AutomationCondition, asset

from src.core.silver_clean_payments import transform
from src.orchestration.utils import create_output_with_metadata


@asset(
    group_name="silver",
    description="Clean and validate payment data",
    compute_kind="polars",
    automation_condition=AutomationCondition.eager(),
    deps=["run_bronze_payments"],
)
def run_silver_clean_payments(run_bronze_payments):
    """Run silver payments transformation."""
    silver_path, df, metadata = transform()
    return create_output_with_metadata(silver_path, df, metadata)
