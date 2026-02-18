"""Silver clean claims asset."""

from dagster import AutomationCondition, asset

from src.core.silver_clean_claims import transform
from src.orchestration.utils import create_output_with_metadata


@asset(
    group_name="silver",
    description="Clean and validate claims data",
    compute_kind="polars",
    automation_condition=AutomationCondition.eager(),
    deps=["run_bronze_claims"],
)
def run_silver_clean_claims(run_bronze_claims):
    """Run silver claims transformation."""
    silver_path, df, metadata = transform()
    return create_output_with_metadata(silver_path, df, metadata)
