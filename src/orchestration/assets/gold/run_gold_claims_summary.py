"""Gold claims summary asset."""

from dagster import AutomationCondition, asset

from src.core.gold_claims_summary import aggregate
from src.orchestration.utils import create_output_with_metadata


@asset(
    group_name="gold",
    description="Aggregate claims by status and type",
    compute_kind="polars",
    automation_condition=AutomationCondition.eager(),
    deps=["run_silver_clean_claims"],
)
def run_gold_claims_summary(run_silver_clean_claims):
    """Run gold claims summary aggregation."""
    gold_path, df, metadata = aggregate()
    return create_output_with_metadata(gold_path, df, metadata)
