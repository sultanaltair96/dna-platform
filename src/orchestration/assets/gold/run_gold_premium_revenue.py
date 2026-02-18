"""Gold premium revenue asset."""

from dagster import AutomationCondition, asset

from src.core.gold_premium_revenue import aggregate
from src.orchestration.utils import create_output_with_metadata


@asset(
    group_name="gold",
    description="Aggregate premium revenue by policy type",
    compute_kind="polars",
    automation_condition=AutomationCondition.eager(),
    deps=["run_silver_clean_policies", "run_silver_clean_payments"],
)
def run_gold_premium_revenue(run_silver_clean_policies, run_silver_clean_payments):
    """Run gold premium revenue aggregation."""
    gold_path, df, metadata = aggregate()
    return create_output_with_metadata(gold_path, df, metadata)
