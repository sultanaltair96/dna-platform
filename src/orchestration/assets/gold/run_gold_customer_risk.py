"""Gold customer risk asset."""

from dagster import AutomationCondition, asset

from src.core.gold_customer_risk import aggregate
from src.orchestration.utils import create_output_with_metadata


@asset(
    group_name="gold",
    description="Calculate customer risk scoring",
    compute_kind="polars",
    automation_condition=AutomationCondition.eager(),
    deps=["run_silver_customer_policies", "run_silver_policy_claims"],
)
def run_gold_customer_risk(run_silver_customer_policies, run_silver_policy_claims):
    """Run gold customer risk aggregation."""
    gold_path, df, metadata = aggregate()
    return create_output_with_metadata(gold_path, df, metadata)
