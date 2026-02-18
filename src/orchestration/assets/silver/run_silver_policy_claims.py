"""Silver policy-claims join asset."""

from dagster import AutomationCondition, asset

from src.core.silver_policy_claims import transform
from src.orchestration.utils import create_output_with_metadata


@asset(
    group_name="silver",
    description="Join policies with claims data",
    compute_kind="polars",
    automation_condition=AutomationCondition.eager(),
    deps=["run_silver_clean_policies", "run_silver_clean_claims"],
)
def run_silver_policy_claims(run_silver_clean_policies, run_silver_clean_claims):
    """Run silver policy-claims join."""
    silver_path, df, metadata = transform()
    return create_output_with_metadata(silver_path, df, metadata)
