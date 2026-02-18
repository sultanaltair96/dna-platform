"""Silver customer-policies join asset."""

from dagster import AutomationCondition, asset

from src.core.silver_customer_policies import transform
from src.orchestration.utils import create_output_with_metadata


@asset(
    group_name="silver",
    description="Join customers with policies data",
    compute_kind="polars",
    automation_condition=AutomationCondition.eager(),
    deps=["run_silver_clean_customers", "run_silver_clean_policies"],
)
def run_silver_customer_policies(run_silver_clean_customers, run_silver_clean_policies):
    """Run silver customer-policies join."""
    silver_path, df, metadata = transform()
    return create_output_with_metadata(silver_path, df, metadata)
