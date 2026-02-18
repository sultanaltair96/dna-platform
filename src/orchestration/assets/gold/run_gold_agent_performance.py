"""Gold agent performance asset."""

from dagster import AutomationCondition, asset

from src.core.gold_agent_performance import aggregate
from src.orchestration.utils import create_output_with_metadata


@asset(
    group_name="gold",
    description="Calculate agent performance metrics",
    compute_kind="polars",
    automation_condition=AutomationCondition.eager(),
    deps=["run_silver_clean_agents", "run_silver_clean_claims"],
)
def run_gold_agent_performance(run_silver_clean_agents, run_silver_clean_claims):
    """Run gold agent performance aggregation."""
    gold_path, df, metadata = aggregate()
    return create_output_with_metadata(gold_path, df, metadata)
