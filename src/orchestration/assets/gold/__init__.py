"""Gold assets package initialization."""

from orchestration.assets.gold.run_gold_claims_summary import run_gold_claims_summary
from orchestration.assets.gold.run_gold_premium_revenue import run_gold_premium_revenue
from orchestration.assets.gold.run_gold_agent_performance import (
    run_gold_agent_performance,
)
from orchestration.assets.gold.run_gold_customer_risk import run_gold_customer_risk

__all__ = [
    "run_gold_claims_summary",
    "run_gold_premium_revenue",
    "run_gold_agent_performance",
    "run_gold_customer_risk",
]
