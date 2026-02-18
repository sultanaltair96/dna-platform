"""Silver assets package initialization."""

from orchestration.assets.silver.run_silver_clean_customers import (
    run_silver_clean_customers,
)
from orchestration.assets.silver.run_silver_clean_policies import (
    run_silver_clean_policies,
)
from orchestration.assets.silver.run_silver_clean_claims import run_silver_clean_claims
from orchestration.assets.silver.run_silver_clean_vehicles import (
    run_silver_clean_vehicles,
)
from orchestration.assets.silver.run_silver_clean_payments import (
    run_silver_clean_payments,
)
from orchestration.assets.silver.run_silver_clean_agents import run_silver_clean_agents
from orchestration.assets.silver.run_silver_policy_claims import (
    run_silver_policy_claims,
)
from orchestration.assets.silver.run_silver_customer_policies import (
    run_silver_customer_policies,
)

__all__ = [
    "run_silver_clean_customers",
    "run_silver_clean_policies",
    "run_silver_clean_claims",
    "run_silver_clean_vehicles",
    "run_silver_clean_payments",
    "run_silver_clean_agents",
    "run_silver_policy_claims",
    "run_silver_customer_policies",
]
