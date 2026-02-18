"""Bronze assets package initialization."""

from orchestration.assets.bronze.run_bronze_customers import run_bronze_customers
from orchestration.assets.bronze.run_bronze_policies import run_bronze_policies
from orchestration.assets.bronze.run_bronze_claims import run_bronze_claims
from orchestration.assets.bronze.run_bronze_vehicles import run_bronze_vehicles
from orchestration.assets.bronze.run_bronze_agents import run_bronze_agents
from orchestration.assets.bronze.run_bronze_payments import run_bronze_payments

__all__ = [
    "run_bronze_customers",
    "run_bronze_policies",
    "run_bronze_claims",
    "run_bronze_vehicles",
    "run_bronze_agents",
    "run_bronze_payments",
]
