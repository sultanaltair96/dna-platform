"""Configuration settings for Polster projects."""

import os

# Environment
ENV = os.getenv("ENV", "dev")

# Base path for data
BASE_PATH = os.getenv("BASE_PATH", "/src/data/dev")

# Storage backend
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "local")

# Dagster home
DAGSTER_HOME = os.getenv("DAGSTER_HOME", ".dagster")
