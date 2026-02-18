"""Path utilities for Polster projects."""

import os

# Dynamically find the project root (parent of src)
current_dir = os.path.dirname(__file__)
while True:
    if os.path.basename(current_dir) == "src":
        PROJECT_ROOT = os.path.dirname(current_dir)
        break
    parent = os.path.dirname(current_dir)
    if parent == current_dir:  # Reached root
        raise ValueError("Could not find 'src' folder in the path")
    current_dir = parent
PROJECT_ROOT = os.path.abspath(PROJECT_ROOT)

# Define project root and data directories
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
SILVER_DIR = os.path.join(DATA_DIR, "silver")
GOLD_DIR = os.path.join(DATA_DIR, "gold")

# Ensure data directories exist
for directory in [DATA_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
    os.makedirs(directory, exist_ok=True)
