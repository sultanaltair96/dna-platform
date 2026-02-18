# dna-platform - Agent Guide

> Quick reference for AI agents working with Polster projects. Use this guide when you need to add, remove, or debug assets without CLI access.

---

## Project Overview

Polster is a data orchestration framework that generates production-ready Dagster projects using the **Medallion Architecture** pattern:

- **Bronze**: Raw data ingestion (APIs, databases, files)
- **Silver**: Data cleaning and validation
- **Gold**: Business metrics and aggregations

**Key Rules**:
- Silver can only depend on Bronze
- Gold can only depend on Silver
- All core functions return `tuple[str, pl.DataFrame, dict]` (path, data, metadata)
- All asset wrappers must use `create_output_with_metadata()`

---

## Run the Pipeline

```bash
# Activate virtual environment
source .venv/bin/activate  # Linux/macOS

# Run with UI (recommended for development)
python run_polster.py --ui
# Open http://localhost:3000 in browser

# Run pipeline only (materialize all assets)
python run_polster.py

# Run with UI AND materialize first
python run_polster.py --ui --materialize

# Run specific asset(s)
python run_polster.py --asset bronze_users
python run_polster.py --asset bronze_orders --asset silver_clean_orders

# Show pipeline status
python run_polster.py --status
```

---

## Project Structure

```
dna-platform/
├── src/
│   ├── core/                          # Your data logic
│   │   ├── bronze_*.py               # Raw data extraction
│   │   ├── silver_*.py               # Data cleaning/transformation
│   │   ├── gold_*.py                 # Aggregations
│   │   ├── storage.py                # Storage abstraction (local/ADLS)
│   │   ├── paths.py                  # Path utilities
│   │   └── utils.py                  # Timing/validation utilities
│   └── orchestration/
│       ├── definitions.py             # Dagster asset definitions
│       ├── utils.py                   # Helper functions (create_output_with_metadata)
│       └── assets/
│           ├── bronze/                # Bronze asset wrappers
│           │   ├── __init__.py       # Asset registrations
│           │   └── run_bronze_*.py   # Individual assets
│           ├── silver/               # Silver asset wrappers
│           └── gold/                 # Gold asset wrappers
├── data/                              # Local data storage (bronze/silver/gold dirs)
├── run_polster.py                    # Main runner script
├── pyproject.toml                   # Python dependencies
├── .env                            # Environment variables
└── AGENTS.md                       # This guide
```

---

## Add a New Bronze Asset

Bronze assets extract raw data from any source. They take no arguments and return a tuple.

### Step 1: Create Core Logic

Create `src/core/bronze_your_name.py`:

```python
import polars as pl
from datetime import datetime, timezone

from core.storage import write_parquet
from core.utils import time_operation, validate_dataframe, log_dataframe_info


def extract() -> tuple[str, pl.DataFrame, dict]:
    """Extract raw data and write to bronze layer.
    
    Returns:
        tuple[str, pl.DataFrame, dict]: (output_path, DataFrame, metadata)
    """
    with time_operation("bronze data extraction"):
        # Your data extraction logic here
        # Examples:
        # df = pl.read_csv("data/file.csv")
        # df = pl.read_json("https://api.example.com/data")
        # response = requests.get("https://api.example.com/data")
        # df = pl.json_normalize(response.json())
        
        # For now, create sample data
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        })
        
        validate_dataframe(df, ["id", "name"])
        log_dataframe_info(df, "Bronze output")
        
        # Write to storage
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = write_parquet(cleaned, "silver", f"silver_cleaned_name_{timestamp}.parquet")
        
        # Prepare metadata
        metadata = {
            "row_count": len(cleaned),
            "columns": list(cleaned.columns),
            "dtypes": {col: str(dtype) for col, dtype in zip(cleaned.columns, cleaned.dtypes)},
        }
        
        return output_path, cleaned, metadata
```

### Step 2: Create Asset Wrapper

Create `src/orchestration/assets/silver/run_silver_cleaned_name.py`:

```python
from dagster import AutomationCondition, asset

from src.core.silver_cleaned_name import transform
from src.orchestration.utils import create_output_with_metadata


@asset(
    group_name="silver",
    description="Clean and validate bronze data",
    compute_kind="polars",
    automation_condition=AutomationCondition.eager(),
    deps=["run_bronze_your_name"],
)
def run_silver_cleaned_name(run_bronze_your_name):
    """Run silver transformation."""
    silver_path, df, metadata = transform()
    return create_output_with_metadata(silver_path, df, metadata)
```

### Step 3: Register Asset

Add to `src/orchestration/assets/silver/__init__.py`:

```python
from orchestration.assets.silver.run_silver_cleaned_name import run_silver_cleaned_name
```

---

## Add a Gold Asset (Aggregations)

Gold assets aggregate silver data. Like Silver, they take **no arguments** and read internally.

### Step 1: Create Core Logic

Create `src/core/gold_metrics_name.py`:

```python
import polars as pl
from datetime import datetime, timezone

from core.storage import read_parquet_latest, write_parquet
from core.utils import time_operation, validate_dataframe, log_dataframe_info


def aggregate() -> tuple[str, pl.DataFrame, dict]:
    """Aggregate silver data and write to gold layer.
    
    Returns:
        tuple[str, pl.DataFrame, dict]: (output_path, DataFrame, metadata)
    """
    with time_operation("gold data aggregation"):
        # Read latest silver data
        df = read_parquet_latest("silver", "silver_cleaned_name_")
        
        validate_dataframe(df, ["name"])
        log_dataframe_info(df, "Silver input")
        
        # Your aggregation logic
        result = df.group_by("name").agg(
            count=pl.len()
        ).sort("count", descending=True)
        
        log_dataframe_info(result, "Gold output")
        
        # Write to storage
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = write_parquet(result, "gold", f"gold_metrics_name_{timestamp}.parquet")
        
        # Prepare metadata
        metadata = {
            "row_count": len(result),
            "columns": list(result.columns),
            "dtypes": {col: str(dtype) for col, dtype in zip(result.columns, result.dtypes)},
        }
        
        return output_path, result, metadata
```

### Step 2: Create Asset Wrapper

Create `src/orchestration/assets/gold/run_gold_metrics_name.py`:

```python
from dagster import AutomationCondition, asset

from src.core.gold_metrics_name import aggregate
from src.orchestration.utils import create_output_with_metadata


@asset(
    group_name="gold",
    description="Calculate metrics from silver data",
    compute_kind="polars",
    automation_condition=AutomationCondition.eager(),
    deps=["run_silver_cleaned_name"],
)
def run_gold_metrics_name(run_silver_cleaned_name):
    """Run gold aggregation."""
    gold_path, df, metadata = aggregate()
    return create_output_with_metadata(gold_path, df, metadata)
```

### Step 3: Register Asset

Add to `src/orchestration/assets/gold/__init__.py`:

```python
from orchestration.assets.gold.run_gold_metrics_name import run_gold_metrics_name
```

---

## File Naming Conventions

| Layer | Core File | Asset File | Asset Function |
|-------|-----------|------------|----------------|
| Bronze | `bronze_*.py` | `run_bronze_*.py` | `run_bronze_*` |
| Silver | `silver_*.py` | `run_silver_*.py` | `run_silver_*` |
| Gold | `gold_*.py` | `run_gold_*.py` | `run_gold_*` |

---

## Storage Architecture

### Writing Data

Use `write_parquet(df, layer, filename)` from `core.storage`:

```python
from core.storage import write_parquet

# Writes to data/{layer}/{filename}
# Returns the path where data was written
output_path = write_parquet(df, "bronze", "my_data.parquet")
```

### Reading Data

Use `read_parquet_latest(layer, prefix)` from `core.storage`:

```python
from core.storage import read_parquet_latest

# Reads the latest file matching {layer}/{prefix}*.parquet
df = read_parquet_latest("bronze", "bronze_orders_")
```

### Dual-Write Behavior

When `STORAGE_BACKEND=adls`:
- Full data is written to Azure Data Lake Storage
- A sample (default 50 rows) is written locally for debugging
- Configure sample size: `DUAL_WRITE_SAMPLE_SIZE=100`
- Disable local sample: `DISABLE_LOCAL_SAMPLE=true`
- Fallback on ADLS failure: `ADLS_FALLBACK_TO_LOCAL=true`

### Core Utilities

Import from `core.utils`:

```python
from core.utils import time_operation, validate_dataframe, log_dataframe_info

# Time an operation
with time_operation("my transformation"):
    # ... your code ...

# Validate required columns exist
validate_dataframe(df, ["order_id", "amount", "date"])

# Log DataFrame info
log_dataframe_info(df, "Input data")
```

---

## Remove an Asset

### Step 1: Check Dependencies

```bash
grep -r "run_bronze_your_name" src/orchestration/assets/
```

### Step 2: Delete Files

Delete both:
- `src/core/bronze_your_name.py`
- `src/orchestration/assets/bronze/run_bronze_your_name.py`

### Step 3: Remove Import

Edit `src/orchestration/assets/bronze/__init__.py` and remove:
```python
from orchestration.assets.bronze.run_bronze_your_name import run_bronze_your_name
```

### Step 4: Update Dependent Assets

If other assets depend on this one, update their `deps` or remove them.

---

## Environment Variables

Edit `.env` file:

```bash
# Storage Backend
STORAGE_BACKEND=local  # or adls

# Azure Data Lake Storage (when STORAGE_BACKEND=adls)
ADLS_ACCOUNT_NAME=your_account
ADLS_ACCOUNT_KEY=your_key
ADLS_CONTAINER=your_container
ADLS_BASE_PATH=polster/data

# Dual Writing Options
DUAL_WRITE_SAMPLE_SIZE=50        # Rows to keep locally (default: 50)
DISABLE_LOCAL_SAMPLE=false       # Skip local samples
ADLS_FALLBACK_TO_LOCAL=false     # Fallback to local on ADLS failure
```

---

## Troubleshooting

### Error: `ModuleNotFoundError: No module named 'core'`

**Cause**: Virtual environment not activated
**Fix**: `source .venv/bin/activate`

### Error: `ModuleNotFoundError: No module named 'src'`

**Cause**: Not running from project root or PYTHONPATH not set
**Fix**: Use `python run_polster.py` (not `python -m ...`)

### Error: Assets not showing in UI

**Cause**: Asset not registered in `__init__.py`
**Fix**: Add import to `src/orchestration/assets/{layer}/__init__.py`

### Error: `Dependency ... not found`

**Cause**: `deps` parameter doesn't match actual asset name
**Fix**: Use exact name, e.g., `deps=["run_bronze_your_name"]`

### Error: `Silver can only depend on Bronze`

**Cause**: Invalid layer dependency
**Fix**: Bronze has no deps, Silver deps Bronze, Gold deps Silver

### Error: ImportError in `__init__.py`

**Cause**: Importing asset that doesn't exist
**Fix**: Verify file exists before adding import

### Error: Missing `create_output_with_metadata`

**Cause**: Asset wrapper doesn't return proper Dagster Output
**Fix**: Must wrap return with `create_output_with_metadata(path, df, metadata)`

### Error: Type mismatch in asset wrapper

**Cause**: Core function returns tuple but asset expects different type
**Fix**: Always use `create_output_with_metadata(path, df, metadata)` — it handles the conversion

---

## Testing Your Code

### Test Core Functions Directly

```python
import polars as pl
from src.core.silver_my_asset import transform

result = transform()  # No arguments - reads from storage
path, df, metadata = result

assert metadata["row_count"] > 0
assert "id" in df.columns
```

### Test Asset Materialization

```bash
# Run specific asset
python run_polster.py --asset bronze_my_asset

# Or from Python
python -c "
from dagster import materialize
from orchestration.assets.bronze.run_bronze_my_asset import run_bronze_my_asset
result = materialize([run_bronze_my_asset])
"
```

---

## Best Practices

1. **Always start with Bronze**: Never skip Bronze layer, even if data seems clean

2. **Core functions are self-contained**: 
   - Bronze: No args, extract data from source
   - Silver: No args, call `read_parquet_latest("bronze", "prefix_")`
   - Gold: No args, call `read_parquet_latest("silver", "prefix_")`

3. **Always return tuple**: Core functions must return `(path, df, metadata)`

4. **Always use `create_output_with_metadata`**: In every asset wrapper

5. **Add `AutomationCondition.eager()`**: For Silver and Gold assets (auto-materialize when upstream changes)

6. **Validate in core**: Use `validate_dataframe()` to check required columns

7. **Log operations**: Use `time_operation()` and `log_dataframe_info()`

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `src/core/{layer}_{name}.py` | Your data logic (extract/transform/aggregate) |
| `src/orchestration/assets/{layer}/run_{layer}_{name}.py` | Dagster asset wrapper |
| `src/orchestration/assets/{layer}/__init__.py` | Asset registration |
| `src/orchestration/definitions.py` | Dagster definitions |
| `src/core/storage.py` | Local/ADLS read/write |
| `src/core/utils.py` | Timing and validation utilities |
| `src/orchestration/utils.py` | `create_output_with_metadata` |
| `run_polster.py` | Main entry point |

---

*Generated with Polster CLI v0.1.1*
