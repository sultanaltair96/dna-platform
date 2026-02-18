# dna-platform

> **Status**: Ready for Production | **Version**: 0.1.1 | **Stack**: Python + Dagster + Polars

Welcome to your new Polster data project! This README will guide you from "what is this?" to "wow, this is awesome!" in just a few minutes.

---

## Why Polster?

If you've ever worked on a data pipeline that started simple and became a maintenance nightmare, you already know why Polster exists. We wanted to bring the same structure that `dbt` brought to SQL transformations - but for Python.

**Polster gives you:**
- One command to create a production-ready pipeline
- Enforced best practices (no more "I'll figure it out later")
- Built-in Azure Data Lake Storage integration
- Automatic CI/CD setup for GitHub, Azure DevOps, or GitLab
- A framework that grows with your project

Think of it as `dbt init` for Python data engineering. Same reliability principles, but with Python's full ecosystem at your fingertips.

> **Note**: The Polster CLI is currently available by invite only. This README shows you how the CLI works to help you understand the project structure. If you don't have CLI access, skip to the [How to Use Without CLI](#how-to-use-without-cli) section for manual workflows.

---

## Quick Start (5 Minutes to First Pipeline)

Let's get you running in under 5 minutes:

```bash
# 1. Navigate to your project
cd dna-platform

# 2. Activate the virtual environment
# Linux/macOS:
source .venv/bin/activate

# Windows PowerShell:
.venv\Scripts\Activate.ps1

# 3. Test the sample pipeline
python src/core/bronze_example.py
python src/core/silver_example.py
python src/core/gold_example.py

# 4. Launch the dashboard (optional but recommended)
python run_polster.py --ui
```

Open http://127.0.0.1:3000 in your browser to see your pipeline running with automatic scheduling and monitoring!

---

## What You'll Build

Polster projects are designed around the **Medallion Architecture** - a battle-tested pattern used by companies like Netflix, Airbnb, and Uber for data reliability. Here's what that looks like in practice:

### Example 1: Customer Analytics Pipeline

```
Bronze (Raw)       →    Silver (Cleaned)      →    Gold (Insights)
─────────────────       ──────────────────          ───────────────
customer_events.csv →  deduplicated_events   →    daily_active_users
                           ↓
                    validated_emails        →    user_cohorts
```

Your Bronze layer pulls raw data. Silver cleans and validates it. Gold creates business-ready metrics.

### Example 2: Financial Reporting System

```
Bronze                   Silver                    Gold
─────────                ──────                    ────
transactions_raw    →   validated_trans     →    revenue_by_month
                         ↓                       ↓
                    categorized_spend     →    profit_margin_kpis
```

### Example 3: IoT Data Processing

```
Bronze                Silver                 Gold
──────                ──────                 ────
sensor_readings   →   filtered_readings  →    anomaly_alerts
                         ↓
                    aggregated_hourly   →    maintenance_predictions
```

The pattern stays the same regardless of your data source. That's the power of the Medallion Architecture.

---

## Medallion Architecture Explained

Polster enforces a three-layer architecture that keeps your data reliable as it grows:

### Bronze Layer: Raw Data

The Bronze layer is your landing zone. Data arrives here exactly as it comes in - no transformations, no cleaning.

- **Purpose**: Ingest raw data as-is
- **Dependencies**: None (this is your source)
- **Typical Sources**: API responses, CSV uploads, database dumps, event streams
- **Golden Rule**: Never skip the Bronze layer and write directly to Silver

```python
# src/core/bronze_customer_events.py
def extract() -> pl.DataFrame:
    """Load raw customer events from source"""
    return pl.read_csv("data/customer_events.csv")
```

### Silver Layer: Clean & Transformed

The Silver layer is where the magic happens. This is where you clean, validate, and transform your raw data into something reliable.

- **Purpose**: Validate, clean, and transform data
- **Dependencies**: Bronze assets only
- **Typical Operations**: Deduplication, type casting, business logic, enrichment
- **Golden Rule**: Silver can ONLY depend on Bronze

```python
# src/core/silver_validated_events.py
def transform(df: pl.DataFrame) -> pl.DataFrame:
    """Clean and validate customer events"""
    return (
        df
        .drop_nulls()  # Remove rows with missing data
        .unique()       # Remove duplicates
        .with_columns([
            pl.col("timestamp").str.to_datetime("%Y-%m-%d %H:%M:%S"),
            pl.col("email").str.to_lowercase(),
        ])
    )
```

### Gold Layer: Business-Ready

The Gold layer is what your stakeholders actually use. These are the KPIs, dashboards, and reports that drive decisions.

- **Purpose**: Aggregate for business decisions
- **Dependencies**: Silver assets only
- **Typical Outputs**: Daily reports, ML features, executive dashboards
- **Golden Rule**: Gold can ONLY depend on Silver

```python
# src/core/gold_daily_metrics.py
def aggregate(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate daily engagement metrics"""
    return (
        df
        .group_by("date")
        .agg([
            pl.col("user_id").n_unique().alias("unique_users"),
            pl.col("event_type").count().alias("total_events"),
        ])
        .sort("date")
    )
```

**Why This Matters:**
- Prevents the "spaghetti data" problem
- Makes debugging easy (know exactly where errors occur)
- Enables data quality tracking at each stage

---

## CLI Commands

Polster includes a CLI tool that makes managing your project effortless. Here's how it works and what each command does:

> **Note**: The Polster CLI is available by invite only. If you don't have access, skip to the [How to Use Without CLI](#how-to-use-without-cli) section for manual workflows.

### The Idea Behind the CLI

The Polster CLI is designed to automate repetitive tasks and enforce best practices. Instead of manually creating files, updating imports, and worrying about dependency management, the CLI handles it all:

- **Scaffolding**: Creates the right files in the right places
- **Dependency Tracking**: Knows which assets depend on which
- **Safety Checks**: Warns before breaking things
- **Template Rendering**: Fills in project names and configurations

Think of it as having a knowledgeable teammate who always knows the right way to set up data pipeline components.

### polster init

Create a new Polster project with all the right structure:

```bash
polster init my-project
polster init analytics-pipeline --platform github
```

What it does:
- Creates the project directory structure
- Generates configuration files (pyproject.toml, workspace.yaml, .env)
- Sets up the virtual environment
- Optionally initializes git and creates remote repository
- Configures CI/CD pipelines for GitHub, Azure DevOps, or GitLab
- Optionally configures Azure Data Lake Storage

Options:
- `--git` / `--no-git`: Initialize git repository
- `--platform`: Choose CI/CD platform (github, azure-devops, gitlab)
- `--start-dagster`: Launch dashboard after creation
- `--dry-run`: Preview what would be created

### polster add-asset

Add a new data asset to your pipeline with proper structure:

```bash
# Interactive mode (recommended)
polster add-asset

# Command-line mode
polster add-asset --layer bronze --name customer_orders
```

What it does:
- Creates the core logic file (`src/core/{layer}_{name}.py`)
- Creates the Dagster asset wrapper (`src/orchestration/assets/{layer}/run_{layer}_{name}.py`)
- Automatically registers the asset in `__init__.py`
- Guides you through selecting dependencies for Silver/Gold assets
- Enforces layer rules (Silver can only depend on Bronze, etc.)

### polster remove-asset

Safely remove an asset from your pipeline:

```bash
# Interactive mode (recommended)
polster remove-asset

# Command-line mode
polster remove-asset --layer bronze --name old_data_source
```

What it does:
- Checks for downstream dependencies
- Warns if removing would break other assets
- Deletes both the core file and orchestration wrapper
- Cleans up imports from `__init__.py`
- Offers a dry-run mode to preview changes

### polster setup

Recreate or repair your project's virtual environment:

```bash
polster setup
polster setup --force  # Force recreation even if .venv exists
```

What it does:
- Creates a fresh `.venv` if needed
- Installs all project dependencies
- Ensures the environment matches pyproject.toml

---

## Building Your Pipeline

Let's walk through adding a complete Bronze → Silver → Gold pipeline:

### Step 1: Add a Bronze Asset

```bash
polster add-asset --layer bronze --name api_users
```

This creates:
- `src/core/bronze_api_users.py` - Your extraction logic
- `src/orchestration/assets/bronze/run_bronze_api_users.py` - Dagster asset wrapper

Edit the core file to load your data:

```python
# src/core/bronze_api_users.py
import polars as pl
import requests

def extract() -> pl.DataFrame:
    """Load user data from API"""
    response = requests.get("https://api.example.com/users")
    return pl.json_normalize(response.json())
```

### Step 2: Add a Silver Asset

```bash
polster add-asset --layer silver --name clean_users
```

When prompted, select `run_bronze_api_users` as a dependency. Edit the core file:

```python
# src/core/silver_clean_users.py
import polars as pl

def transform(df: pl.DataFrame) -> pl.DataFrame:
    """Clean and validate user data"""
    return (
        df
        .drop_nulls("email")
        .with_columns([
            pl.col("email").str.to_lowercase(),
            pl.col("created_at").str.to_datetime("%Y-%m-%d"),
        ])
    )
```

### Step 3: Add a Gold Asset

```bash
polster add-asset --layer gold --name user_metrics
```

Select `run_silver_clean_users` as a dependency:

```python
# src/core/gold_user_metrics.py
import polars as pl

def aggregate(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate user engagement metrics"""
    return (
        df
        .group_by(pl.col("created_at").dt.month())
        .agg([
            pl.col("user_id").n_unique().alias("monthly_users"),
            pl.col("email").count().alias("total_signups"),
        ])
        .sort("created_at")
    )
```

### Step 4: Run Your Pipeline

```bash
# Run everything and see results
python run_polster.py --ui
```

The UI at http://127.0.0.1:3000 shows your complete pipeline with dependency visualization, execution history, and data previews.

---

## Running & Monitoring Your Pipeline

### Development Mode

```bash
# Run pipeline and open dashboard
python run_polster.py --materialize --ui

# Run pipeline only (no UI)
python run_polster.py

# Open dashboard only
python run_polster.py --ui
```

### Production Mode

Schedule your pipeline with cron or your favorite scheduler:

```bash
# Run daily at 6 AM
0 6 * * * cd /path/to/project && source .venv/bin/activate && python run_polster.py
```

The dashboard shows:
- Asset dependencies and execution status
- Run history with timing
- Data previews and metadata
- Errors and logs

---

## Common Data Patterns

### CSV Files

```python
def extract() -> pl.DataFrame:
    return pl.read_csv("data/sales.csv")
```

### APIs

```python
import requests

def extract() -> pl.DataFrame:
    response = requests.get("https://api.example.com/orders")
    return pl.json_normalize(response.json())
```

### Databases

```python
def extract() -> pl.DataFrame:
    return pl.read_database(
        "SELECT * FROM orders WHERE created_at > NOW() - INTERVAL '7 days'",
        connection_string="postgresql://user:pass@localhost:5432/db"
    )
```

### Excel Files

```python
def extract() -> pl.DataFrame:
    return pl.read_excel("data/report.xlsx", sheet_name="Q4 Sales")
```

---

## Configuration & Storage

### Built-in Reliability

Your Polster pipeline includes:

**Error Handling:**
- User-friendly messages with troubleshooting steps
- Graceful fallback when ADLS is unavailable
- Input validation for DataFrames

**Logging:**
- Operation timing for performance monitoring
- Console output during development
- Error logging for debugging

**Storage Resilience:**
- Dual writing: full data to cloud, samples locally
- Automatic fallback to local storage on errors
- Configurable sample sizes

### Local Development

Default settings work out of the box:

```bash
# Everything runs locally in data/ directory
python run_polster.py --materialize
```

### Cloud Storage (ADLS)

Configure Azure Data Lake Storage Gen2:

```bash
# Edit your .env file
STORAGE_BACKEND=adls
ADLS_ACCOUNT_NAME=your_storage_account
ADLS_CONTAINER=your_container
ADLS_BASE_PATH=data/dna-platform
ADLS_ACCOUNT_KEY=your_key_here
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `STORAGE_BACKEND` | local or adls | local |
| `ADLS_ACCOUNT_NAME` | Azure storage account | - |
| `ADLS_ACCOUNT_KEY` | Azure access key | - |
| `ADLS_CONTAINER` | Container name | - |
| `ADLS_BASE_PATH` | Path within container | project_name/data |
| `DUAL_WRITE_SAMPLE_SIZE` | Rows for local samples | 50 |
| `DISABLE_LOCAL_SAMPLE` | Skip local sample writing | false |
| `ADLS_FALLBACK_TO_LOCAL` | Fallback on ADLS failure | false |

---

## Troubleshooting

### "ModuleNotFoundError: No module named 'core'"

Activate your virtual environment first:

```bash
source .venv/bin/activate  # Linux/macOS
.venv\Scripts\Activate.ps1  # Windows
```

### "Pipeline won't start"

Check your configuration:

```bash
# Verify workspace.yaml
cat workspace.yaml

# Test imports
source .venv/bin/activate
python -c "from src.core import bronze_example"
```

### "Assets not showing in UI"

1. Check that files are named correctly: `run_bronze_*`, `run_silver_*`, `run_gold_*`
2. Verify imports in `src/orchestration/assets/{layer}/__init__.py`
3. Restart the UI: Ctrl+C and run `python run_polster.py --ui` again

### "ADLS connection failed"

- Verify credentials in `.env` file
- Check network connectivity
- Ensure container exists in storage account

### "Dependency errors"

Remember the rules:
- Silver can ONLY depend on Bronze
- Gold can ONLY depend on Silver

If you see errors about missing dependencies, check your asset's `deps` parameter in the orchestration file.

---

## Project Structure

```
dna-platform/
├── src/
│   ├── core/                    # Your data logic
│   │   ├── bronze_*.py         # Raw data ingestion
│   │   ├── silver_*.py         # Data cleaning
│   │   ├── gold_*.py           # Business aggregations
│   │   ├── storage.py          # Storage abstraction
│   │   └── settings.py        # Configuration
│   └── orchestration/           # Dagster setup
│       ├── definitions.py      # Asset definitions
│       ├── assets/             # Asset wrappers
│       │   ├── bronze/
│       │   ├── silver/
│       │   └── gold/
│       └── utils.py            # Helper functions
├── run_polster.py              # Main runner
├── workspace.yaml              # Dagster config
├── pyproject.toml              # Dependencies
├── .env                        # Your configuration
└── README.md                   # This file
```

---

## Best Practices Tips

### 1. Start with Clean Bronze Layers

Always ingest data as-is in Bronze. Don't try to clean data at the ingestion point - you'll lose the original data if your transformation logic changes.

```python
# Good: Store everything
def extract() -> pl.DataFrame:
    return pl.read_csv("data/raw.csv")  # No transformations

# Avoid: Don't clean in Bronze
def extract() -> pl.DataFrame:
    return pl.read_csv("data/raw.csv").drop_nulls()  # Loses data!
```

### 2. Validate Early, Validate Often

Use Silver layer to enforce data quality:

```python
def transform(df: pl.DataFrame) -> pl.DataFrame:
    # Check required columns exist
    required_cols = ["order_id", "amount", "date"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Validate data types
    df = df.with_columns([
        pl.col("amount").cast(pl.Float64),
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")
    ])

    # Apply business rules
    df = df.filter(pl.col("amount") > 0)

    return df
```

This example shows the three key validation steps:
1. **Schema validation** - Ensure required columns exist
2. **Type validation** - Cast columns to correct data types
3. **Business rules** - Filter out invalid records

### 3. Keep Gold Layers Simple

Gold should just aggregate - let Silver do the heavy lifting:

```python
# Good: Simple aggregation
def aggregate(df: pl.DataFrame) -> pl.DataFrame:
    return df.group_by("date").agg(pl.col("amount").sum())

# Avoid: Complex logic in Gold
def aggregate(df: pl.DataFrame) -> pl.DataFrame:
    # This should be in Silver!
    df = df.filter(pl.col("amount") > 0)
    df = df.with_columns(pl.col("date").dt.month())
    return df.group_by("date").agg(...)
```

### 4. Name Assets Consistently

Use descriptive, consistent names:

```python
# Good
bronze_api_customers
silver_cleaned_customers
gold_customer_kpis

# Avoid
bronze_data
silver_stuff
gold_final
```

### 5. Test Your Transformations

Polars makes testing easy:

```python
import polars as pl
from src.core.silver_clean_customers import transform

def test_transform():
    df = pl.DataFrame({
        "email": ["TEST@EXAMPLE.COM", None],
        "age": [25, 30]
    })
    
    result = transform(df)
    
    assert result["email"][0] == "test@example.com"
    assert result.height == 1  # Null removed
```

---

## How to Use Without CLI

> **This section is for you if you don't have access to the Polster CLI.** The CLI makes things easier, but you can still fully use Polster projects manually.

If you don't have access to the Polster CLI (e.g., someone else created the project for you), you can still work with Polster projects manually. Here's the quick version:

### The Core Workflow

1. **Add a Bronze Asset**
   - Create: `src/core/bronze_your_data.py` with an `extract()` function
   - Create: `src/orchestration/assets/bronze/run_bronze_your_data.py` with a Dagster `@asset`
   - Register in: `src/orchestration/assets/bronze/__init__.py`

2. **Add a Silver Asset**
   - Create: `src/core/silver_your_data.py` with a `transform(df)` function
   - Create: `src/orchestration/assets/silver/run_silver_your_data.py` with `@asset` that depends on your bronze asset
   - Register in: `src/orchestration/assets/silver/__init__.py`

3. **Add a Gold Asset**
   - Same pattern, but depends on Silver

### Key Files to Know

- `src/core/` - Your data transformation logic
- `src/orchestration/assets/{layer}/` - Dagster asset wrappers
- `run_polster.py` - Runs your pipeline with the UI
- `workspace.yaml` - Dagster configuration

For detailed instructions, check the comprehensive guide in `AGENTS.md` at the project root.

---

## Next Steps

Now that your project is set up, here's what to do next:

1. **Customize Your Assets**: Edit the generated files in `src/core/` to match your data sources
2. **Add More Assets**: Use `polster add-asset` if you have CLI access, or manually create files following the patterns in this README
3. **Remove Assets**: Use `polster remove-asset` if you have CLI access (it handles all the cleanup automatically)
4. **Configure Storage**: Set up ADLS if you're using cloud storage
5. **Set Up CI/CD**: Configure your CI/CD platform for automated runs

> **No CLI Access?** Start by editing files in `src/core/` to add your data logic, then create the corresponding orchestration wrappers in `src/orchestration/assets/`. See the section above for step-by-step instructions.

---

## Need Help?

- **Documentation**: Check this README and the project root's AGENTS.md
- **Issues**: Report bugs or request features on GitHub
- **Questions**: Open a discussion on GitHub

Happy building! Your data pipeline just got a whole lot better.

---

*Generated with Polster CLI v0.1.1*
