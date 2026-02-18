"""Shared utilities for Dagster orchestration."""

import polars as pl
from dagster import MetadataValue, Output


def df_to_markdown_table(df: pl.DataFrame) -> str:
    """Convert Polars DataFrame to markdown table format.

    Args:
        df: Polars DataFrame to convert

    Returns:
        Markdown-formatted table string
    """
    headers = df.columns
    header_row = "| " + " | ".join(headers) + " |"
    separator_row = "| " + " | ".join(["---" for _ in headers]) + " |"

    data_rows = []
    for row in df.iter_rows():
        row_str = "| " + " | ".join([str(val) for val in row]) + " |"
        data_rows.append(row_str)

    return "\n".join([header_row, separator_row] + data_rows)


def create_output_with_metadata(
    file_path: str,
    df: pl.DataFrame,
    metadata: dict | None = None,
    sample_size: int = 20,
) -> Output:
    """Create Dagster Output with standard metadata for a parquet file.

    Uses the provided DataFrame directly instead of re-reading from storage,
    avoiding authentication issues with Azure ADLS.

    Args:
        file_path: Path to the parquet file
        df: DataFrame containing the data (already in memory)
        metadata: Optional metadata dictionary with row_count, columns, dtypes
        sample_size: Number of rows to include in preview (default 20)

    Returns:
        Output object with metadata including row count, column count,
        column list, and data preview
    """
    # Use the DataFrame directly - it's already in memory from the core function
    row_count = len(df)
    columns = list(df.columns)
    schema = dict(zip(df.columns, df.dtypes, strict=False))

    # Get sample for preview
    preview_df = df.head(sample_size)

    # Build metadata dict
    output_metadata = {
        "row_count": metadata.get("row_count", row_count) if metadata else row_count,
        "column_count": len(columns),
        "columns": MetadataValue.json(columns),
        "column_types": MetadataValue.json({k: str(v) for k, v in schema.items()}),
        "preview": MetadataValue.md(df_to_markdown_table(preview_df)),
        "file_path": file_path,
    }

    return Output(
        value=file_path,
        metadata=output_metadata,
    )
