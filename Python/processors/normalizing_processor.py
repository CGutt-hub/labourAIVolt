"""
LAV Normalizing Processor: Cleans and reshapes long-format raw API data
into a wide-format time-series suitable for downstream analysis.

Steps:
  1. Drop null values and deduplicate
  2. Pivot from long → wide (one row per country-year, one column per indicator)
  3. Sort by year

Usage:
    python normalizing_processor.py <LAV_XXX_api_raw.parquet>
Output:
    <LAV_XXX>_normalized.parquet  (wide format: country, iso3, year, <indicators…>)
"""

import sys
import os

import polars as pl

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
def log_info(msg):    print(f"[normalizing_processor] INFO: {msg}")
def log_warning(msg): print(f"[normalizing_processor] WARNING: {msg}")
def log_error(msg):   print(f"[normalizing_processor] ERROR: {msg}")


def run(input_path):
    log_info(f"Started for: {input_path}")

    df = pl.read_parquet(input_path)
    log_info(f"Loaded {len(df)} records, columns: {df.columns}")

    if len(df) == 0:
        log_warning("Empty raw dataset — writing empty normalized parquet")
        output_file = _output_name(input_path)
        pl.DataFrame().write_parquet(output_file, compression="snappy")
        return

    # Required columns check
    required = {"participant_id", "country", "iso3", "indicator", "year", "value"}
    missing = required - set(df.columns)
    if missing:
        log_error(f"Missing required columns: {missing}")
        sys.exit(1)

    # Drop nulls in key columns
    df = df.filter(
        pl.col("value").is_not_null()
        & pl.col("indicator").is_not_null()
        & pl.col("year").is_not_null()
    )
    log_info(f"After null drop: {len(df)} records")

    # Keep meta columns stable
    participant_id = df["participant_id"][0]
    country        = df["country"][0]
    iso3           = df["iso3"][0]

    # Deduplicate: same country-indicator-year → keep first
    df = df.unique(subset=["iso3", "indicator", "year"], keep="first")

    # Pivot to wide format
    df_wide = df.pivot(
        values="value",
        index=["country", "iso3", "year"],
        on="indicator",
        aggregate_function="mean",
    )

    # Re-attach participant_id (pivot drops it)
    df_wide = df_wide.with_columns(
        pl.lit(participant_id).alias("participant_id")
    )

    # Sort chronologically
    df_wide = df_wide.sort(["iso3", "year"])

    log_info(f"Wide-format shape: {df_wide.shape}")
    log_info(
        f"Year range: {df_wide['year'].min()}–{df_wide['year'].max()}"
    )
    log_info(f"Columns: {df_wide.columns}")

    output_file = _output_name(input_path)
    df_wide.write_parquet(output_file, compression="snappy")
    log_info(f"Saved → {output_file}")


def _output_name(input_path):
    base = os.path.basename(input_path)
    return base.replace("_api_raw.parquet", "_normalized.parquet")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(
            "[normalizing_processor] Usage: "
            "python normalizing_processor.py <LAV_XXX_api_raw.parquet>"
        )
        sys.exit(1)
    try:
        run(sys.argv[1])
    except Exception as exc:
        log_error(f"Fatal: {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
