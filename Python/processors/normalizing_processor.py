"""
pivot_processor (LAV) — thin CLI wrapper for Nextflow / manual use.

Pivots the long-format World Bank API parquet into wide format
(one row per year, one column per indicator).

NOTE: This is a PIVOT operation, not statistical normalization.
      For zscore / min-max / robust normalization use the AnalysisToolbox
      normalizing_processor.py instead.
      Once AT gains a pivot_processor.py this wrapper will delegate to it.

Core logic lives in Python/lav_pipeline.normalize().

Usage:
  python normalizing_processor.py <LAV_XXX_api_raw.parquet>
Output:
  <LAV_XXX>_normalized.parquet
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import polars as pl
from lav_pipeline import normalize, log_info, log_error

TAG = "pivot_processor"

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python normalizing_processor.py <LAV_XXX_api_raw.parquet>")
        sys.exit(1)
    try:
        input_path = sys.argv[1]
        log_info(f"Pivoting: {input_path}", TAG)
        df_raw  = pl.read_parquet(input_path)
        df_wide = normalize(df_raw)
        out = os.path.basename(input_path).replace("_api_raw.parquet", "_normalized.parquet")
        df_wide.write_parquet(out, compression="snappy")
        log_info(f"Saved -> {out}", TAG)
    except Exception as exc:
        log_error(f"Fatal: {exc}", TAG)
        import traceback; traceback.print_exc()
        sys.exit(1)
