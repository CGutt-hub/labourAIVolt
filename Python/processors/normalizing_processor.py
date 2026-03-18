"""
normalizing_processor — thin CLI wrapper for Nextflow / manual use.
Core logic lives in Python/lav_pipeline.py.

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

TAG = "normalizing_processor"

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: python normalizing_processor.py <LAV_XXX_api_raw.parquet>")
        sys.exit(1)
    try:
        input_path = sys.argv[1]
        log_info(f"Started for: {input_path}", TAG)
        df_raw  = pl.read_parquet(input_path)
        df_wide = normalize(df_raw)
        out = os.path.basename(input_path).replace("_api_raw.parquet", "_normalized.parquet")
        df_wide.write_parquet(out, compression="snappy")
        log_info(f"Saved → {out}", TAG)
    except Exception as exc:
        log_error(f"Fatal: {exc}", TAG)
        import traceback; traceback.print_exc()
        sys.exit(1)
