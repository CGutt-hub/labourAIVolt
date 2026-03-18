"""
trend_analyzer — thin CLI wrapper for Nextflow / manual use.
Core logic lives in Python/lav_pipeline.py.

Usage:
  python trend_analyzer.py <LAV_XXX_normalized.parquet>
Output:
  <LAV_XXX>_trends.parquet
  <LAV_XXX>_unemployment_vis.parquet  (AnalysisToolbox bar chart)
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import polars as pl
from lav_pipeline import (analyze_trends, make_unemployment_vis,
                           log_info, log_error)

TAG = "trend_analyzer"

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python trend_analyzer.py <LAV_XXX_normalized.parquet>")
        sys.exit(1)
    try:
        input_path = sys.argv[1]
        log_info(f"Started for: {input_path}", TAG)
        df_wide   = pl.read_parquet(input_path)
        df_trends = analyze_trends(df_wide)
        base      = os.path.basename(input_path).replace("_normalized.parquet", "")

        df_trends.write_parquet(f"{base}_trends.parquet", compression="snappy")
        log_info(f"Saved → {base}_trends.parquet", TAG)

        vis_unemp = make_unemployment_vis(df_wide)
        if vis_unemp is not None:
            vis_unemp.write_parquet(f"{base}_unemployment_vis.parquet", compression="snappy")
            log_info(f"Saved vis → {base}_unemployment_vis.parquet", TAG)
    except Exception as exc:
        log_error(f"Fatal: {exc}", TAG)
        import traceback; traceback.print_exc()
        sys.exit(1)
