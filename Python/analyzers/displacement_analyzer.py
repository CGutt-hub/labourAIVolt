"""
displacement_analyzer — thin CLI wrapper for Nextflow / manual use.
Core logic lives in Python/lav_pipeline.py.

Usage:
  python displacement_analyzer.py <LAV_XXX_normalized.parquet>
Output:
  <LAV_XXX>_displacement.parquet
  <LAV_XXX>_sector_employment_vis.parquet   (AnalysisToolbox bar chart)
  <LAV_XXX>_displacement_score_vis.parquet  (AnalysisToolbox bar chart)
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import polars as pl
from lav_pipeline import (analyze_displacement, make_sector_employment_vis,
                           make_displacement_score_vis, log_info, log_error)

TAG = "displacement_analyzer"

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python displacement_analyzer.py <LAV_XXX_normalized.parquet>")
        sys.exit(1)
    try:
        input_path = sys.argv[1]
        log_info(f"Started for: {input_path}", TAG)
        df_wide  = pl.read_parquet(input_path)
        df_disp  = analyze_displacement(df_wide)
        base     = os.path.basename(input_path).replace("_normalized.parquet", "")

        df_disp.write_parquet(f"{base}_displacement.parquet", compression="snappy")
        log_info(f"Saved → {base}_displacement.parquet", TAG)

        # AnalysisToolbox vis.parquet (auto-picked up by IOInterface)
        vis_emp  = make_sector_employment_vis(df_wide)
        vis_disp = make_displacement_score_vis(df_disp)
        if vis_emp  is not None:
            vis_emp.write_parquet(f"{base}_sector_employment_vis.parquet", compression="snappy")
            log_info(f"Saved vis → {base}_sector_employment_vis.parquet", TAG)
        if vis_disp is not None:
            vis_disp.write_parquet(f"{base}_displacement_score_vis.parquet", compression="snappy")
            log_info(f"Saved vis → {base}_displacement_score_vis.parquet", TAG)
    except Exception as exc:
        log_error(f"Fatal: {exc}", TAG)
        import traceback; traceback.print_exc()
        sys.exit(1)
