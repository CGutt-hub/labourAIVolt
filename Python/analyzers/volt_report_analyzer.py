"""
volt_report_analyzer — thin CLI wrapper for Nextflow / manual use.
Core logic lives in Python/lav_pipeline.py.

Usage:
  python volt_report_analyzer.py <LAV_001_displacement.parquet> \
                                  <LAV_002_displacement.parquet> ... \
                                  <LAV_001_trends.parquet> ...
Output (written to cwd):
  LAV_volt_report.parquet
  LAV_displacement_summary.parquet
  LAV_trends_summary.parquet
  LAV_cross_country_adpi_vis.parquet   (AnalysisToolbox bar chart)
  LAV_vulnerability_vis.parquet        (AnalysisToolbox bar chart)
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import polars as pl
from lav_pipeline import (generate_volt_report, make_group_adpi_vis,
                           make_vulnerability_vis, _concat_frames,
                           log_info, log_error)

TAG = "volt_report_analyzer"

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python volt_report_analyzer.py <file1.parquet> ...")
        sys.exit(1)
    try:
        paths       = sys.argv[1:]
        disp_paths  = [p for p in paths if "_displacement.parquet" in p]
        trend_paths = [p for p in paths if "_trends.parquet"       in p]
        log_info(f"Displacement files: {len(disp_paths)}, "
                 f"Trend files: {len(trend_paths)}", TAG)

        def load(ps):
            frames = []
            for p in ps:
                try:
                    df = pl.read_parquet(p)
                    if len(df) > 0:
                        frames.append(df)
                except Exception as e:
                    log_error(f"Could not read {p}: {e}", TAG)
            return frames

        disp_dfs  = load(disp_paths)
        trend_dfs = load(trend_paths)

        policy = generate_volt_report(disp_dfs, trend_dfs, os.getcwd())

        # AnalysisToolbox vis.parquet outputs
        df_disp_all = _concat_frames(disp_dfs)
        vis_adpi  = make_group_adpi_vis(df_disp_all)
        vis_vuln  = make_vulnerability_vis(policy)
        if vis_adpi is not None:
            vis_adpi.write_parquet("LAV_cross_country_adpi_vis.parquet", compression="snappy")
            log_info("Saved vis → LAV_cross_country_adpi_vis.parquet", TAG)
        if vis_vuln is not None:
            vis_vuln.write_parquet("LAV_vulnerability_vis.parquet", compression="snappy")
            log_info("Saved vis → LAV_vulnerability_vis.parquet", TAG)
    except Exception as exc:
        log_error(f"Fatal: {exc}", TAG)
        import traceback; traceback.print_exc()
        sys.exit(1)
