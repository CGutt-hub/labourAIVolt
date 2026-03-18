"""
LAV Volt Report Analyzer: Group-level (L2) cross-country synthesis.

Takes all per-country displacement and trend parquets and produces:
  1. Cross-country displacement ranking by sector
  2. Europe-wide displacement score summary
  3. Trend comparison across Volt chapters
  4. Digitalization vs displacement correlation
  5. Volt-focused policy-relevant metrics

Usage:
    python volt_report_analyzer.py <LAV_001_displacement.parquet> \
                                   <LAV_002_displacement.parquet> \
                                   ... \
                                   <LAV_001_trends.parquet> \
                                   <LAV_002_trends.parquet> \
                                   ...
Output:
    LAV_volt_report.parquet         (full combined table)
    LAV_displacement_summary.parquet (cross-country displacement ranking)
    LAV_trends_summary.parquet       (cross-country trend comparison)
"""

import sys
import os

import polars as pl
import numpy as np

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
def log_info(msg):    print(f"[volt_report_analyzer] INFO: {msg}")
def log_warning(msg): print(f"[volt_report_analyzer] WARNING: {msg}")
def log_error(msg):   print(f"[volt_report_analyzer] ERROR: {msg}")

# ---------------------------------------------------------------------------
# Volt policy context (sector labels & narrative)
# ---------------------------------------------------------------------------
SECTOR_LABELS = {
    "agriculture": "Agriculture & Food Production",
    "industry":    "Manufacturing & Industry",
    "services":    "Services (Finance, Retail, Admin, etc.)",
}


def _load_parquets(paths):
    """Read a list of parquet paths, skipping empty files."""
    frames = []
    for p in paths:
        try:
            df = pl.read_parquet(p)
            if len(df) > 0:
                frames.append(df)
        except Exception as exc:
            log_warning(f"Could not read {p}: {exc}")
    if not frames:
        return None
    return pl.concat(frames, how="diagonal")


def displacement_summary(df_disp):
    """Cross-country displacement ranking per sector."""
    if df_disp is None or len(df_disp) == 0:
        return pl.DataFrame()

    # Rank countries by displacement_score within each sector (higher = more displaced)
    df = df_disp.sort(["sector", "displacement_score"], descending=[False, True])

    # Add sector rank per country
    df = df.with_columns(
        pl.col("displacement_score")
        .rank(method="dense", descending=True)
        .over("sector")
        .alias("rank_in_sector")
    )

    # Europe-wide mean score per sector
    sector_stats = (
        df.group_by("sector")
        .agg([
            pl.col("displacement_score").mean().round(5).alias("eu_mean_score"),
            pl.col("displacement_score").std().round(5).alias("eu_std_score"),
            pl.col("displacement_score").max().round(5).alias("eu_max_score"),
            pl.col("country").sort().str.join(", ").alias("countries"),
            pl.len().alias("n_countries"),
        ])
        .sort("eu_mean_score", descending=True)
    )

    log_info("── Sector displacement ranking (EU average) ──")
    for row in sector_stats.iter_rows(named=True):
        log_info(
            f"  {row['sector']:12s}: EU avg score={row['eu_mean_score']:.4f}  "
            f"max={row['eu_max_score']:.4f}"
        )

    return df


def trends_summary(df_trends):
    """Cross-country trend comparison for key displacement indicators."""
    if df_trends is None or len(df_trends) == 0:
        return pl.DataFrame()

    KEY_INDICATORS = [
        "employment_industry_pct",
        "employment_services_pct",
        "unemployment_rate",
        "internet_users_pct",
        "high_tech_exports_pct_mfg",
    ]
    df_key = df_trends.filter(pl.col("indicator").is_in(KEY_INDICATORS))

    summary = (
        df_key.group_by("indicator")
        .agg([
            pl.col("trend_slope").mean().round(5).alias("eu_mean_slope"),
            pl.col("trend_slope").std().round(5).alias("eu_std_slope"),
            pl.col("total_change_pct").mean().round(2).alias("eu_mean_total_change_pct"),
            pl.col("trend_significant").sum().alias("n_significant"),
            pl.len().alias("n_countries"),
        ])
        .sort("indicator")
    )

    log_info("── Cross-country trend summary (key indicators) ──")
    for row in summary.iter_rows(named=True):
        log_info(
            f"  {row['indicator']:35s}: EU avg slope={row['eu_mean_slope']:+.4f}/yr  "
            f"sig in {row['n_significant']}/{row['n_countries']} countries"
        )

    return summary


def volt_policy_metrics(df_disp, df_trends):
    """
    Compute Volt-relevant composite policy metrics:
      - Overall AI Displacement Pressure Index (ADPI) per country
      - Digitalization Readiness Score (DRS) per country
      - Vulnerability Score = ADPI / (DRS + 1)
    """
    if df_disp is None or len(df_disp) == 0:
        return pl.DataFrame()

    # ADPI: mean displacement score across all sectors for a country
    adpi = (
        df_disp.group_by(["participant_id", "country", "iso3"])
        .agg(
            pl.col("displacement_score").mean().round(5).alias("adpi"),
            pl.col("displacement_score").max().round(5).alias("adpi_max_sector"),
            pl.col("sector")
            .sort_by(df_disp["displacement_score"])
            .last()
            .alias("most_at_risk_sector"),
        )
    )

    # DRS: internet_users_pct + high_tech_exports_pct_mfg (latest year)
    if df_trends is not None and len(df_trends) > 0:
        drs_indicators = ["internet_users_pct", "high_tech_exports_pct_mfg"]
        drs_df = (
            df_trends.filter(pl.col("indicator").is_in(drs_indicators))
            .group_by(["participant_id", "country", "iso3"])
            .agg(
                pl.col("value_last").mean().round(3).alias("drs_raw"),
            )
        )
        # Normalise DRS to 0–1 range
        drs_max = drs_df["drs_raw"].max() or 1.0
        drs_df  = drs_df.with_columns(
            (pl.col("drs_raw") / drs_max).round(4).alias("drs")
        )

        metrics = adpi.join(
            drs_df.select(["participant_id", "drs"]),
            on="participant_id",
            how="left",
        )
        metrics = metrics.with_columns(
            (pl.col("adpi") / (pl.col("drs") + 0.01)).round(5).alias("vulnerability_score")
        )
    else:
        metrics = adpi.with_columns([
            pl.lit(None).cast(pl.Float64).alias("drs"),
            pl.lit(None).cast(pl.Float64).alias("vulnerability_score"),
        ])

    metrics = metrics.sort("adpi", descending=True)

    log_info("── Volt Policy Metrics ──")
    log_info(f"  {'Country':15s} {'ADPI':>8} {'DRS':>8} {'Vulnerability':>14}  Most-at-risk sector")
    for row in metrics.iter_rows(named=True):
        drs_str = f"{row['drs']:.4f}" if row["drs"] is not None else "  n/a "
        vuln    = f"{row['vulnerability_score']:.4f}" if row["vulnerability_score"] is not None else "  n/a  "
        log_info(
            f"  {row['country']:15s} {row['adpi']:8.4f} {drs_str:>8} {vuln:>14}  "
            f"{row['most_at_risk_sector']}"
        )

    return metrics


def run(input_paths):
    log_info(f"Started with {len(input_paths)} input files")

    displacement_paths = [p for p in input_paths if "_displacement.parquet" in p]
    trend_paths        = [p for p in input_paths if "_trends.parquet" in p]

    log_info(f"Displacement files: {len(displacement_paths)}")
    log_info(f"Trend files:        {len(trend_paths)}")

    df_disp   = _load_parquets(displacement_paths)
    df_trends = _load_parquets(trend_paths)

    if df_disp is None and df_trends is None:
        log_warning("No valid input data — writing empty report parquet")
        pl.DataFrame().write_parquet("LAV_volt_report.parquet", compression="snappy")
        return

    # Run analyses
    disp_summary   = displacement_summary(df_disp)
    trends_summary_df = trends_summary(df_trends)
    policy_metrics    = volt_policy_metrics(df_disp, df_trends)

    # Write individual summary files
    if len(disp_summary) > 0:
        disp_summary.write_parquet("LAV_displacement_summary.parquet", compression="snappy")
        log_info("Saved → LAV_displacement_summary.parquet")

    if len(trends_summary_df) > 0:
        trends_summary_df.write_parquet("LAV_trends_summary.parquet", compression="snappy")
        log_info("Saved → LAV_trends_summary.parquet")

    # Write combined report
    # Use diagonal concat so each table keeps its own columns;
    # missing columns are filled with null rather than using misleading aliases.
    report_parts = []
    if df_disp is not None and len(df_disp) > 0:
        report_parts.append(
            df_disp.with_columns(pl.lit("displacement").alias("table_type"))
        )
    if len(policy_metrics) > 0:
        report_parts.append(
            policy_metrics.with_columns(pl.lit("policy_metrics").alias("table_type"))
        )

    if report_parts:
        full_report = pl.concat(report_parts, how="diagonal")
        full_report.write_parquet("LAV_volt_report.parquet", compression="snappy")
        log_info("Saved → LAV_volt_report.parquet")
    else:
        pl.DataFrame().write_parquet("LAV_volt_report.parquet", compression="snappy")

    log_info("Volt report analysis complete.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(
            "[volt_report_analyzer] Usage: "
            "python volt_report_analyzer.py <file1.parquet> [file2.parquet ...]"
        )
        sys.exit(1)
    try:
        run(sys.argv[1:])
    except Exception as exc:
        log_error(f"Fatal: {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
