"""
LAV Trend Analyzer: Fits linear time-series trends to every available
indicator for a single country.

For each indicator column the script computes:
  - slope (units/year)
  - standard error of the slope
  - p-value (two-tailed t-test)
  - R²
  - first / last / mean value
  - total change as a percentage of the first value

Usage:
    python trend_analyzer.py <LAV_XXX_normalized.parquet>
Output:
    <LAV_XXX>_trends.parquet
"""

import sys
import os

import polars as pl
import numpy as np
from scipy import stats

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
def log_info(msg):    print(f"[trend_analyzer] INFO: {msg}")
def log_warning(msg): print(f"[trend_analyzer] WARNING: {msg}")
def log_error(msg):   print(f"[trend_analyzer] ERROR: {msg}")

# ---------------------------------------------------------------------------
# Indicators to include in trend analysis (subset of wide-format columns)
# ---------------------------------------------------------------------------
TREND_INDICATORS = [
    "employment_agriculture_pct",
    "employment_industry_pct",
    "employment_services_pct",
    "unemployment_rate",
    "youth_unemployment_rate",
    "employment_to_pop_ratio",
    "wage_salary_workers_pct",
    "internet_users_pct",
    "gdp_per_capita_usd",
    "gdp_growth_annual_pct",
    "high_tech_exports_pct_mfg",
    "ict_goods_exports_pct",
]


def run(input_path):
    log_info(f"Started for: {input_path}")

    df = pl.read_parquet(input_path)
    log_info(f"Loaded wide-format data: {df.shape}")

    if len(df) == 0:
        log_warning("Empty input — writing empty trends parquet")
        pl.DataFrame().write_parquet(_output_name(input_path), compression="snappy")
        return

    country        = df["country"][0]
    iso3           = df["iso3"][0]
    participant_id = df["participant_id"][0]
    log_info(f"Analyzing trends: {country} ({iso3})")

    records = []

    for indicator in TREND_INDICATORS:
        if indicator not in df.columns:
            continue

        data = (
            df.select(["year", indicator])
            .filter(pl.col(indicator).is_not_null())
            .sort("year")
        )
        if len(data) < 3:
            log_warning(f"Too few observations for '{indicator}' — skipping")
            continue

        years  = data["year"].to_numpy().astype(float)
        values = data[indicator].to_numpy().astype(float)

        slope, intercept, r, p, se = stats.linregress(years, values)

        total_change_pct = (
            (values[-1] - values[0]) / (abs(values[0]) + 1e-9) * 100
        )

        records.append({
            "participant_id":     participant_id,
            "country":            country,
            "iso3":               iso3,
            "indicator":          indicator,
            "trend_slope":        round(float(slope), 6),
            "trend_slope_se":     round(float(se), 6),
            "trend_p_value":      round(float(p), 5),
            "trend_r_squared":    round(float(r ** 2), 4),
            "trend_significant":  bool(p < 0.05),
            "value_first":        round(float(values[0]), 4),
            "value_last":         round(float(values[-1]), 4),
            "value_mean":         round(float(np.mean(values)), 4),
            "total_change_pct":   round(float(total_change_pct), 2),
            "year_start":         int(years[0]),
            "year_end":           int(years[-1]),
            "n_observations":     len(years),
        })

    if not records:
        log_warning("No indicators found — writing empty trends parquet")
        pl.DataFrame().write_parquet(_output_name(input_path), compression="snappy")
        return

    result_df = pl.DataFrame(records)

    output_file = _output_name(input_path)
    result_df.write_parquet(output_file, compression="snappy")
    log_info(f"Saved trends → {output_file}  ({len(records)} indicators)")

    # Summary: top-5 by absolute slope
    sig_count = sum(1 for r in records if r["trend_significant"])
    log_info(f"Significant trends (p<0.05): {sig_count}/{len(records)}")
    for row in sorted(records, key=lambda x: abs(x["trend_slope"]), reverse=True)[:5]:
        sig = " **" if row["trend_significant"] else ""
        log_info(
            f"  {row['indicator']:35s}: slope={row['trend_slope']:+.4f}/yr  "
            f"p={row['trend_p_value']:.3f}{sig}"
        )


def _output_name(input_path):
    base = os.path.basename(input_path)
    return base.replace("_normalized.parquet", "_trends.parquet")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(
            "[trend_analyzer] Usage: "
            "python trend_analyzer.py <LAV_XXX_normalized.parquet>"
        )
        sys.exit(1)
    try:
        run(sys.argv[1])
    except Exception as exc:
        log_error(f"Fatal: {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
