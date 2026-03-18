"""
LAV Displacement Analyzer: Computes AI-driven labour displacement metrics
per broad employment sector for a single country.

Methodology:
  - Negative employment-share trend → displacement signal
  - Weighted by Frey & Osborne (2013) automation risk (embedded)
  - Composite displacement score = signal × automation_risk

Usage:
    python displacement_analyzer.py <LAV_XXX_normalized.parquet>
Output:
    <LAV_XXX>_displacement.parquet
"""

import sys
import os

import polars as pl
import numpy as np
from scipy import stats

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
def log_info(msg):    print(f"[displacement_analyzer] INFO: {msg}")
def log_warning(msg): print(f"[displacement_analyzer] WARNING: {msg}")
def log_error(msg):   print(f"[displacement_analyzer] ERROR: {msg}")

# ---------------------------------------------------------------------------
# Sector → World Bank column mapping
# ---------------------------------------------------------------------------
SECTOR_COLUMNS = {
    "agriculture": "employment_agriculture_pct",
    "industry":    "employment_industry_pct",
    "services":    "employment_services_pct",
}

# ---------------------------------------------------------------------------
# Frey & Osborne (2013) automation-risk proxies by broad sector
# Source: "The Future of Employment" (Oxford Martin School)
# Scale: 0.0 (very low risk) → 1.0 (very high risk)
# ---------------------------------------------------------------------------
AUTOMATION_RISK = {
    "agriculture": 0.82,   # Routine physical tasks; precision agriculture AI
    "industry":    0.79,   # Manufacturing robots, quality-control vision AI
    "services":    0.63,   # Wide range: high-risk clerical ↔ low-risk care/mgmt
}


def _linear_trend(years, values):
    """Return (slope_per_year, p_value, r_squared) from a simple OLS."""
    if len(years) < 3:
        return 0.0, 1.0, 0.0
    slope, _, r, p, _ = stats.linregress(years, values)
    return float(slope), float(p), float(r ** 2)


def run(input_path):
    log_info(f"Started for: {input_path}")

    df = pl.read_parquet(input_path)
    log_info(f"Loaded wide-format data: {df.shape}")

    if len(df) == 0:
        log_warning("Empty input — writing empty displacement parquet")
        pl.DataFrame().write_parquet(_output_name(input_path), compression="snappy")
        return

    country        = df["country"][0]
    iso3           = df["iso3"][0]
    participant_id = df["participant_id"][0]
    log_info(f"Analyzing: {country} ({iso3})")

    records = []

    for sector, col in SECTOR_COLUMNS.items():
        if col not in df.columns:
            log_warning(f"Column '{col}' not found — skipping sector '{sector}'")
            continue

        sector_df = (
            df.select(["year", col])
            .filter(pl.col(col).is_not_null())
            .sort("year")
        )
        if len(sector_df) < 3:
            log_warning(f"Too few observations for '{sector}' — skipping")
            continue

        years  = sector_df["year"].to_numpy().astype(float)
        values = sector_df[col].to_numpy().astype(float)

        slope, p_value, r_sq = _linear_trend(years, values)

        mean_val = float(np.mean(values))

        # Displacement signal: negative slope normalised by mean level.
        # A sector losing 1 pp/year from a 20 pp base has signal = 0.05;
        # a sector losing 1 pp/year from a 5 pp base has signal = 0.20.
        displacement_signal = float(max(0.0, -slope / (mean_val + 1e-9)))

        automation_risk   = AUTOMATION_RISK.get(sector, 0.5)
        displacement_score = displacement_signal * automation_risk

        records.append({
            "participant_id":            participant_id,
            "country":                   country,
            "iso3":                      iso3,
            "sector":                    sector,
            "employment_mean_pct":       round(mean_val, 3),
            "employment_latest_pct":     round(float(values[-1]), 3),
            "trend_slope_pp_per_yr":     round(slope, 4),
            "trend_p_value":             round(p_value, 4),
            "trend_significant":         bool(p_value < 0.05),
            "trend_r_squared":           round(r_sq, 4),
            "displacement_signal":       round(displacement_signal, 5),
            "automation_risk_frey_osborne": automation_risk,
            "displacement_score":        round(displacement_score, 5),
            "year_start":                int(years[0]),
            "year_end":                  int(years[-1]),
            "n_observations":            len(years),
        })

    if not records:
        log_warning("No valid sectors — writing empty displacement parquet")
        pl.DataFrame().write_parquet(_output_name(input_path), compression="snappy")
        return

    result_df = pl.DataFrame(records)

    output_file = _output_name(input_path)
    result_df.write_parquet(output_file, compression="snappy")
    log_info(f"Saved displacement analysis → {output_file}")

    # Human-readable summary
    for row in sorted(records, key=lambda x: x["displacement_score"], reverse=True):
        sig = " **" if row["trend_significant"] else ""
        log_info(
            f"  {row['sector']:12s}: score={row['displacement_score']:.4f}  "
            f"slope={row['trend_slope_pp_per_yr']:+.3f}pp/yr  "
            f"risk_F&O={row['automation_risk_frey_osborne']:.2f}{sig}"
        )


def _output_name(input_path):
    base = os.path.basename(input_path)
    return base.replace("_normalized.parquet", "_displacement.parquet")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(
            "[displacement_analyzer] Usage: "
            "python displacement_analyzer.py <LAV_XXX_normalized.parquet>"
        )
        sys.exit(1)
    try:
        run(sys.argv[1])
    except Exception as exc:
        log_error(f"Fatal: {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
