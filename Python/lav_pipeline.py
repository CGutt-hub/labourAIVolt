"""
LAV Pipeline — consolidated analysis and plotting module.

All fetch, normalize, analysis, and plotting logic lives here.
Import from this module in:
  - Python/lav_run.py                          (orchestrator, direct function calls)
  - Python/readers/api_reader.py               (thin CLI wrapper for Nextflow)
  - Python/processors/normalizing_processor.py (thin CLI wrapper)
  - Python/analyzers/displacement_analyzer.py  (thin CLI wrapper)
  - Python/analyzers/trend_analyzer.py         (thin CLI wrapper)
  - Python/analyzers/volt_report_analyzer.py   (thin CLI wrapper)

Plotting outputs:
  _vis.parquet  — AnalysisToolbox interactive_plotter format; bar charts
                  showing employment share and displacement year-by-year.
                  Registered into a single HTML archive via register_vis()
                  which calls interactive_plotter.py from the AnalysisToolbox.
                  In Nextflow the IOInterface bash block auto-calls the plotter.
"""

import os
import sys
import time
from datetime import datetime

import numpy as np
import polars as pl
import requests
from scipy import stats

# ── Logging ───────────────────────────────────────────────────────────────────
def log_info(msg, tag="lav"):    print(f"[{tag}] INFO: {msg}")
def log_warning(msg, tag="lav"): print(f"[{tag}] WARNING: {msg}")
def log_error(msg, tag="lav"):   print(f"[{tag}] ERROR: {msg}")

# ── Constants ─────────────────────────────────────────────────────────────────
DEFAULT_YEAR_START = 2000
WB_BASE_URL        = "https://api.worldbank.org/v2"

VOLT_COUNTRIES = [
    {"participant_id": "LAV_001", "country": "Germany",     "iso3": "DEU", "iso2": "DE", "volt_chapter": "Volt Deutschland", "year_start": DEFAULT_YEAR_START},
    {"participant_id": "LAV_002", "country": "France",      "iso3": "FRA", "iso2": "FR", "volt_chapter": "Volt France",      "year_start": DEFAULT_YEAR_START},
    {"participant_id": "LAV_003", "country": "Netherlands", "iso3": "NLD", "iso2": "NL", "volt_chapter": "Volt Nederland",   "year_start": DEFAULT_YEAR_START},
    {"participant_id": "LAV_004", "country": "Belgium",     "iso3": "BEL", "iso2": "BE", "volt_chapter": "Volt Belgium",     "year_start": DEFAULT_YEAR_START},
    {"participant_id": "LAV_005", "country": "Italy",       "iso3": "ITA", "iso2": "IT", "volt_chapter": "Volt Italia",      "year_start": DEFAULT_YEAR_START},
    {"participant_id": "LAV_006", "country": "Spain",       "iso3": "ESP", "iso2": "ES", "volt_chapter": "Volt España",      "year_start": DEFAULT_YEAR_START},
]
_VOLT_BY_ISO2 = {c["iso2"]: c for c in VOLT_COUNTRIES}

WORLDBANK_INDICATORS = {
    "employment_industry_pct":    "SL.IND.EMPL.ZS",
    "employment_agriculture_pct": "SL.AGR.EMPL.ZS",
    "employment_services_pct":    "SL.SRV.EMPL.ZS",
    "unemployment_rate":          "SL.UEM.TOTL.ZS",
    "youth_unemployment_rate":    "SL.UEM.1524.ZS",
    "labor_force_total":          "SL.TLF.TOTL.IN",
    "employment_to_pop_ratio":    "SL.EMP.TOTL.SP.ZS",
    "internet_users_pct":         "IT.NET.USER.ZS",
    "gdp_per_capita_usd":         "NY.GDP.PCAP.CD",
    "gdp_growth_annual_pct":      "NY.GDP.MKTP.KD.ZG",
    "high_tech_exports_pct_mfg":  "TX.VAL.TECH.MF.ZS",
    "ict_goods_exports_pct":      "TX.VAL.ICTG.ZS.UN",
    "wage_salary_workers_pct":    "SL.EMP.WORK.ZS",
}

SECTOR_COLUMNS = {
    "Agriculture": "employment_agriculture_pct",
    "Industry":    "employment_industry_pct",
    "Services":    "employment_services_pct",
}

AUTOMATION_RISK = {
    "Agriculture": 0.82,
    "Industry":    0.79,
    "Services":    0.63,
}

TREND_INDICATORS = [
    "employment_agriculture_pct", "employment_industry_pct", "employment_services_pct",
    "unemployment_rate", "youth_unemployment_rate", "employment_to_pop_ratio",
    "wage_salary_workers_pct", "internet_users_pct", "gdp_per_capita_usd",
    "gdp_growth_annual_pct", "high_tech_exports_pct_mfg", "ict_goods_exports_pct",
]


# ═══════════════════════════════════════════════════════════════════════════════
# 1. FETCH
# ═══════════════════════════════════════════════════════════════════════════════

def latest_available_year(iso2, indicator_code="SL.SRV.EMPL.ZS", retries=3):
    """Query World Bank mrv=1 to get the latest published data year."""
    url = (f"{WB_BASE_URL}/country/{iso2}/indicator/{indicator_code}"
           f"?format=json&mrv=1&per_page=1")
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            payload = resp.json()
            if len(payload) >= 2 and payload[1]:
                return int(payload[1][0]["date"])
        except Exception:
            if attempt < retries - 1:
                time.sleep(1)
    return datetime.utcnow().year - 1


def resolve_year_end(iso2, year_end_override, tag="lav"):
    if year_end_override is not None:
        return year_end_override
    year = latest_available_year(iso2)
    log_info(f"Auto-detected latest year from World Bank: {year}", tag)
    return year


def _fetch_one(iso2, indicator_name, indicator_code, year_start, year_end, retries=3):
    url = (f"{WB_BASE_URL}/country/{iso2}/indicator/{indicator_code}"
           f"?format=json&date={year_start}:{year_end}&per_page=100")
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
            if len(payload) < 2 or not payload[1]:
                return []
            return [
                {"source": "WorldBank", "indicator": indicator_name,
                 "indicator_code": indicator_code,
                 "year": int(e["date"]), "value": float(e["value"])}
                for e in payload[1] if e.get("value") is not None
            ]
        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
        except requests.exceptions.RequestException:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
        except Exception as exc:
            log_error(f"Unexpected error fetching {indicator_name}: {exc}")
            return []
    return []


def fetch_country(participant_id, country_name, iso3, iso2,
                  year_start=DEFAULT_YEAR_START, year_end_override=None):
    """Fetch all WB indicators for one country. Returns long-format DataFrame."""
    tag = "api_reader"
    year_end = resolve_year_end(iso2, year_end_override, tag)
    log_info(f"Country: {country_name} ({iso2}/{iso3}), {year_start}–{year_end}", tag)

    all_records = []
    for indicator_name, indicator_code in WORLDBANK_INDICATORS.items():
        records = _fetch_one(iso2, indicator_name, indicator_code, year_start, year_end)
        log_info(f"  {indicator_name}: {len(records)} obs", tag) if records \
            else log_warning(f"  {indicator_name}: no data", tag)
        all_records.extend(records)
        time.sleep(0.25)

    if all_records:
        return (pl.DataFrame(all_records)
                .with_columns([pl.lit(country_name).alias("country"),
                                pl.lit(iso3).alias("iso3"),
                                pl.lit(participant_id).alias("participant_id")])
                .sort(["indicator", "year"]))

    log_warning("No data fetched — returning empty DataFrame", tag)
    return pl.DataFrame({
        "participant_id": pl.Series([], dtype=pl.Utf8),
        "country":        pl.Series([], dtype=pl.Utf8),
        "iso3":           pl.Series([], dtype=pl.Utf8),
        "source":         pl.Series([], dtype=pl.Utf8),
        "indicator":      pl.Series([], dtype=pl.Utf8),
        "indicator_code": pl.Series([], dtype=pl.Utf8),
        "year":           pl.Series([], dtype=pl.Int64),
        "value":          pl.Series([], dtype=pl.Float64),
    })


# ═══════════════════════════════════════════════════════════════════════════════
# 2. NORMALIZE
# ═══════════════════════════════════════════════════════════════════════════════

def normalize(df_raw):
    """Pivot long-format API data to wide time-series. Returns wide DataFrame."""
    tag = "normalizing_processor"
    if len(df_raw) == 0:
        log_warning("Empty input", tag)
        return pl.DataFrame()
    required = {"participant_id", "country", "iso3", "indicator", "year", "value"}
    missing = required - set(df_raw.columns)
    if missing:
        log_error(f"Missing columns: {missing}", tag)
        raise ValueError(f"Missing columns: {missing}")
    participant_id = df_raw["participant_id"][0]
    df = (df_raw
          .filter(pl.col("value").is_not_null()
                  & pl.col("indicator").is_not_null()
                  & pl.col("year").is_not_null())
          .unique(subset=["iso3", "indicator", "year"], keep="first"))
    df_wide = (df.pivot(values="value", index=["country", "iso3", "year"],
                        on="indicator", aggregate_function="mean")
               .with_columns(pl.lit(participant_id).alias("participant_id"))
               .sort(["iso3", "year"]))
    log_info(f"Wide shape: {df_wide.shape}, "
             f"years {df_wide['year'].min()}–{df_wide['year'].max()}", tag)
    return df_wide


# ═══════════════════════════════════════════════════════════════════════════════
# 3. DISPLACEMENT ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════

def _linear_trend(years, values):
    if len(years) < 3:
        return 0.0, 1.0, 0.0
    slope, _, r, p, _ = stats.linregress(years, values)
    return float(slope), float(p), float(r ** 2)


def analyze_displacement(df_wide):
    """Compute per-sector AI displacement scores. Returns displacement DataFrame."""
    tag = "displacement_analyzer"
    if len(df_wide) == 0:
        return pl.DataFrame()
    country        = df_wide["country"][0]
    iso3           = df_wide["iso3"][0]
    participant_id = df_wide["participant_id"][0]
    log_info(f"Analyzing: {country} ({iso3})", tag)
    records = []
    for sector, col in SECTOR_COLUMNS.items():
        if col not in df_wide.columns:
            log_warning(f"Column '{col}' missing — skipping '{sector}'", tag)
            continue
        sector_df = df_wide.select(["year", col]).filter(pl.col(col).is_not_null()).sort("year")
        if len(sector_df) < 3:
            continue
        years  = sector_df["year"].to_numpy().astype(float)
        values = sector_df[col].to_numpy().astype(float)
        slope, p_value, r_sq = _linear_trend(years, values)
        mean_val            = float(np.mean(values))
        displacement_signal = float(max(0.0, -slope / (mean_val + 1e-9)))
        automation_risk     = AUTOMATION_RISK[sector]
        records.append({
            "participant_id":               participant_id,
            "country":                      country,
            "iso3":                         iso3,
            "sector":                       sector,
            "employment_mean_pct":          round(mean_val, 3),
            "employment_latest_pct":        round(float(values[-1]), 3),
            "trend_slope_pp_per_yr":        round(slope, 4),
            "trend_p_value":                round(p_value, 4),
            "trend_significant":            bool(p_value < 0.05),
            "trend_r_squared":              round(r_sq, 4),
            "displacement_signal":          round(displacement_signal, 5),
            "automation_risk_frey_osborne": automation_risk,
            "displacement_score":           round(displacement_signal * automation_risk, 5),
            "year_start":                   int(years[0]),
            "year_end":                     int(years[-1]),
            "n_observations":               len(years),
        })
    if not records:
        return pl.DataFrame()
    result = pl.DataFrame(records)
    for row in sorted(records, key=lambda x: x["displacement_score"], reverse=True):
        sig = " **" if row["trend_significant"] else ""
        log_info(f"  {row['sector']:12s}: score={row['displacement_score']:.4f}  "
                 f"slope={row['trend_slope_pp_per_yr']:+.3f}pp/yr{sig}", tag)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 4. TREND ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════

def analyze_trends(df_wide):
    """OLS slope + p + R² per indicator. Returns trends DataFrame."""
    tag = "trend_analyzer"
    if len(df_wide) == 0:
        return pl.DataFrame()
    country        = df_wide["country"][0]
    iso3           = df_wide["iso3"][0]
    participant_id = df_wide["participant_id"][0]
    records = []
    for indicator in TREND_INDICATORS:
        if indicator not in df_wide.columns:
            continue
        data = (df_wide.select(["year", indicator])
                .filter(pl.col(indicator).is_not_null()).sort("year"))
        if len(data) < 3:
            continue
        years  = data["year"].to_numpy().astype(float)
        values = data[indicator].to_numpy().astype(float)
        slope, _, r, p, se = stats.linregress(years, values)
        records.append({
            "participant_id":    participant_id,
            "country":           country,
            "iso3":              iso3,
            "indicator":         indicator,
            "trend_slope":       round(float(slope), 6),
            "trend_slope_se":    round(float(se), 6),
            "trend_p_value":     round(float(p), 5),
            "trend_r_squared":   round(float(r ** 2), 4),
            "trend_significant": bool(p < 0.05),
            "value_first":       round(float(values[0]), 4),
            "value_last":        round(float(values[-1]), 4),
            "value_mean":        round(float(np.mean(values)), 4),
            "total_change_pct":  round((values[-1] - values[0]) / (abs(values[0]) + 1e-9) * 100, 2),
            "year_start":        int(years[0]),
            "year_end":          int(years[-1]),
            "n_observations":    len(years),
        })
    if not records:
        return pl.DataFrame()
    result    = pl.DataFrame(records)
    sig_count = sum(1 for r in records if r["trend_significant"])
    log_info(f"Significant trends (p<0.05): {sig_count}/{len(records)}", tag)
    for row in sorted(records, key=lambda x: abs(x["trend_slope"]), reverse=True)[:5]:
        log_info(f"  {row['indicator']:35s}: slope={row['trend_slope']:+.4f}/yr  "
                 f"p={row['trend_p_value']:.3f}{'  **' if row['trend_significant'] else ''}", tag)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 5. GROUP REPORT (L2)
# ═══════════════════════════════════════════════════════════════════════════════

def _concat_frames(frames):
    valid = [f for f in (frames or []) if f is not None and len(f) > 0]
    return pl.concat(valid, how="diagonal") if valid else None


def generate_volt_report(disp_dfs, trend_dfs, output_dir):
    """
    Aggregate per-country DataFrames into cross-country report.
    Writes LAV_volt_report, LAV_displacement_summary, LAV_trends_summary
    parquets to output_dir.  Returns the policy_metrics DataFrame.
    """
    tag = "volt_report"
    os.makedirs(output_dir, exist_ok=True)
    df_disp   = _concat_frames(disp_dfs)
    df_trends = _concat_frames(trend_dfs)

    if df_disp is None and df_trends is None:
        log_warning("No valid group inputs — writing empty report", tag)
        pl.DataFrame().write_parquet(
            os.path.join(output_dir, "LAV_volt_report.parquet"), compression="snappy")
        return pl.DataFrame()

    # Displacement ranking
    disp_sum = pl.DataFrame()
    if df_disp is not None and len(df_disp) > 0:
        df_ranked = (df_disp
                     .sort(["sector", "displacement_score"], descending=[False, True])
                     .with_columns(
                         pl.col("displacement_score")
                         .rank(method="dense", descending=True).over("sector")
                         .alias("rank_in_sector")))
        disp_sum = df_ranked
        for row in (df_disp.group_by("sector")
                    .agg(pl.col("displacement_score").mean().round(5).alias("eu_mean"),
                         pl.col("displacement_score").max().round(5).alias("eu_max"))
                    .sort("eu_mean", descending=True).iter_rows(named=True)):
            log_info(f"  {row['sector']:12s}: EU avg={row['eu_mean']:.4f}  "
                     f"max={row['eu_max']:.4f}", tag)

    # Trend summary
    trend_sum = pl.DataFrame()
    if df_trends is not None and len(df_trends) > 0:
        KEY = ["employment_industry_pct", "employment_services_pct", "unemployment_rate",
               "internet_users_pct", "high_tech_exports_pct_mfg"]
        trend_sum = (df_trends.filter(pl.col("indicator").is_in(KEY))
                     .group_by("indicator")
                     .agg(pl.col("trend_slope").mean().round(5).alias("eu_mean_slope"),
                          pl.col("trend_slope").std().round(5).alias("eu_std_slope"),
                          pl.col("total_change_pct").mean().round(2).alias("eu_mean_total_change_pct"),
                          pl.col("trend_significant").sum().alias("n_significant"),
                          pl.len().alias("n_countries"))
                     .sort("indicator"))

    # Policy metrics
    policy = pl.DataFrame()
    if df_disp is not None and len(df_disp) > 0:
        adpi = (df_disp.group_by(["participant_id", "country", "iso3"])
                .agg(pl.col("displacement_score").mean().round(5).alias("adpi"),
                     pl.col("displacement_score").max().round(5).alias("adpi_max_sector"),
                     pl.col("sector")
                     .sort_by(df_disp["displacement_score"]).last()
                     .alias("most_at_risk_sector")))
        if df_trends is not None and len(df_trends) > 0:
            drs_df = (df_trends
                      .filter(pl.col("indicator").is_in(
                          ["internet_users_pct", "high_tech_exports_pct_mfg"]))
                      .group_by(["participant_id", "country", "iso3"])
                      .agg(pl.col("value_last").mean().round(3).alias("drs_raw")))
            drs_max = drs_df["drs_raw"].max() or 1.0
            drs_df  = drs_df.with_columns((pl.col("drs_raw") / drs_max).round(4).alias("drs"))
            policy  = (adpi.join(drs_df.select(["participant_id", "drs"]),
                                 on="participant_id", how="left")
                       .with_columns(
                           (pl.col("adpi") / (pl.col("drs") + 0.01)).round(5)
                           .alias("vulnerability_score")))
        else:
            policy = adpi.with_columns(
                [pl.lit(None).cast(pl.Float64).alias("drs"),
                 pl.lit(None).cast(pl.Float64).alias("vulnerability_score")])
        policy = policy.sort("adpi", descending=True)
        log_info(f"  {'Country':15s} {'ADPI':>8} {'DRS':>8} {'Vulnerability':>14}", tag)
        for row in policy.iter_rows(named=True):
            log_info(f"  {row['country']:15s} {row['adpi']:8.4f} "
                     f"{row['drs'] or 0.0:8.4f} "
                     f"{row['vulnerability_score'] or 0.0:14.4f}", tag)

    # Write outputs
    if len(disp_sum) > 0:
        disp_sum.write_parquet(os.path.join(output_dir, "LAV_displacement_summary.parquet"),
                               compression="snappy")
    if len(trend_sum) > 0:
        trend_sum.write_parquet(os.path.join(output_dir, "LAV_trends_summary.parquet"),
                                compression="snappy")
    parts = []
    if df_disp is not None and len(df_disp) > 0:
        parts.append(df_disp.with_columns(pl.lit("displacement").alias("table_type")))
    if len(policy) > 0:
        parts.append(policy.with_columns(pl.lit("policy_metrics").alias("table_type")))
    (pl.concat(parts, how="diagonal") if parts else pl.DataFrame()).write_parquet(
        os.path.join(output_dir, "LAV_volt_report.parquet"), compression="snappy")
    log_info(f"Saved group report → {output_dir}", tag)
    return policy


# ═══════════════════════════════════════════════════════════════════════════════
# 6. PLOTTING — AnalysisToolbox _vis.parquet format
#    Schema: one row per plot; x_data flat List, y_data nested List[List]
#    bar  → grouped bars (year-by-year per sector)
#    Picked up automatically by IOInterface bash block in Nextflow.
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_ts(df_wide, col):
    """Return (years_list, values_list) for one wide-format column."""
    data = (df_wide.select(["year", col])
            .filter(pl.col(col).is_not_null())
            .sort("year"))
    return data["year"].to_list(), [round(v, 3) for v in data[col].to_list()]


def _make_vis(plot_type, title, x_label, y_label, labels, x_data, y_data):
    """
    Build a single-row _vis.parquet DataFrame.

    x_data : flat list  — shared x-axis (years as float, or category strings)
    y_data : list of lists — one sub-list per series
    labels : flat list of series names
    """
    if x_data and isinstance(x_data[0], str):
        x_col = pl.Series([x_data], dtype=pl.List(pl.Utf8))
    else:
        x_col = pl.Series([[float(v) for v in x_data]], dtype=pl.List(pl.Float64))
    return pl.DataFrame({
        "plot_type": pl.Series([plot_type]),
        "title":     pl.Series([title]),
        "x_label":   pl.Series([x_label]),
        "y_label":   pl.Series([y_label]),
        "labels":    pl.Series([labels], dtype=pl.List(pl.Utf8)),
        "x_data":    x_col,
        "y_data":    pl.Series([[[float(v) for v in s] for s in y_data]],
                               dtype=pl.List(pl.List(pl.Float64))),
    })


def make_sector_employment_vis(df_wide):
    """
    Grouped bar: employment share by sector, one group per year.
    Shows year-by-year displacement development directly from the raw data.
    Returns vis DataFrame, or None if no sector columns are present.
    """
    country = df_wide["country"][0]
    labels, y_data, x_years = [], [], None
    for sector, col in SECTOR_COLUMNS.items():
        if col not in df_wide.columns:
            continue
        years, values = _extract_ts(df_wide, col)
        if not years:
            continue
        if x_years is None:
            x_years = years
        labels.append(sector)
        y_data.append(values)
    if not labels:
        return None
    return _make_vis("bar",
                     f"Employment Share by Sector — {country}",
                     "Year", "Employment Share (%)",
                     labels, x_years, y_data)


def make_unemployment_vis(df_wide):
    """
    Grouped bar: unemployment rate + youth unemployment, year-by-year.
    Returns vis DataFrame, or None.
    """
    country = df_wide["country"][0]
    SERIES  = {"unemployment_rate": "Unemployment Rate (%)",
               "youth_unemployment_rate": "Youth Unemployment (%)"}
    labels, y_data, x_years = [], [], None
    for col, label in SERIES.items():
        if col not in df_wide.columns:
            continue
        years, values = _extract_ts(df_wide, col)
        if not years:
            continue
        if x_years is None:
            x_years = years
        labels.append(label)
        y_data.append(values)
    if not labels:
        return None
    return _make_vis("bar",
                     f"Unemployment Rates — {country}",
                     "Year", "Rate (%)",
                     labels, x_years, y_data)


def make_displacement_score_vis(df_disp):
    """
    Bar chart: AI displacement score per sector for one country.
    Returns vis DataFrame, or None.
    """
    if df_disp is None or len(df_disp) == 0:
        return None
    country   = df_disp["country"][0]
    df_sorted = df_disp.sort("displacement_score", descending=True)
    sectors   = df_sorted["sector"].to_list()
    scores    = [round(v, 5) for v in df_sorted["displacement_score"].to_list()]
    return _make_vis("bar",
                     f"AI Displacement Score by Sector — {country}",
                     "Sector", "Displacement Score",
                     ["Displacement Score (Frey & Osborne)"],
                     sectors, [scores])


def make_group_adpi_vis(df_disp_all):
    """
    Grouped bar: displacement score per sector, one group per country.
    Returns vis DataFrame, or None.
    """
    if df_disp_all is None or len(df_disp_all) == 0:
        return None
    countries = sorted(df_disp_all["country"].unique().to_list())
    labels, y_data = [], []
    for sector in SECTOR_COLUMNS:
        scores = []
        for country in countries:
            row = df_disp_all.filter(
                (pl.col("country") == country) & (pl.col("sector") == sector))
            scores.append(float(row["displacement_score"][0]) if len(row) > 0 else 0.0)
        labels.append(sector)
        y_data.append(scores)
    return _make_vis("bar",
                     "AI Displacement Score by Country and Sector",
                     "Country", "Displacement Score",
                     labels, countries, y_data)


def make_vulnerability_vis(policy_df):
    """
    Bar chart: vulnerability score (ADPI/DRS) per country.
    Returns vis DataFrame, or None.
    """
    if policy_df is None or len(policy_df) == 0:
        return None
    df = policy_df.sort("vulnerability_score", descending=True)
    countries = df["country"].to_list()
    vulns     = [round(v or 0.0, 4) for v in df["vulnerability_score"].to_list()]
    adpis     = [round(v or 0.0, 4) for v in df["adpi"].to_list()]
    return _make_vis("bar",
                     "Labour Displacement Vulnerability by Country",
                     "Country", "Score",
                     ["Vulnerability (ADPI/DRS)", "ADPI"],
                     countries, [vulns, adpis])


# ═══════════════════════════════════════════════════════════════════════════════
# 7. PLOTTING — call interactive_plotter.py from the AnalysisToolbox
#
#    interactive_plotter.py turns every _vis.parquet into an entry in a single
#    interactive HTML archive with collapsible tree navigation and Plotly charts.
#
#    CLI:  python interactive_plotter.py <vis.parquet> <out_dir> <prefix>
#                                        <project_name> <sidecar_dir>
#
#    The AnalysisToolbox is expected as a sibling directory of this repo:
#      ../AnalysisToolbox/Python/utils/interactive_plotter.py
#    Override with env var INTERACTIVE_PLOTTER_PATH if stored elsewhere.
# ═══════════════════════════════════════════════════════════════════════════════

def find_interactive_plotter():
    """
    Locate interactive_plotter.py from the AnalysisToolbox.
    Returns the absolute path, or None if not found.
    """
    # Env var override (useful in CI)
    env_path = os.environ.get("INTERACTIVE_PLOTTER_PATH", "")
    if env_path and os.path.exists(env_path):
        return env_path

    # Standard sibling-repo layout:  ../AnalysisToolbox/Python/utils/
    this_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    candidate = os.path.join(os.path.dirname(this_repo),
                             "AnalysisToolbox", "Python", "utils",
                             "interactive_plotter.py")
    if os.path.exists(candidate):
        return candidate

    log_warning(
        f"AnalysisToolbox not found at expected path: {candidate}\n"
        "  Clone https://github.com/CGutt-hub/AnalysisToolbox alongside this repo,\n"
        "  or set INTERACTIVE_PLOTTER_PATH env var.",
        "plotter")
    return None


def register_vis(vis_parquet, archive_root, prefix, sidecar_dir,
                 project="LAV", plotter=None):
    """
    Register one _vis.parquet file with interactive_plotter.py.

    Parameters
    ----------
    vis_parquet  : path to the _vis.parquet produced by a make_*_vis() function
    archive_root : root directory for the HTML archive
                   (archive lives at <archive_root>/.bin/LAV_results.html)
    prefix       : plot identifier used in the tree (e.g. LAV_001_sector_employment_vis)
    sidecar_dir  : directory where the parquet sidecar copy is stored
                   (e.g. LAV_results/LAV_l1/LAV_001/plots/)
    project      : project name for the HTML archive filename
    plotter      : path to interactive_plotter.py (auto-detected if None)
    """
    import subprocess
    if plotter is None:
        plotter = find_interactive_plotter()
    if plotter is None:
        log_warning(
            "AnalysisToolbox interactive_plotter.py not found — skipping HTML registration.\n"
            "  Clone https://github.com/CGutt-hub/AnalysisToolbox alongside this repo,\n"
            "  or set INTERACTIVE_PLOTTER_PATH env var.",
            "plotter")
        return
    os.makedirs(sidecar_dir, exist_ok=True)
    result = subprocess.run(
        [sys.executable, "-u", plotter,
         os.path.abspath(vis_parquet),
         os.path.abspath(archive_root),
         prefix,
         project,
         os.path.abspath(sidecar_dir)],
        check=False,
    )
    if result.returncode != 0:
        log_warning(f"interactive_plotter returned exit code {result.returncode} "
                    f"for prefix '{prefix}'", "plotter")
