"""
LAV Pipeline — consolidated analysis module.

All fetch, pivot, analysis, and vis.parquet production lives here.
Import from this module in:
  - Python/lav_run.py                          (orchestrator — calls functions directly)
  - Python/readers/api_reader.py               (thin CLI wrapper for Nextflow)
  - Python/processors/normalizing_processor.py (thin CLI wrapper for the pivot step)

AnalysisToolbox modules used directly:
  - analyzers/group_analyzer.py   → cross-country sector aggregation in generate_volt_report()
  - utils/interactive_plotter.py  → HTML archive registration via register_vis()

Missing AnalysisToolbox building blocks (flagged for addition):
  - timeseries_ols_processor.py   → OLS on (x_col: year, y_cols: indicator cols) in wide parquet;
                                    would replace the linregress loops in analyze_ols().
  - pivot_processor.py            → long→wide parquet pivot; would replace normalize().

Every analysis function writes its own _vis.parquet files to work_dir so that
interactive_plotter.py can register them into the HTML archive.
"""

import contextlib
import importlib
import os
import sys
import time
from datetime import datetime
from typing import Optional

import numpy as np
import polars as pl
import requests
from scipy import stats

# ── Logging ───────────────────────────────────────────────────────────────────
def log_info(msg, tag="lav"):    print(f"[{tag}] INFO: {msg}")
def log_warning(msg, tag="lav"): print(f"[{tag}] WARNING: {msg}")
def log_error(msg, tag="lav"):   print(f"[{tag}] ERROR: {msg}")

# ── AnalysisToolbox integration ───────────────────────────────────────────────
# Modules are imported lazily so the pipeline works without the sibling repo;
# only the HTML archive registration and cross-country group vis.parquet are
# affected when the AT repo is absent.

_AT_PYTHON: Optional[str] = None


def _at_python_path() -> Optional[str]:
    """Locate the AnalysisToolbox/Python/ directory (sibling repo).

    Resolution order:
      1. AT_PYTHON_PATH env var
      2. ../AnalysisToolbox/Python/ relative to this repo root
    """
    global _AT_PYTHON
    if _AT_PYTHON is not None:
        return _AT_PYTHON
    env = os.environ.get("AT_PYTHON_PATH", "")
    if env and os.path.isdir(env):
        _AT_PYTHON = env
        return _AT_PYTHON
    this_repo  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    candidate  = os.path.join(os.path.dirname(this_repo), "AnalysisToolbox", "Python")
    if os.path.isdir(candidate):
        _AT_PYTHON = candidate
        return _AT_PYTHON
    log_warning(
        f"AnalysisToolbox not found at: {candidate}\n"
        "  Clone https://github.com/CGutt-hub/AnalysisToolbox alongside this repo,\n"
        "  or set AT_PYTHON_PATH env var.",
        "at",
    )
    return None


def _at_module(dotpath: str):
    """Import an AnalysisToolbox module by dotpath (e.g. 'analyzers.group_analyzer').
    Returns the module, or None if the AT repo is not available."""
    at_path = _at_python_path()
    if not at_path:
        return None
    if at_path not in sys.path:
        sys.path.insert(0, at_path)
    try:
        return importlib.import_module(dotpath)
    except ImportError as exc:
        log_warning(f"Cannot import AT module '{dotpath}': {exc}", "at")
        return None


@contextlib.contextmanager
def _in_dir(path: str):
    """Context manager: temporarily change cwd to *path*.
    Required because AT modules write output relative to os.getcwd()."""
    old = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old)

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
# 3. OLS ANALYSIS  (trends + displacement — same math, one function)
#
# NOTE: This implements what `timeseries_ols_processor.py` would provide once
#       added to the AnalysisToolbox.  That module should accept:
#           (x_col: year, y_cols: indicator columns, wide-format parquet)
#       and output slope / se / p_value / r_squared per column + _vis.parquet.
#
#       The displacement score = max(0, -slope/mean) × automation_risk is a
#       scalar weighting applied after the OLS step.  Once the AT module exists,
#       only the Frey & Osborne weight table and that formula stay in LAV.
# ═══════════════════════════════════════════════════════════════════════════════

def analyze_ols(df_wide, work_dir):
    """
    Time-series OLS for all TREND_INDICATORS (year as x, value as y).

    For SECTOR_COLUMNS additionally computes:
        displacement_signal = max(0, -slope / mean)
        displacement_score  = displacement_signal × automation_risk (Frey & Osborne)

    Writes to work_dir:
        {pid}_ols_vis.parquet         — bar chart: OLS slope per indicator
        {pid}_displacement_vis.parquet — bar chart: displacement score per sector

    Returns df_ols (one row per indicator; displacement columns null for non-sectors).
    """
    tag = "ols_analyzer"
    if len(df_wide) == 0:
        return pl.DataFrame()
    country        = df_wide["country"][0]
    iso3           = df_wide["iso3"][0]
    participant_id = df_wide["participant_id"][0]
    log_info(f"OLS analysis: {country} ({iso3})", tag)

    records = []
    for indicator in TREND_INDICATORS:
        if indicator not in df_wide.columns:
            continue
        data = (df_wide.select(["year", indicator])
                .filter(pl.col(indicator).is_not_null())
                .sort("year"))
        if len(data) < 3:
            continue
        years  = data["year"].to_numpy().astype(float)
        values = data[indicator].to_numpy().astype(float)
        slope, _, r, p, se = stats.linregress(years, values)

        # Displacement fields — populated only for sector indicators
        sector     = next((s for s, col in SECTOR_COLUMNS.items() if col == indicator), None)
        risk       = AUTOMATION_RISK.get(sector) if sector else None
        mean_val   = float(np.mean(values))
        signal     = float(max(0.0, -slope / (mean_val + 1e-9))) if sector else None
        disp_score = round(signal * risk, 5) if (signal is not None and risk is not None) else None

        records.append({
            "participant_id":      participant_id,
            "country":             country,
            "iso3":                iso3,
            "indicator":           indicator,
            "sector":              sector,
            "trend_slope":         round(float(slope), 6),
            "trend_slope_se":      round(float(se), 6),
            "trend_p_value":       round(float(p), 5),
            "trend_r_squared":     round(float(r ** 2), 4),
            "trend_significant":   bool(p < 0.05),
            "value_first":         round(float(values[0]), 4),
            "value_last":          round(float(values[-1]), 4),
            "value_mean":          round(mean_val, 4),
            "total_change_pct":    round(
                (values[-1] - values[0]) / (abs(values[0]) + 1e-9) * 100, 2),
            "year_start":          int(years[0]),
            "year_end":            int(years[-1]),
            "n_observations":      len(years),
            "automation_risk":     risk,
            "displacement_signal": round(signal, 5) if signal is not None else None,
            "displacement_score":  disp_score,
        })

    if not records:
        return pl.DataFrame()

    df_ols    = pl.DataFrame(records)
    sig_count = sum(1 for r in records if r["trend_significant"])
    log_info(f"OLS: {len(records)} indicators, {sig_count} significant (p<0.05)", tag)
    for row in [r for r in records if r.get("displacement_score") is not None]:
        sig = " **" if row["trend_significant"] else ""
        log_info(f"  {row['sector']:12s}: score={row['displacement_score']:.4f}  "
                 f"slope={row['trend_slope']:+.4f}/yr{sig}", tag)

    os.makedirs(work_dir, exist_ok=True)

    # vis 1 — OLS slope bar chart (all indicators)
    indicators = [r["indicator"] for r in records]
    slopes     = [r["trend_slope"] for r in records]
    sig_flags  = [r["trend_significant"] for r in records]
    _make_vis(
        "bar", f"OLS Trends (slope/yr) — {country}",
        "Indicator", "Slope (units/yr)",
        [("* " if s else "") + ind for s, ind in zip(sig_flags, indicators)],
        indicators, [slopes],
    ).write_parquet(
        os.path.join(work_dir, f"{participant_id}_ols_vis.parquet"),
        compression="snappy",
    )

    # vis 2 — displacement score bar chart (sectors only)
    sector_rows = sorted(
        [r for r in records if r.get("displacement_score") is not None],
        key=lambda x: x["displacement_score"], reverse=True,
    )
    if sector_rows:
        _make_vis(
            "bar", f"AI Displacement Score — {country}",
            "Sector", "Score (signal × automation risk)",
            ["Displacement Score"],
            [r["sector"] for r in sector_rows],
            [[r["displacement_score"] for r in sector_rows]],
        ).write_parquet(
            os.path.join(work_dir, f"{participant_id}_displacement_vis.parquet"),
            compression="snappy",
        )

    return df_ols


# ═══════════════════════════════════════════════════════════════════════════════
# 4. CROSS-COUNTRY REPORT  (group synthesis — uses AT modules directly)
# ═══════════════════════════════════════════════════════════════════════════════

def _concat_frames(frames):
    valid = [f for f in (frames or []) if f is not None and len(f) > 0]
    return pl.concat(valid, how="diagonal_relaxed") if valid else None


def generate_volt_report(all_ols_dfs, output_dir):
    """
    Cross-country synthesis — uses AnalysisToolbox modules:

      group_analyzer.analyze_groups()    → EU-average displacement vis.parquet
                                           (one bar-chart row per country in
                                            interactive_plotter's grid view)

      normalizing_processor.normalize()  → min-max DRS (deferred; currently
                                           done inline — two lines of polars).
                                           Activate once pivot_processor.py
                                           provides the value_last column in a
                                           standalone parquet.

    Returns policy DataFrame (ADPI, DRS, vulnerability_score per country).
    """
    tag = "volt_report"
    os.makedirs(output_dir, exist_ok=True)

    valid = [df for df in (all_ols_dfs or []) if df is not None and len(df) > 0]
    if not valid:
        log_warning("No valid OLS inputs — writing empty report", tag)
        pl.DataFrame().write_parquet(
            os.path.join(output_dir, "LAV_volt_report.parquet"), compression="snappy")
        return pl.DataFrame()

    df_all = pl.concat(valid, how="diagonal_relaxed")
    df_all.write_parquet(
        os.path.join(output_dir, "LAV_ols_summary.parquet"), compression="snappy")

    # ── Displacement subset ───────────────────────────────────────────────────
    df_disp = df_all.filter(pl.col("displacement_score").is_not_null())

    if len(df_disp) > 0:
        # Build epoch-format parquet for AT group_analyzer:
        #   condition = country  (each country is one "condition")
        #   epoch_id  = "run"    (single epoch per country)
        #   columns   = sectors  (displacement scores)
        #
        # group_analyzer computes mean ± SEM per sector across epochs (countries)
        # and writes a _vis.parquet with one row per condition (country).
        epoch_df = (
            df_disp.pivot(values="displacement_score",
                          index=["country"], on="sector")
            .rename({"country": "epoch_id"})
            .with_columns(pl.lit("AI_Displacement").alias("condition"))
        )
        epoch_path = os.path.join(output_dir, "LAV_displacement_epoch.parquet")
        epoch_df.write_parquet(epoch_path, compression="snappy")

        ga = _at_module("analyzers.group_analyzer")
        if ga is not None:
            import json
            present_sectors = [s for s in SECTOR_COLUMNS if s in epoch_df.columns]
            groups_cfg = json.dumps({s: [s] for s in present_sectors})
            with _in_dir(output_dir):
                ga.analyze_groups(
                    epoch_path, groups_cfg,
                    x_label="Sector", y_label="AI Displacement Score",
                    suffix="displacement_grp",
                )
            log_info("AT group_analyzer: cross-country displacement vis.parquet written", tag)
        else:
            # Fallback: inline cross-country grouped bar vis.parquet
            log_warning("AT group_analyzer unavailable — producing inline fallback vis", tag)
            countries = sorted(df_disp["country"].unique().to_list())
            labels, y_data = [], []
            for sector in SECTOR_COLUMNS:
                scores = []
                for c in countries:
                    row = df_disp.filter(
                        (pl.col("country") == c) & (pl.col("sector") == sector))
                    scores.append(float(row["displacement_score"][0]) if len(row) > 0 else 0.0)
                labels.append(sector)
                y_data.append(scores)
            _make_vis(
                "bar", "AI Displacement by Country & Sector",
                "Country", "Displacement Score",
                labels, countries, y_data,
            ).write_parquet(
                os.path.join(output_dir, "LAV_displacement_epoch_displacement_grp_vis.parquet"),
                compression="snappy",
            )

    # ── ADPI per country = mean displacement across sectors ───────────────────
    adpi_df = (
        df_disp.group_by(["participant_id", "country", "iso3"])
        .agg(
            pl.col("displacement_score").mean().round(5).alias("adpi"),
            pl.col("displacement_score").max().round(5).alias("adpi_max_sector"),
            pl.col("sector").sort_by("displacement_score").last()
              .alias("most_at_risk_sector"),
        )
        .sort("adpi", descending=True)
    )
    for row in adpi_df.iter_rows(named=True):
        log_info(f"  {row['country']:15s}: ADPI={row['adpi']:.4f}  "
                 f"most-at-risk={row['most_at_risk_sector']}", tag)

    # ── DRS per country (digital readiness signal) ────────────────────────────
    # AT: normalizing_processor.normalize(path, 'minmax', 'drs_raw') would do
    # the min-max step once drs_raw is available in a standalone parquet file.
    DRS_INDICATORS = ["internet_users_pct", "high_tech_exports_pct_mfg"]
    drs_rows = []
    for pid in df_all["participant_id"].unique().to_list():
        sub     = df_all.filter(pl.col("participant_id") == pid)
        country = sub["country"][0]
        vals    = [
            float(sub.filter(pl.col("indicator") == ind)["value_last"][0])
            for ind in DRS_INDICATORS
            if ind in sub["indicator"].to_list()
               and sub.filter(pl.col("indicator") == ind)["value_last"][0] is not None
        ]
        drs_rows.append({
            "participant_id": pid,
            "country":        country,
            "drs_raw":        float(np.mean(vals)) if vals else 0.0,
        })
    drs_df  = pl.DataFrame(drs_rows)
    drs_max = drs_df["drs_raw"].max() or 1.0
    # AT normalizing_processor.normalize(path, 'minmax', 'drs_raw') equivalent:
    drs_df = drs_df.with_columns((pl.col("drs_raw") / drs_max).round(4).alias("drs"))

    # ── Policy table: ADPI + DRS + vulnerability ──────────────────────────────
    policy = (
        adpi_df.join(drs_df.select(["participant_id", "drs"]),
                     on="participant_id", how="left")
        .with_columns(
            (pl.col("adpi") / (pl.col("drs") + 0.01)).round(5)
            .alias("vulnerability_score"))
        .sort("adpi", descending=True)
    )
    log_info(f"  {'Country':15s} {'ADPI':>8} {'DRS':>8} {'Vulnerability':>14}", tag)
    for row in policy.iter_rows(named=True):
        log_info(f"  {row['country']:15s} {row['adpi']:8.4f} "
                 f"{row['drs'] or 0.0:8.4f} "
                 f"{row['vulnerability_score'] or 0.0:14.4f}", tag)

    # ── Vulnerability vis.parquet ─────────────────────────────────────────────
    df_s = policy.sort("vulnerability_score", descending=True)
    _make_vis(
        "bar", "Labour Displacement Vulnerability",
        "Country", "Score",
        ["Vulnerability (ADPI/DRS)", "ADPI"],
        df_s["country"].to_list(),
        [
            [round(v or 0.0, 4) for v in df_s["vulnerability_score"].to_list()],
            [round(v or 0.0, 4) for v in df_s["adpi"].to_list()],
        ],
    ).write_parquet(
        os.path.join(output_dir, "LAV_vulnerability_vis.parquet"),
        compression="snappy",
    )

    policy.write_parquet(
        os.path.join(output_dir, "LAV_volt_report.parquet"), compression="snappy")
    log_info(f"Saved volt report → {output_dir}", tag)
    return policy


# ═══════════════════════════════════════════════════════════════════════════════
# 5. YEAR-BY-YEAR VIS HELPERS  (time-series views — no AT equivalent yet)
#
# These produce the year-by-year grouped bar charts showing how employment
# shares and unemployment rates evolved over time.  They are unique to the
# LAV analysis (no generic AT module covers this layout today).
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_ts(df_wide, col):
    """Return (years_list, values_list) for one wide-format column."""
    data = (df_wide.select(["year", col])
            .filter(pl.col(col).is_not_null())
            .sort("year"))
    return data["year"].to_list(), [round(v, 3) for v in data[col].to_list()]


def _make_vis(plot_type, title, x_label, y_label, labels, x_data, y_data):
    """
    Build a single-row _vis.parquet DataFrame in AT interactive_plotter schema.

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
    Writes to caller; returns vis DataFrame or None.
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
    Writes to caller; returns vis DataFrame or None.
    """
    country = df_wide["country"][0]
    SERIES  = {"unemployment_rate":       "Unemployment Rate (%)",
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




# ═══════════════════════════════════════════════════════════════════════════════
# 6. INTERACTIVE PLOTTER BRIDGE  (AT interactive_plotter.py)
#
#    Every _vis.parquet is registered into a single interactive HTML archive
#    with collapsible tree navigation and Plotly charts.
#
#    CLI:  python interactive_plotter.py <vis.parquet> <out_dir> <prefix>
#                                        <project_name> <sidecar_dir>
#
#    Uses the same sibling-repo resolution as _at_python_path().
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
