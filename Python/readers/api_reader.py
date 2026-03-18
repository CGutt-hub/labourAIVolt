"""
LAV API Reader: Fetches labour market and AI-displacement-relevant data
from the World Bank public API (no API key required).

Indicators collected:
  - Employment by broad sector (industry, agriculture, services)
  - Unemployment rate
  - Labour force size
  - Employment-to-population ratio
  - Internet users (digitalization proxy)
  - GDP per capita and growth
  - High-technology exports (automation-economy proxy)
  - ICT goods exports

Usage — two equivalent forms:

  # Form 1: pass a JSON config file (original behaviour, still supported)
  python api_reader.py LAV_001_config.json

  # Form 2: pass country details inline — NO config file needed
  python api_reader.py --iso2 DE --iso3 DEU --country Germany \
                       --participant-id LAV_001

  # Both forms accept optional year overrides:
  python api_reader.py --iso2 DE ... --year-start 2005 --year-end 2022

  # Omit --year-end (or set it in JSON) to always fetch up to the
  # most recently published World Bank data automatically.

Output:
    <participant_id>_api_raw.parquet
    (long-format: participant_id, country, iso3, source,
                  indicator, indicator_code, year, value)
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

import polars as pl
import requests

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
def log_info(msg):    print(f"[api_reader] INFO: {msg}")
def log_warning(msg): print(f"[api_reader] WARNING: {msg}")
def log_error(msg):   print(f"[api_reader] ERROR: {msg}")

# ---------------------------------------------------------------------------
# World Bank indicator catalogue (no auth required)
# ---------------------------------------------------------------------------
WORLDBANK_INDICATORS = {
    "employment_industry_pct":    "SL.IND.EMPL.ZS",   # Employment in industry (%)
    "employment_agriculture_pct": "SL.AGR.EMPL.ZS",   # Employment in agriculture (%)
    "employment_services_pct":    "SL.SRV.EMPL.ZS",   # Employment in services (%)
    "unemployment_rate":          "SL.UEM.TOTL.ZS",   # Unemployment, total (% labour force)
    "youth_unemployment_rate":    "SL.UEM.1524.ZS",   # Youth unemployment (%)
    "labor_force_total":          "SL.TLF.TOTL.IN",   # Labour force, total
    "employment_to_pop_ratio":    "SL.EMP.TOTL.SP.ZS",# Employment-to-population ratio (%)
    "internet_users_pct":         "IT.NET.USER.ZS",   # Internet users (% population)
    "gdp_per_capita_usd":         "NY.GDP.PCAP.CD",   # GDP per capita (current USD)
    "gdp_growth_annual_pct":      "NY.GDP.MKTP.KD.ZG",# GDP growth (annual %)
    "high_tech_exports_pct_mfg":  "TX.VAL.TECH.MF.ZS",# High-tech exports (% manufactured exports)
    "ict_goods_exports_pct":      "TX.VAL.ICTG.ZS.UN",# ICT goods exports (% total goods exports)
    "wage_salary_workers_pct":    "SL.EMP.WORK.ZS",   # Wage and salaried workers (% total)
}

WB_BASE_URL = "https://api.worldbank.org/v2"

# Default start year; end year is always resolved dynamically from the API.
DEFAULT_YEAR_START = 2000


# ---------------------------------------------------------------------------
# Year auto-detection
# ---------------------------------------------------------------------------
def latest_available_year(iso2, indicator_code, retries=3):
    """
    Ask the World Bank for the single most-recent available data point
    for *indicator_code* in country *iso2* and return that year as int.

    Falls back to (current calendar year - 1) if the API is unreachable,
    ensuring new data is always captured as soon as the World Bank
    publishes it — with no manual year_end maintenance required.
    """
    url = (
        f"{WB_BASE_URL}/country/{iso2}/indicator/{indicator_code}"
        f"?format=json&mrv=1&per_page=1"
    )
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
    # Fallback: assume data up to last calendar year is available
    return datetime.utcnow().year - 1


def resolve_year_end(iso2, year_end_override):
    """
    Return the effective year_end to use.

    If *year_end_override* is provided (not None), use it directly.
    Otherwise query the World Bank for the latest year with employment data
    so the pipeline always captures freshly published data automatically.
    """
    if year_end_override is not None:
        return year_end_override
    # Use employment-in-services as the probe indicator (typically updated first)
    year = latest_available_year(iso2, "SL.SRV.EMPL.ZS")
    log_info(f"Auto-detected latest available year from World Bank: {year}")
    return year


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------
def fetch_worldbank(iso2, indicator_name, indicator_code,
                    year_start, year_end, retries=3):
    """Fetch one World Bank indicator time-series for a country."""
    url = (
        f"{WB_BASE_URL}/country/{iso2}/indicator/{indicator_code}"
        f"?format=json&date={year_start}:{year_end}&per_page=100"
    )
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
            if len(payload) < 2 or not payload[1]:
                log_warning(
                    f"No data returned for {indicator_name} ({indicator_code})"
                )
                return []
            records = []
            for entry in payload[1]:
                if entry.get("value") is not None:
                    records.append({
                        "source":           "WorldBank",
                        "indicator":        indicator_name,
                        "indicator_code":   indicator_code,
                        "year":             int(entry["date"]),
                        "value":            float(entry["value"]),
                    })
            return records
        except requests.exceptions.Timeout:
            log_warning(
                f"Timeout on attempt {attempt + 1}/{retries} for {indicator_name}"
            )
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
        except requests.exceptions.RequestException as exc:
            log_warning(
                f"Request error (attempt {attempt + 1}/{retries}): {exc}"
            )
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
        except Exception as exc:
            log_error(f"Unexpected error fetching {indicator_name}: {exc}")
            return []
    return []


# ---------------------------------------------------------------------------
# Core runner
# ---------------------------------------------------------------------------
def run(participant_id, country_name, iso3, iso2,
        year_start=DEFAULT_YEAR_START, year_end_override=None):
    """
    Fetch all indicators for one country and write the raw parquet.

    Parameters
    ----------
    year_end_override : int or None
        Pass an explicit year to stop at, or None to auto-detect the latest
        year currently available in the World Bank API.
    """
    year_end = resolve_year_end(iso2, year_end_override)

    log_info(
        f"Country: {country_name} ({iso2}/{iso3}), "
        f"years {year_start}–{year_end}"
    )

    all_records = []

    for indicator_name, indicator_code in WORLDBANK_INDICATORS.items():
        records = fetch_worldbank(
            iso2, indicator_name, indicator_code,
            year_start, year_end
        )
        if records:
            log_info(f"  {indicator_name}: {len(records)} observations")
        else:
            log_warning(f"  {indicator_name}: no data")
        all_records.extend(records)
        time.sleep(0.25)   # polite rate-limiting

    # Build output DataFrame
    if all_records:
        df = pl.DataFrame(all_records).with_columns([
            pl.lit(country_name).alias("country"),
            pl.lit(iso3).alias("iso3"),
            pl.lit(participant_id).alias("participant_id"),
        ])
        df = df.sort(["indicator", "year"])
    else:
        log_warning("No data fetched — writing empty parquet")
        df = pl.DataFrame({
            "participant_id":  pl.Series([], dtype=pl.Utf8),
            "country":         pl.Series([], dtype=pl.Utf8),
            "iso3":            pl.Series([], dtype=pl.Utf8),
            "source":          pl.Series([], dtype=pl.Utf8),
            "indicator":       pl.Series([], dtype=pl.Utf8),
            "indicator_code":  pl.Series([], dtype=pl.Utf8),
            "year":            pl.Series([], dtype=pl.Int64),
            "value":           pl.Series([], dtype=pl.Float64),
        })

    output_file = f"{participant_id}_api_raw.parquet"
    df.write_parquet(output_file, compression="snappy")
    log_info(f"Saved {len(df)} records → {output_file}")
    if len(df) > 0:
        log_info(f"Indicators: {sorted(df['indicator'].unique().to_list())}")


# ---------------------------------------------------------------------------
# Entry point — accepts either a JSON config path or inline --flags
# ---------------------------------------------------------------------------
def _parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Fetch World Bank labour-market data for one country.\n\n"
            "Two usage forms are supported:\n"
            "  1) Pass a JSON config file as the first positional argument.\n"
            "  2) Pass country details via --iso2, --iso3, --country, "
            "--participant-id flags (no file needed)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # Positional — optional so inline flags work without it
    parser.add_argument(
        "config",
        nargs="?",
        default=None,
        help="Path to a LAV_XXX_config.json file (optional if inline flags given)",
    )
    parser.add_argument("--iso2",           default=None, help="ISO-2 country code (e.g. DE)")
    parser.add_argument("--iso3",           default=None, help="ISO-3 country code (e.g. DEU)")
    parser.add_argument("--country",        default=None, help="Country name (e.g. Germany)")
    parser.add_argument("--participant-id", default=None, dest="participant_id",
                        help="Participant ID (e.g. LAV_001)")
    parser.add_argument("--year-start",     default=None, type=int, dest="year_start",
                        help=f"First year to fetch (default: {DEFAULT_YEAR_START})")
    parser.add_argument("--year-end",       default=None, type=int, dest="year_end",
                        help="Last year to fetch (default: auto-detect latest from API)")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    try:
        if args.config:
            # ── Form 1: JSON config file ──────────────────────────────────
            log_info(f"Loading config: {args.config}")
            with open(args.config, "r", encoding="utf-8") as fh:
                cfg = json.load(fh)
            participant_id = cfg["participant_id"]
            country_name   = cfg["country"]
            iso3           = cfg["iso3"]
            iso2           = cfg["iso2"]
            # CLI flags override JSON values when both are present
            year_start = args.year_start if args.year_start is not None \
                         else cfg.get("year_start", DEFAULT_YEAR_START)
            # year_end: CLI > JSON > None (auto-detect)
            year_end_cfg = cfg.get("year_end", None)
            year_end_override = args.year_end if args.year_end is not None \
                                else year_end_cfg
        else:
            # ── Form 2: inline flags — no config file needed ──────────────
            missing = [f for f, v in [
                ("--iso2",           args.iso2),
                ("--iso3",           args.iso3),
                ("--country",        args.country),
                ("--participant-id", args.participant_id),
            ] if not v]
            if missing:
                print(
                    f"[api_reader] ERROR: Missing required flags: {', '.join(missing)}\n"
                    "Provide either a config JSON path or all four inline flags."
                )
                sys.exit(1)
            participant_id = args.participant_id
            country_name   = args.country
            iso3           = args.iso3
            iso2           = args.iso2
            year_start     = args.year_start if args.year_start is not None \
                             else DEFAULT_YEAR_START
            year_end_override = args.year_end  # None → auto-detect

        run(
            participant_id=participant_id,
            country_name=country_name,
            iso3=iso3,
            iso2=iso2,
            year_start=year_start,
            year_end_override=year_end_override,
        )

    except Exception as exc:
        log_error(f"Fatal: {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
