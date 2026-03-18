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

Usage:
    python api_reader.py <LAV_XXX_config.json>
Output:
    <LAV_XXX>_api_raw.parquet  (long-format: country, iso3, indicator, year, value)
"""

import sys
import os
import json
import time

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


def run(config_path):
    log_info(f"Started for: {config_path}")

    with open(config_path, "r", encoding="utf-8") as fh:
        config = json.load(fh)

    participant_id = config["participant_id"]
    country_name   = config["country"]
    iso3           = config["iso3"]
    iso2           = config["iso2"]
    year_start     = config.get("year_start", 2010)
    year_end       = config.get("year_end", 2023)

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
            log_info(
                f"  {indicator_name}: {len(records)} observations"
            )
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
        # Sort for reproducibility
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
        indicators_fetched = df["indicator"].unique().to_list()
        log_info(f"Indicators: {sorted(indicators_fetched)}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(
            "[api_reader] Usage: python api_reader.py <LAV_XXX_config.json>"
        )
        sys.exit(1)
    try:
        run(sys.argv[1])
    except Exception as exc:
        log_error(f"Fatal: {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
