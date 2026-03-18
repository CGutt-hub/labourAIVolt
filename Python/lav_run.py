"""
LAV Run: Standalone orchestrator for the Labour-AI-Volt analysis pipeline.

Executes the full analysis without requiring Nextflow or the AnalysisToolbox,
making it suitable for GitHub Actions CI and quick local runs.

The pipeline can now run in two modes:

  MODE A — embedded country list (no LAV_data folder needed)
  ─────────────────────────────────────────────────────────
    python Python/lav_run.py --countries all
    python Python/lav_run.py --countries DE,FR,NL

  MODE B — config files in LAV_data/ (original behaviour, still supported)
  ─────────────────────────────────────────────────────────────────────────
    python Python/lav_run.py --data-dir LAV_data

  When both --data-dir and --countries are given, country configs from LAV_data
  take precedence and --countries fills in any countries NOT in the data dir.

  If --data-dir is given but the folder has no LAV_*/LAV_*_config.json files,
  the pipeline automatically falls back to the embedded country list — so it
  works out-of-the-box on a fresh clone with no manual setup.

Data freshness
──────────────
  The World Bank API is queried live every run. year_end is auto-detected from
  the API so new data is picked up automatically as soon as it is published —
  no manual year_end maintenance is required.

Pipeline steps (per country):
  1. api_reader              – fetch World Bank labour data (auto year_end)
  2. normalizing_processor   – pivot to wide format
  3. displacement_analyzer   – compute AI displacement scores
  4. trend_analyzer          – fit time-series trends

Group step (all countries):
  5. volt_report_analyzer    – cross-country synthesis & Volt policy metrics

Results land in:  <output_dir>/LAV_l1/<participant_id>/
Group results in: <output_dir>/LAV_l2/
"""

import argparse
import glob
import os
import subprocess
import sys

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
def log_info(msg):    print(f"[lav_run] INFO: {msg}")
def log_warning(msg): print(f"[lav_run] WARNING: {msg}")
def log_error(msg):   print(f"[lav_run] ERROR: {msg}")


# ---------------------------------------------------------------------------
# Embedded Volt country catalogue
# These countries are used when no LAV_data/ config files are found, or when
# --countries is specified explicitly.  Add rows here to expand coverage.
# year_start / year_end are intentionally absent — api_reader auto-detects
# the latest available year from the World Bank API on every run.
# ---------------------------------------------------------------------------
VOLT_COUNTRIES = [
    {
        "participant_id": "LAV_001",
        "country":        "Germany",
        "iso3":           "DEU",
        "iso2":           "DE",
        "volt_chapter":   "Volt Deutschland",
        "year_start":     2000,
    },
    {
        "participant_id": "LAV_002",
        "country":        "France",
        "iso3":           "FRA",
        "iso2":           "FR",
        "volt_chapter":   "Volt France",
        "year_start":     2000,
    },
    {
        "participant_id": "LAV_003",
        "country":        "Netherlands",
        "iso3":           "NLD",
        "iso2":           "NL",
        "volt_chapter":   "Volt Nederland",
        "year_start":     2000,
    },
    {
        "participant_id": "LAV_004",
        "country":        "Belgium",
        "iso3":           "BEL",
        "iso2":           "BE",
        "volt_chapter":   "Volt Belgium",
        "year_start":     2000,
    },
    {
        "participant_id": "LAV_005",
        "country":        "Italy",
        "iso3":           "ITA",
        "iso2":           "IT",
        "volt_chapter":   "Volt Italia",
        "year_start":     2000,
    },
    {
        "participant_id": "LAV_006",
        "country":        "Spain",
        "iso3":           "ESP",
        "iso2":           "ES",
        "volt_chapter":   "Volt España",
        "year_start":     2000,
    },
]

# Index by ISO-2 code for fast lookup
_VOLT_BY_ISO2 = {c["iso2"]: c for c in VOLT_COUNTRIES}


# ---------------------------------------------------------------------------
# Script paths (relative to this file's location)
# ---------------------------------------------------------------------------
HERE           = os.path.dirname(os.path.abspath(__file__))
READERS_DIR    = os.path.join(HERE, "readers")
PROCESSORS_DIR = os.path.join(HERE, "processors")
ANALYZERS_DIR  = os.path.join(HERE, "analyzers")

API_READER        = os.path.join(READERS_DIR,    "api_reader.py")
NORMALIZER        = os.path.join(PROCESSORS_DIR, "normalizing_processor.py")
DISPLACEMENT_ANLZ = os.path.join(ANALYZERS_DIR,  "displacement_analyzer.py")
TREND_ANLZ        = os.path.join(ANALYZERS_DIR,  "trend_analyzer.py")
VOLT_REPORT_ANLZ  = os.path.join(ANALYZERS_DIR,  "volt_report_analyzer.py")


# ---------------------------------------------------------------------------
# Helper: run a Python script in a subprocess
# ---------------------------------------------------------------------------
def run_script(script_path, args, cwd=None):
    """Run `python <script_path> <args...>` and stream output."""
    cmd = [sys.executable, "-u", script_path] + [str(a) for a in args]
    log_info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd)
    if result.returncode != 0:
        log_error(f"Script failed (exit {result.returncode}): {script_path}")
        return False
    return True


# ---------------------------------------------------------------------------
# Country resolution
# ---------------------------------------------------------------------------
def _load_config_files(data_dir):
    """Return a list of dicts loaded from LAV_data JSON config files."""
    import json
    configs = []
    for path in sorted(glob.glob(
        os.path.join(data_dir, "LAV_*", "LAV_*_config.json")
    )):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                cfg = json.load(fh)
            # Normalize: remove explicit year_end so API auto-detects latest
            cfg.pop("year_end", None)
            if "year_start" not in cfg:
                cfg["year_start"] = 2000
            configs.append(cfg)
        except Exception as exc:
            log_warning(f"Could not read {path}: {exc}")
    return configs


def resolve_countries(data_dir, countries_arg):
    """
    Build the final list of country dicts to process.

    Priority:
      1. If --countries is set, use those (either 'all' or comma-separated ISO-2s).
         JSON configs from data_dir are merged in for any matching countries
         so local overrides (e.g. custom year_start) are respected.
      2. If --countries is not set and data_dir has configs, use those.
      3. If --countries is not set and data_dir has NO configs, fall back to
         the full embedded VOLT_COUNTRIES list (zero-config mode).
    """
    # Load any JSON configs present in data_dir
    dir_configs = {}
    if data_dir and os.path.isdir(data_dir):
        for cfg in _load_config_files(data_dir):
            dir_configs[cfg.get("iso2", "").upper()] = cfg

    if countries_arg:
        # Explicit --countries flag
        if countries_arg.lower() == "all":
            iso2_list = [c["iso2"] for c in VOLT_COUNTRIES]
        else:
            iso2_list = [c.strip().upper() for c in countries_arg.split(",") if c.strip()]

        result = []
        for iso2 in iso2_list:
            if iso2 in dir_configs:
                result.append(dir_configs[iso2])
            elif iso2 in _VOLT_BY_ISO2:
                result.append(dict(_VOLT_BY_ISO2[iso2]))   # copy
            else:
                log_warning(
                    f"ISO-2 '{iso2}' not found in embedded list or data dir — skipping"
                )
        return result

    # No explicit --countries
    if dir_configs:
        log_info(f"Using {len(dir_configs)} country configs from {data_dir}")
        return list(dir_configs.values())

    # No configs found anywhere → zero-config fallback
    log_info(
        "No country configs found in data dir — "
        "using embedded Volt country list (zero-config mode)"
    )
    return [dict(c) for c in VOLT_COUNTRIES]


# ---------------------------------------------------------------------------
# Per-country pipeline
# ---------------------------------------------------------------------------
def process_country(cfg, l1_dir):
    """Run the full L1 pipeline for one country. Returns (disp_file, trend_file)."""
    participant_id = cfg["participant_id"]
    work_dir = os.path.join(l1_dir, participant_id)
    os.makedirs(work_dir, exist_ok=True)

    log_info("─" * 60)
    log_info(f"Processing: {cfg['country']} ({cfg['iso2']})  [{participant_id}]")
    log_info(f"Working dir: {work_dir}")

    # ── Step 1: fetch data (inline flags — no JSON config file required) ──
    api_args = [
        "--iso2",           cfg["iso2"],
        "--iso3",           cfg["iso3"],
        "--country",        cfg["country"],
        "--participant-id", participant_id,
        "--year-start",     str(cfg.get("year_start", 2000)),
        # Intentionally omit --year-end → api_reader auto-detects latest year
    ]
    if not run_script(API_READER, api_args, cwd=work_dir):
        log_warning(f"API fetch failed for {participant_id} — skipping")
        return None, None

    raw_parquet = os.path.join(work_dir, f"{participant_id}_api_raw.parquet")
    if not os.path.exists(raw_parquet):
        log_warning(f"Expected output not found: {raw_parquet}")
        return None, None

    # ── Step 2: normalize ────────────────────────────────────────────────
    if not run_script(NORMALIZER, [raw_parquet], cwd=work_dir):
        log_warning(f"Normalization failed for {participant_id} — skipping")
        return None, None

    norm_parquet = os.path.join(work_dir, f"{participant_id}_normalized.parquet")
    if not os.path.exists(norm_parquet):
        log_warning(f"Expected output not found: {norm_parquet}")
        return None, None

    # ── Step 3a: displacement analysis ───────────────────────────────────
    disp_file = None
    if run_script(DISPLACEMENT_ANLZ, [norm_parquet], cwd=work_dir):
        candidate = os.path.join(work_dir, f"{participant_id}_displacement.parquet")
        if os.path.exists(candidate):
            disp_file = candidate
        else:
            log_warning(f"Displacement output not found: {candidate}")
    else:
        log_warning(f"Displacement analysis failed for {participant_id}")

    # ── Step 3b: trend analysis ───────────────────────────────────────────
    trend_file = None
    if run_script(TREND_ANLZ, [norm_parquet], cwd=work_dir):
        candidate = os.path.join(work_dir, f"{participant_id}_trends.parquet")
        if os.path.exists(candidate):
            trend_file = candidate
        else:
            log_warning(f"Trends output not found: {candidate}")
    else:
        log_warning(f"Trend analysis failed for {participant_id}")

    return disp_file, trend_file


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(data_dir, output_dir, countries_arg):
    repo_root  = os.path.dirname(HERE)
    data_dir   = os.path.abspath(os.path.join(repo_root, data_dir)) if data_dir else None
    output_dir = os.path.abspath(os.path.join(repo_root, output_dir))

    l1_dir = os.path.join(output_dir, "LAV_l1")
    l2_dir = os.path.join(output_dir, "LAV_l2")
    os.makedirs(l1_dir, exist_ok=True)
    os.makedirs(l2_dir, exist_ok=True)

    log_info(f"Output directory: {output_dir}")

    country_list = resolve_countries(data_dir, countries_arg)
    if not country_list:
        log_error("No countries to process — exiting.")
        sys.exit(1)

    log_info(f"Countries to process: {len(country_list)}")

    displacement_files = []
    trend_files        = []
    failed             = []

    for cfg in country_list:
        disp, trend = process_country(cfg, l1_dir)
        if disp is None and trend is None:
            failed.append(cfg["participant_id"])
        if disp:
            displacement_files.append(disp)
        if trend:
            trend_files.append(trend)

    # ── Group-level Volt report ──────────────────────────────────────────
    log_info("─" * 60)
    log_info("Running group-level Volt report analysis …")

    all_group_inputs = displacement_files + trend_files
    if all_group_inputs:
        if not run_script(VOLT_REPORT_ANLZ, all_group_inputs, cwd=l2_dir):
            log_warning("Volt report analyzer returned an error")
    else:
        log_warning("No group inputs available — skipping Volt report")

    # ── Summary ──────────────────────────────────────────────────────────
    log_info("═" * 60)
    log_info("Pipeline complete.")
    log_info(
        f"Successful: {len(country_list) - len(failed)}/{len(country_list)}"
    )
    if failed:
        log_warning(f"Failed: {', '.join(failed)}")
    log_info(f"Results → {output_dir}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "LAV Labour-AI-Volt analysis pipeline (standalone, no Nextflow needed).\n\n"
            "Runs without any config files:\n"
            "  python Python/lav_run.py --countries all\n\n"
            "Or with a LAV_data/ directory:\n"
            "  python Python/lav_run.py\n\n"
            "Year-end is always auto-detected from the World Bank API, so\n"
            "new data is picked up automatically when it is published."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--data-dir",
        default="LAV_data",
        help=(
            "Directory containing per-country LAV_XXX/ subdirectories. "
            "If omitted or empty, the embedded Volt country list is used. "
            "(default: LAV_data)"
        ),
    )
    parser.add_argument(
        "--output-dir",
        default="LAV_results",
        help="Root directory for pipeline outputs (default: LAV_results)",
    )
    parser.add_argument(
        "--countries",
        default=None,
        metavar="all|ISO2[,ISO2,...]",
        help=(
            "Countries to analyse. Use 'all' for all embedded Volt countries, "
            "or a comma-separated list of ISO-2 codes (e.g. 'DE,FR,NL'). "
            "When omitted, country configs are read from --data-dir; if none "
            "exist the full embedded list is used automatically."
        ),
    )
    args = parser.parse_args()
    main(args.data_dir, args.output_dir, args.countries)
