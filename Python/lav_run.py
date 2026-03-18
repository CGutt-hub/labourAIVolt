"""
LAV Run — standalone orchestrator (no Nextflow, no subprocesses).

Calls lav_pipeline functions directly in-process:
  api_reader -> pivot -> analyze_ols (OLS trends + displacement scores)
  -> generate_volt_report (AT group_analyzer + policy table)
  -> register vis.parquets with AT interactive_plotter

Run modes
---------
  # Zero-config: uses the embedded Volt country list, auto year-end
  python Python/lav_run.py --countries all

  # Specific countries (ISO-2 codes)
  python Python/lav_run.py --countries DE,FR,NL

  # With LAV_data/ config files (original behaviour)
  python Python/lav_run.py

Results land in:  <output_dir>/LAV_l1/<participant_id>/
Group results in: <output_dir>/LAV_l2/
HTML archive:     <output_dir>/.bin/LAV_results.html  (requires AnalysisToolbox)
"""

import argparse
import glob
import json
import os
import sys

# Allow import from the Python/ package root regardless of cwd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lav_pipeline import (
    fetch_country, normalize, analyze_ols,
    generate_volt_report,
    make_sector_employment_vis, make_unemployment_vis,
    register_vis, find_interactive_plotter,
    VOLT_COUNTRIES, _VOLT_BY_ISO2,
    log_info, log_warning, log_error,
)

# -- Country resolution --------------------------------------------------------

def _load_config_files(data_dir):
    configs = []
    for path in sorted(glob.glob(os.path.join(data_dir, "LAV_*", "LAV_*_config.json"))):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                cfg = json.load(fh)
            cfg.pop("year_end", None)
            cfg.setdefault("year_start", 2000)
            configs.append(cfg)
        except Exception as exc:
            log_warning(f"Could not read {path}: {exc}")
    return configs


def resolve_countries(data_dir, countries_arg):
    dir_configs = {}
    if data_dir and os.path.isdir(data_dir):
        for cfg in _load_config_files(data_dir):
            dir_configs[cfg.get("iso2", "").upper()] = cfg

    if countries_arg:
        iso2_list = ([c["iso2"] for c in VOLT_COUNTRIES]
                     if countries_arg.lower() == "all"
                     else [c.strip().upper() for c in countries_arg.split(",") if c.strip()])
        result = []
        for iso2 in iso2_list:
            if iso2 in dir_configs:
                result.append(dir_configs[iso2])
            elif iso2 in _VOLT_BY_ISO2:
                result.append(dict(_VOLT_BY_ISO2[iso2]))
            else:
                log_warning(f"ISO-2 '{iso2}' not found -- skipping")
        return result

    if dir_configs:
        log_info(f"Using {len(dir_configs)} country configs from {data_dir}")
        return list(dir_configs.values())

    log_info("No config files found -- using embedded Volt country list (zero-config mode)")
    return [dict(c) for c in VOLT_COUNTRIES]


# -- Per-country L1 pipeline ---------------------------------------------------

def process_country(cfg, l1_dir, output_dir, plotter=None):
    """Run full L1 pipeline for one country. Returns df_ols or None."""
    pid      = cfg["participant_id"]
    work_dir = os.path.join(l1_dir, pid)
    sidecar  = os.path.join(work_dir, "plots")
    os.makedirs(work_dir, exist_ok=True)

    log_info("-" * 60)
    log_info(f"Processing: {cfg['country']} ({cfg['iso2']})  [{pid}]")

    # Step 1 -- Fetch (World Bank REST API, auto year-end)
    df_raw = fetch_country(pid, cfg["country"], cfg["iso3"], cfg["iso2"],
                           year_start=cfg.get("year_start", 2000))
    if len(df_raw) == 0:
        log_warning(f"No data fetched for {pid} -- skipping")
        return None
    df_raw.write_parquet(os.path.join(work_dir, f"{pid}_api_raw.parquet"),
                         compression="snappy")

    # Step 2 -- Pivot long->wide
    # NOTE: AT pivot_processor.py would replace this step once added.
    df_wide = normalize(df_raw)
    if len(df_wide) == 0:
        log_warning(f"Pivot produced empty result for {pid} -- skipping")
        return None
    df_wide.write_parquet(os.path.join(work_dir, f"{pid}_normalized.parquet"),
                          compression="snappy")

    # Step 3 -- OLS trends + displacement scores
    # NOTE: AT timeseries_ols_processor.py would replace the linregress loops;
    #       only the Frey & Osborne weighting stays in LAV.
    # analyze_ols() writes its own _ols_vis and _displacement_vis parquets.
    df_ols = analyze_ols(df_wide, work_dir)
    if len(df_ols) == 0:
        log_warning(f"OLS produced empty result for {pid} -- skipping")
        return None
    df_ols.write_parquet(os.path.join(work_dir, f"{pid}_ols.parquet"),
                         compression="snappy")

    # Step 4 -- Year-by-year vis.parquets (sector employment + unemployment)
    for vis, prefix in [
        (make_sector_employment_vis(df_wide), f"{pid}_sector_employment_vis"),
        (make_unemployment_vis(df_wide),      f"{pid}_unemployment_vis"),
    ]:
        if vis is not None:
            vis.write_parquet(os.path.join(work_dir, f"{prefix}.parquet"),
                              compression="snappy")

    # Step 5 -- Register all vis.parquets with AT interactive_plotter
    for fname in [
        f"{pid}_ols_vis.parquet",
        f"{pid}_displacement_vis.parquet",
        f"{pid}_sector_employment_vis.parquet",
        f"{pid}_unemployment_vis.parquet",
    ]:
        vis_path = os.path.join(work_dir, fname)
        if os.path.exists(vis_path):
            register_vis(vis_path, output_dir,
                         fname.replace(".parquet", ""),
                         sidecar, project="LAV", plotter=plotter)

    return df_ols


# -- Main ----------------------------------------------------------------------

def main(data_dir, output_dir, countries_arg):
    repo_root  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir   = os.path.abspath(os.path.join(repo_root, data_dir)) if data_dir else None
    output_dir = os.path.abspath(os.path.join(repo_root, output_dir))
    l1_dir     = os.path.join(output_dir, "LAV_l1")
    l2_dir     = os.path.join(output_dir, "LAV_l2")
    os.makedirs(l1_dir, exist_ok=True)
    os.makedirs(l2_dir, exist_ok=True)

    log_info(f"Output directory: {output_dir}")

    country_list = resolve_countries(data_dir, countries_arg)
    if not country_list:
        log_error("No countries to process -- exiting.")
        sys.exit(1)
    log_info(f"Countries to process: {len(country_list)}")

    plotter = find_interactive_plotter()
    if plotter:
        log_info(f"AnalysisToolbox interactive_plotter found: {plotter}")
    else:
        log_warning("AnalysisToolbox not found -- vis.parquet files written but "
                    "HTML archive skipped.\n"
                    "  Clone https://github.com/CGutt-hub/AnalysisToolbox alongside "
                    "this repo, or set INTERACTIVE_PLOTTER_PATH.")

    all_ols, failed = [], []
    for cfg in country_list:
        df_ols = process_country(cfg, l1_dir, output_dir, plotter)
        if df_ols is None:
            failed.append(cfg["participant_id"])
        else:
            all_ols.append(df_ols)

    # L2 -- Group report (AT group_analyzer + policy table)
    log_info("-" * 60)
    log_info("Running group-level Volt report ...")
    policy = generate_volt_report(all_ols, l2_dir)

    # Register L2 vis.parquets with AT interactive_plotter
    l2_sidecar = os.path.join(l2_dir, "plots")
    for fname in [
        "LAV_displacement_epoch_displacement_grp_vis.parquet",
        "LAV_vulnerability_vis.parquet",
    ]:
        vis_path = os.path.join(l2_dir, fname)
        if os.path.exists(vis_path):
            register_vis(vis_path, output_dir,
                         fname.replace(".parquet", ""),
                         l2_sidecar, project="LAV", plotter=plotter)

    # Summary
    log_info("=" * 60)
    log_info("Pipeline complete.")
    log_info(f"Successful: {len(country_list) - len(failed)}/{len(country_list)}")
    if failed:
        log_warning(f"Failed: {', '.join(failed)}")
    log_info(f"Results  -> {output_dir}")
    log_info(f"HTML archive (AnalysisToolbox) -> {output_dir}/.bin/LAV_results.html")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-dir",   default="LAV_data",
                        help="LAV_data/ directory (default: LAV_data)")
    parser.add_argument("--output-dir", default="LAV_results",
                        help="Output root (default: LAV_results)")
    parser.add_argument("--countries",  default=None,
                        metavar="all|ISO2[,ISO2,...]",
                        help="'all' or comma-separated ISO-2 codes; "
                             "omit to use LAV_data/ configs or embedded list")
    args = parser.parse_args()
    main(args.data_dir, args.output_dir, args.countries)
