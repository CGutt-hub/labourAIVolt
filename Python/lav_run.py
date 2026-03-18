"""
LAV Run — standalone orchestrator (no Nextflow, no subprocesses).

Calls lav_pipeline functions directly in-process for maximum efficiency:
  api_reader → normalize → displacement_analyzer + trend_analyzer
  → volt_report_analyzer → plots (vis.parquet + standalone HTML)

Run modes
─────────
  # Zero-config: uses the embedded Volt country list, auto year-end
  python Python/lav_run.py --countries all

  # Specific countries (ISO-2 codes)
  python Python/lav_run.py --countries DE,FR,NL

  # With LAV_data/ config files (original behaviour)
  python Python/lav_run.py

Results land in:  <output_dir>/LAV_l1/<participant_id>/
Group results in: <output_dir>/LAV_l2/
HTML plots in:    <output_dir>/LAV_l1/<id>/plots/  and  <output_dir>/LAV_l2/plots/
"""

import argparse
import glob
import json
import os
import sys

# Allow import from the Python/ package root regardless of cwd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lav_pipeline import (
    fetch_country, normalize, analyze_displacement, analyze_trends,
    generate_volt_report, _concat_frames,
    make_sector_employment_vis, make_unemployment_vis,
    make_displacement_score_vis, make_group_adpi_vis, make_vulnerability_vis,
    write_country_html_plots, write_group_html_plots,
    VOLT_COUNTRIES, _VOLT_BY_ISO2,
    log_info, log_warning, log_error,
)

# ── Country resolution ────────────────────────────────────────────────────────

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
                log_warning(f"ISO-2 '{iso2}' not found — skipping")
        return result

    if dir_configs:
        log_info(f"Using {len(dir_configs)} country configs from {data_dir}")
        return list(dir_configs.values())

    log_info("No config files found — using embedded Volt country list (zero-config mode)")
    return [dict(c) for c in VOLT_COUNTRIES]


# ── Per-country L1 pipeline ───────────────────────────────────────────────────

def process_country(cfg, l1_dir):
    """Run full L1 pipeline for one country. Returns (df_disp, df_trends) or (None, None)."""
    pid      = cfg["participant_id"]
    work_dir = os.path.join(l1_dir, pid)
    os.makedirs(work_dir, exist_ok=True)

    log_info("─" * 60)
    log_info(f"Processing: {cfg['country']} ({cfg['iso2']})  [{pid}]")

    # Step 1 — Fetch
    df_raw = fetch_country(pid, cfg["country"], cfg["iso3"], cfg["iso2"],
                           year_start=cfg.get("year_start", 2000))
    if len(df_raw) == 0:
        log_warning(f"No data fetched for {pid} — skipping")
        return None, None
    df_raw.write_parquet(os.path.join(work_dir, f"{pid}_api_raw.parquet"),
                         compression="snappy")

    # Step 2 — Normalize
    df_wide = normalize(df_raw)
    if len(df_wide) == 0:
        log_warning(f"Normalization produced empty result for {pid} — skipping")
        return None, None
    df_wide.write_parquet(os.path.join(work_dir, f"{pid}_normalized.parquet"),
                          compression="snappy")

    # Step 3a — Displacement analysis
    df_disp = analyze_displacement(df_wide)
    if len(df_disp) > 0:
        df_disp.write_parquet(os.path.join(work_dir, f"{pid}_displacement.parquet"),
                              compression="snappy")
        # vis.parquet for AnalysisToolbox
        vis_emp  = make_sector_employment_vis(df_wide)
        vis_disp = make_displacement_score_vis(df_disp)
        if vis_emp:
            vis_emp.write_parquet(
                os.path.join(work_dir, f"{pid}_sector_employment_vis.parquet"),
                compression="snappy")
        if vis_disp:
            vis_disp.write_parquet(
                os.path.join(work_dir, f"{pid}_displacement_score_vis.parquet"),
                compression="snappy")

    # Step 3b — Trend analysis
    df_trends = analyze_trends(df_wide)
    if len(df_trends) > 0:
        df_trends.write_parquet(os.path.join(work_dir, f"{pid}_trends.parquet"),
                                compression="snappy")
        vis_unemp = make_unemployment_vis(df_wide)
        if vis_unemp:
            vis_unemp.write_parquet(
                os.path.join(work_dir, f"{pid}_unemployment_vis.parquet"),
                compression="snappy")

    # HTML plots — each year as a dot (lines+markers)
    write_country_html_plots(df_wide, df_disp if len(df_disp) > 0 else None,
                             work_dir, pid)

    return (df_disp   if len(df_disp)   > 0 else None,
            df_trends if len(df_trends) > 0 else None)


# ── Main ──────────────────────────────────────────────────────────────────────

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
        log_error("No countries to process — exiting.")
        sys.exit(1)
    log_info(f"Countries to process: {len(country_list)}")

    all_disp, all_trends, failed = [], [], []
    for cfg in country_list:
        df_disp, df_trends = process_country(cfg, l1_dir)
        if df_disp is None and df_trends is None:
            failed.append(cfg["participant_id"])
        if df_disp   is not None: all_disp.append(df_disp)
        if df_trends is not None: all_trends.append(df_trends)

    # L2 — Group report
    log_info("─" * 60)
    log_info("Running group-level Volt report …")
    policy = generate_volt_report(all_disp, all_trends, l2_dir)

    # L2 vis.parquet + HTML plots
    df_disp_all = _concat_frames(all_disp)
    vis_adpi = make_group_adpi_vis(df_disp_all)
    vis_vuln = make_vulnerability_vis(policy if len(policy) > 0 else None)
    if vis_adpi:
        vis_adpi.write_parquet(os.path.join(l2_dir, "LAV_cross_country_adpi_vis.parquet"),
                               compression="snappy")
    if vis_vuln:
        vis_vuln.write_parquet(os.path.join(l2_dir, "LAV_vulnerability_vis.parquet"),
                               compression="snappy")
    write_group_html_plots(df_disp_all,
                           policy if len(policy) > 0 else None,
                           l2_dir)

    # Summary
    log_info("═" * 60)
    log_info("Pipeline complete.")
    log_info(f"Successful: {len(country_list) - len(failed)}/{len(country_list)}")
    if failed:
        log_warning(f"Failed: {', '.join(failed)}")
    log_info(f"Results  → {output_dir}")
    log_info(f"HTML plots:")
    for country_cfg in country_list:
        pid = country_cfg["participant_id"]
        log_info(f"  {l1_dir}/{pid}/plots/")
    log_info(f"  {l2_dir}/plots/")


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
