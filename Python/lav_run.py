"""
LAV Run: Standalone orchestrator for the Labour-AI-Volt analysis pipeline.

Executes the full analysis without requiring Nextflow or the AnalysisToolbox,
making it suitable for GitHub Actions CI and quick local runs.

Pipeline steps (per country):
  1. api_reader          – fetch World Bank labour data
  2. normalizing_processor – pivot to wide format
  3. displacement_analyzer – compute AI displacement scores
  4. trend_analyzer        – fit time-series trends

Group step (all countries):
  5. volt_report_analyzer  – cross-country synthesis & Volt policy metrics

Usage:
    python Python/lav_run.py [--data-dir LAV_data] [--output-dir LAV_results]

Results land in:  <output_dir>/LAV_l1/<participant_id>/
Group results in: <output_dir>/LAV_l2/
"""

import argparse
import glob
import importlib.util
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
# Helper: run a Python script in a subprocess
# ---------------------------------------------------------------------------
def run_script(script_path, args, cwd=None, env=None):
    """
    Run `python <script_path> <args...>` and stream output.

    Returns True on success, False on failure.
    """
    cmd = [sys.executable, "-u", script_path] + [str(a) for a in args]
    log_info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, env=env)
    if result.returncode != 0:
        log_error(f"Script failed (exit {result.returncode}): {script_path}")
        return False
    return True


# ---------------------------------------------------------------------------
# Script discovery: resolve paths relative to this file's location
# ---------------------------------------------------------------------------
HERE          = os.path.dirname(os.path.abspath(__file__))
READERS_DIR   = os.path.join(HERE, "readers")
PROCESSORS_DIR = os.path.join(HERE, "processors")
ANALYZERS_DIR = os.path.join(HERE, "analyzers")

API_READER          = os.path.join(READERS_DIR,    "api_reader.py")
NORMALIZER          = os.path.join(PROCESSORS_DIR, "normalizing_processor.py")
DISPLACEMENT_ANLZ   = os.path.join(ANALYZERS_DIR,  "displacement_analyzer.py")
TREND_ANLZ          = os.path.join(ANALYZERS_DIR,  "trend_analyzer.py")
VOLT_REPORT_ANLZ    = os.path.join(ANALYZERS_DIR,  "volt_report_analyzer.py")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def main(data_dir, output_dir):
    # Resolve absolute paths
    repo_root  = os.path.dirname(HERE)
    data_dir   = os.path.abspath(os.path.join(repo_root, data_dir))
    output_dir = os.path.abspath(os.path.join(repo_root, output_dir))

    l1_dir = os.path.join(output_dir, "LAV_l1")
    l2_dir = os.path.join(output_dir, "LAV_l2")
    os.makedirs(l1_dir, exist_ok=True)
    os.makedirs(l2_dir, exist_ok=True)

    log_info(f"Data directory  : {data_dir}")
    log_info(f"Output directory: {output_dir}")

    # Discover participant config files
    config_files = sorted(glob.glob(
        os.path.join(data_dir, "LAV_*", "LAV_*_config.json")
    ))
    if not config_files:
        log_error(f"No config files found in {data_dir}/LAV_*/")
        sys.exit(1)

    log_info(f"Discovered {len(config_files)} country configs")

    displacement_files = []
    trend_files        = []
    failed_participants = []

    for config_path in config_files:
        participant_id = os.path.basename(
            os.path.dirname(config_path)
        )  # e.g. "LAV_001"
        work_dir = os.path.join(l1_dir, participant_id)
        os.makedirs(work_dir, exist_ok=True)

        log_info(f"{'─'*60}")
        log_info(f"Processing participant: {participant_id}")
        log_info(f"Working directory:      {work_dir}")

        # ── Step 1: fetch data ────────────────────────────────────────────
        ok = run_script(API_READER, [config_path], cwd=work_dir)
        if not ok:
            log_warning(f"API fetch failed for {participant_id} — skipping")
            failed_participants.append(participant_id)
            continue

        raw_parquet = os.path.join(work_dir, f"{participant_id}_api_raw.parquet")
        if not os.path.exists(raw_parquet):
            log_warning(f"Expected output not found: {raw_parquet}")
            failed_participants.append(participant_id)
            continue

        # ── Step 2: normalize ─────────────────────────────────────────────
        ok = run_script(NORMALIZER, [raw_parquet], cwd=work_dir)
        if not ok:
            log_warning(f"Normalization failed for {participant_id} — skipping")
            failed_participants.append(participant_id)
            continue

        norm_parquet = os.path.join(work_dir, f"{participant_id}_normalized.parquet")
        if not os.path.exists(norm_parquet):
            log_warning(f"Expected output not found: {norm_parquet}")
            failed_participants.append(participant_id)
            continue

        # ── Step 3a: displacement analysis ────────────────────────────────
        ok = run_script(DISPLACEMENT_ANLZ, [norm_parquet], cwd=work_dir)
        if ok:
            disp_file = os.path.join(work_dir, f"{participant_id}_displacement.parquet")
            if os.path.exists(disp_file):
                displacement_files.append(disp_file)
            else:
                log_warning(f"Displacement output not found: {disp_file}")
        else:
            log_warning(f"Displacement analysis failed for {participant_id}")

        # ── Step 3b: trend analysis ───────────────────────────────────────
        ok = run_script(TREND_ANLZ, [norm_parquet], cwd=work_dir)
        if ok:
            trend_file = os.path.join(work_dir, f"{participant_id}_trends.parquet")
            if os.path.exists(trend_file):
                trend_files.append(trend_file)
            else:
                log_warning(f"Trends output not found: {trend_file}")
        else:
            log_warning(f"Trend analysis failed for {participant_id}")

    # ── Step 4: group-level Volt report ──────────────────────────────────
    log_info(f"{'─'*60}")
    log_info("Running group-level Volt report analysis …")

    all_group_inputs = displacement_files + trend_files
    if all_group_inputs:
        ok = run_script(VOLT_REPORT_ANLZ, all_group_inputs, cwd=l2_dir)
        if not ok:
            log_warning("Volt report analyzer returned an error")
    else:
        log_warning("No group inputs available — skipping Volt report")

    # ── Summary ───────────────────────────────────────────────────────────
    log_info(f"{'═'*60}")
    log_info(f"Pipeline complete.")
    log_info(f"Successful participants : {len(config_files) - len(failed_participants)}/{len(config_files)}")
    if failed_participants:
        log_warning(f"Failed participants    : {', '.join(failed_participants)}")
    log_info(f"Results → {output_dir}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="LAV Labour-AI-Volt analysis pipeline (standalone)"
    )
    parser.add_argument(
        "--data-dir",
        default="LAV_data",
        help="Directory containing per-country LAV_XXX/ subdirectories "
             "(default: LAV_data)",
    )
    parser.add_argument(
        "--output-dir",
        default="LAV_results",
        help="Root directory for pipeline outputs (default: LAV_results)",
    )
    args = parser.parse_args()
    main(args.data_dir, args.output_dir)
