"""
api_reader — thin CLI wrapper for Nextflow / manual use.
Core logic lives in Python/lav_pipeline.py.

Usage:
  # Form 1: JSON config file (Nextflow / legacy)
  python api_reader.py LAV_001_config.json

  # Form 2: inline flags — no config file needed
  python api_reader.py --iso2 DE --iso3 DEU --country Germany --participant-id LAV_001

  # Omit --year-end to auto-detect the latest published year from the API.
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lav_pipeline import (fetch_country, DEFAULT_YEAR_START,
                           log_info, log_warning, log_error)

TAG = "api_reader"


def _parse():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("config", nargs="?", default=None,
                   help="Path to LAV_XXX_config.json (optional if flags given)")
    p.add_argument("--iso2",           default=None)
    p.add_argument("--iso3",           default=None)
    p.add_argument("--country",        default=None)
    p.add_argument("--participant-id", default=None, dest="participant_id")
    p.add_argument("--year-start",     default=None, type=int, dest="year_start")
    p.add_argument("--year-end",       default=None, type=int, dest="year_end")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse()
    try:
        if args.config:
            log_info(f"Loading config: {args.config}", TAG)
            with open(args.config, "r", encoding="utf-8") as fh:
                cfg = json.load(fh)
            participant_id = cfg["participant_id"]
            country_name   = cfg["country"]
            iso3           = cfg["iso3"]
            iso2           = cfg["iso2"]
            year_start     = args.year_start or cfg.get("year_start", DEFAULT_YEAR_START)
            year_end_ovr   = args.year_end   or cfg.get("year_end", None)
        else:
            missing = [f for f, v in [("--iso2", args.iso2), ("--iso3", args.iso3),
                                       ("--country", args.country),
                                       ("--participant-id", args.participant_id)] if not v]
            if missing:
                log_error(f"Missing: {', '.join(missing)}. "
                          "Provide a config file or all flags.", TAG)
                sys.exit(1)
            participant_id = args.participant_id
            country_name   = args.country
            iso3           = args.iso3
            iso2           = args.iso2
            year_start     = args.year_start or DEFAULT_YEAR_START
            year_end_ovr   = args.year_end

        df = fetch_country(participant_id, country_name, iso3, iso2,
                           year_start=year_start, year_end_override=year_end_ovr)
        out = f"{participant_id}_api_raw.parquet"
        df.write_parquet(out, compression="snappy")
        log_info(f"Saved {len(df)} records → {out}", TAG)
    except Exception as exc:
        log_error(f"Fatal: {exc}", TAG)
        import traceback; traceback.print_exc()
        sys.exit(1)
