#!/usr/bin/env python3
"""
Sync partial results from checkpoint CSVs to Supabase.
Used when the scraper times out (exit 124) so we still persist scraped data.
"""
from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

# Path setup
_scripts = Path(__file__).resolve().parent
_root = _scripts.parent
sys.path.insert(0, str(_root))
sys.path.insert(0, str(_scripts))

from run_monthly_sync import sync_new_records


def main() -> int:
    raw = os.getenv("SCRAPER_CHECKPOINT_DIR")
    if raw:
        checkpoint_dir = Path(raw).expanduser()
    else:
        checkpoint_dir = _root / "artifacts" / "checkpoints"
    if not checkpoint_dir.is_absolute():
        checkpoint_dir = _root / checkpoint_dir

    files = sorted(checkpoint_dir.glob("dpd_*.csv"), key=lambda p: p.stat().st_mtime)
    if not files:
        print("No checkpoint files found.", flush=True)
        return 0

    latest = files[-1]
    print(f"Loading from {latest} ({latest.stat().st_size} bytes)", flush=True)
    with open(latest, "r", encoding="utf-8") as f:
        products = list(csv.DictReader(f))
    print(f"Loaded {len(products)} products.", flush=True)

    if not products:
        print("Checkpoint empty, nothing to sync.", flush=True)
        return 0

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        print("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set.", flush=True)
        return 1

    print("Syncing to Supabase (duplicates will be skipped)...", flush=True)
    sync_new_records(
        products,
        url,
        key,
        "nexara_all_source",
        batch_size=500,
    )
    print("Partial sync completed successfully.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
