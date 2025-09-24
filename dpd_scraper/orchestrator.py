# dpd_scraper/orchestrator.py
import os, time, datetime as dt
from .supabase_io import load_baseline, write_run, stage_changes
from .diffing import compute_diff
from dpd_scraper.dpd_scraper import (
    run_full_scrape, DEF_REQUEST_SLEEP, DEF_MAX_DEPTH, DEF_TARGET_MIN_ROWS
)

REQUEST_SLEEP = DEF_REQUEST_SLEEP
MAX_DEPTH     = DEF_MAX_DEPTH
TARGET_MIN_ROWS = DEF_TARGET_MIN_ROWS
ENRICH_FLUSH_EVERY = 50

# NEW: hard cap from env (dev=5000, prod≈59000 or 0 for “no cap”)
SCRAPER_MAX_ROWS = int(os.getenv("SCRAPER_MAX_ROWS", "0"))

def weekly_job(return_rows: bool = False):
    started = time.time()

    rows, meta = run_full_scrape(
        max_depth=MAX_DEPTH,
        target_min_rows=TARGET_MIN_ROWS,
        enrich_flush_every=ENRICH_FLUSH_EVERY,
        request_sleep=REQUEST_SLEEP,
        max_rows=SCRAPER_MAX_ROWS,   # 👈 pass hard cap
    )

    if return_rows:
        return rows, {
            "strategy": meta.get("strategy"),
            "elapsed_sec": meta.get("elapsed_sec"),
            "rows": len(rows),
            "request_sleep": meta.get("request_sleep"),
            "max_depth": meta.get("max_depth"),
            "target_min_rows": meta.get("target_min_rows"),
            "max_rows": meta.get("max_rows"),
        }

    run_id = write_run(rows, meta, label=f"weekly-{dt.datetime.utcnow().strftime('%Y%m%d-%H%M%S')}")
    baseline = load_baseline()
    added, removed, modified = compute_diff(rows, baseline)
    stage_changes(run_id, added, removed, modified)

    return {
        "run_id": run_id,
        "rows": len(rows),
        "added": len(added),
        "removed": len(removed),
        "modified": len(modified),
        "elapsed_sec": round(time.time() - started, 1),
    }
