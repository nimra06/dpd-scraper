import os, time, datetime as dt
import pandas as pd

from .supabase_io import load_baseline, write_run, stage_changes, upload_styled_xlsx
from .diffing import compute_diff
from dpd_scraper.dpd_scraper import (
    run_full_scrape,
    DEF_REQUEST_SLEEP, DEF_MAX_DEPTH, DEF_TARGET_MIN_ROWS, DETAIL_COLS,
)
# If you moved the Excel helper elsewhere, adjust import:

from dpd_scraper.dpd_scraper import save_styled_excel  # uses the one from scraper module

REQUEST_SLEEP = DEF_REQUEST_SLEEP
MAX_DEPTH     = DEF_MAX_DEPTH
TARGET_MIN_ROWS = DEF_TARGET_MIN_ROWS
ENRICH_FLUSH_EVERY = int(os.getenv("SCRAPER_ENRICH_FLUSH_EVERY", "50"))
SCRAPER_MAX_ROWS   = int(os.getenv("SCRAPER_MAX_ROWS", "0"))

def weekly_job(return_rows: bool = False):
    started = time.time()

    rows, meta = run_full_scrape(
        max_depth=MAX_DEPTH,
        target_min_rows=TARGET_MIN_ROWS,
        enrich_flush_every=ENRICH_FLUSH_EVERY,
        request_sleep=REQUEST_SLEEP,
        max_rows=SCRAPER_MAX_ROWS,
    )

    # Always include ordered columns in meta (downstream expects this)
    meta["columns_order"] = DETAIL_COLS

    if return_rows:
        return rows, {
            "strategy": meta.get("strategy"),
            "elapsed_sec": meta.get("elapsed_sec"),
            "rows": len(rows),
            "request_sleep": meta.get("request_sleep"),
            "max_depth": meta.get("max_depth"),
            "target_min_rows": meta.get("target_min_rows"),
            "max_rows": meta.get("max_rows"),
            "columns_order": DETAIL_COLS,
        }

    # Local styled XLSX artifact
    out_dir = os.getenv("ARTIFACT_DIR", "artifacts")
    ts = time.strftime("%Y%m%d_%H%M%S")
    os.makedirs(os.path.join(out_dir, ts), exist_ok=True)
    xlsx_path = os.path.join(out_dir, ts, "snapshot.xlsx")
    df = pd.DataFrame(rows, columns=DETAIL_COLS)
    save_styled_excel(df, xlsx_path)

    # Upload xlsx to Storage (best-effort)
    xlsx_url = None
    try:
        xlsx_url = upload_styled_xlsx(xlsx_path)
    except Exception as e:
        print("[weekly_job] XLSX upload failed:", repr(e))

    # Write run (resilient to schema diffs)
    run_id = write_run(
        rows=rows,
        meta=meta,
        label=f"weekly-{dt.datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
        xlsx_url=xlsx_url,
    )

    # Diff vs baseline and stage changes
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
        "xlsx_url": xlsx_url,
    }
