# scripts/run_weekly_job.py
import os
import json
import time
import pandas as pd

from dpd_scraper.orchestrator import weekly_job

try:
    from dpd_scraper.dpd_scraper import DETAIL_COLS, save_styled_excel
except Exception:
    DETAIL_COLS = None
    save_styled_excel = None  

ART_DIR = os.environ.get("ARTIFACT_DIR", "artifacts")
RETURN_ROWS = os.environ.get("WEEKLY_RETURN_ROWS", "1") == "1"

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _write_json(obj, path: str) -> None:
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def _write_csv(rows, path: str) -> None:
    _ensure_dir(os.path.dirname(path))
    df = pd.DataFrame(rows if DETAIL_COLS is None else rows, columns=DETAIL_COLS)
    df.to_csv(path, index=False)

def _write_xlsx(rows, path: str) -> None:
    _ensure_dir(os.path.dirname(path))
    df = pd.DataFrame(rows if DETAIL_COLS is None else rows, columns=DETAIL_COLS)
    # Prefer styled writer if available
    if save_styled_excel is not None:
        save_styled_excel(df, path)
        return
    # Fallback: plain to_excel
    try:
        df.to_excel(path, index=False, engine="openpyxl")
    except Exception:
        df.to_excel(path, index=False)

def _write_artifacts(rows, meta, out_dir: str):
    json_path = os.path.join(out_dir, "snapshot.json")
    csv_path  = os.path.join(out_dir, "snapshot.csv")
    xlsx_path = os.path.join(out_dir, "snapshot.xlsx")

    _write_json({"meta": meta, "rows": rows}, json_path)
    _write_csv(rows, csv_path)
    _write_xlsx(rows, xlsx_path)

    return {"json": json_path, "csv": csv_path, "xlsx": xlsx_path}

def main():
    tstamp = time.strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.join(ART_DIR, tstamp)
    _ensure_dir(out_dir)

    if RETURN_ROWS:
        # ARTIFACT MODE: save styled XLSX + CSV + JSON
        rows, meta = weekly_job(return_rows=True)
        paths = _write_artifacts(rows, meta, out_dir)
        print({
            "mode": "artifacts-only",
            "saved_dir": out_dir,
            "paths": paths,
            "rows": len(rows),
            **meta,
        })
    else:
        res = weekly_job(return_rows=False)
        _write_json(res, os.path.join(out_dir, "run_meta.json"))
        print({
            "mode": "db-write",
            "saved_dir": out_dir,
            **res,
        })

if __name__ == "__main__":
    main()
