# scripts/run_weekly_job.py
import os, json, time
import pandas as pd
from dpd_scraper.orchestrator import weekly_job

ART_DIR = os.environ.get("ARTIFACT_DIR", "artifacts")

def main():
    tstamp = time.strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.join(ART_DIR, tstamp)
    os.makedirs(out_dir, exist_ok=True)

    # ask orchestrator for raw rows + meta so we can save files here
    rows, meta = weekly_job(return_rows=True)

    # JSON
    json_path = os.path.join(out_dir, "snapshot.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({"meta": meta, "rows": rows}, f, ensure_ascii=False, indent=2)

    # CSV
    csv_path = os.path.join(out_dir, "snapshot.csv")
    pd.DataFrame(rows).to_csv(csv_path, index=False)

    # XLSX (requires openpyxl installed)
    xlsx_path = os.path.join(out_dir, "snapshot.xlsx")
    try:
        pd.DataFrame(rows).to_excel(xlsx_path, index=False, engine="openpyxl")
    except Exception:
        # fallback without explicit engine (older pandas chooses engine automatically)
        pd.DataFrame(rows).to_excel(xlsx_path, index=False)

    print({
        "saved_dir": out_dir,
        "rows": len(rows),
        **meta
    })

if __name__ == "__main__":
    main()
