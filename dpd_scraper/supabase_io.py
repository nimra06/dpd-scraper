# dpd_scraper/supabase_io.py
import os, time, datetime as dt
from dotenv import load_dotenv
from supabase import create_client, Client
from .normalize import row_hash

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
ARTIFACTS_BUCKET = os.getenv("SUPABASE_ARTIFACTS_BUCKET", "dpd-artifacts")  # default

def sb() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def ensure_bucket(bucket: str) -> None:
    try:
        # list buckets; if not there, create
        buckets = sb().storage.list_buckets()
        names = {b.name for b in buckets}
        if bucket not in names:
            sb().storage.create_bucket(bucket, public=True)  # make it publicly readable
    except Exception:
        # older libs: try create and ignore if exists
        try:
            sb().storage.create_bucket(bucket, public=True)
        except Exception:
            pass

def upload_styled_xlsx(xlsx_path: str) -> str:
    """
    Uploads an XLSX file to Storage and returns a public URL.
    Assumes bucket exists or creates it.
    """
    ensure_bucket(ARTIFACTS_BUCKET)
    key = f"{time.strftime('%Y%m%d_%H%M%S')}/snapshot.xlsx"
    with open(xlsx_path, "rb") as f:
        sb().storage.from_(ARTIFACTS_BUCKET).upload(
            key, f,
            {"content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "upsert": "true"}
        )
    # get a public URL
    url = sb().storage.from_(ARTIFACTS_BUCKET).get_public_url(key)
    return url

def load_baseline() -> dict[str, dict]:
    rows = sb().table("dpd_baseline").select("din,row").execute().data or []
    return {r["din"]: r["row"] for r in rows}

def write_run(rows: list[dict], meta: dict, label: str = None, xlsx_url: str | None = None) -> int:
    payload = {
        "run_label": label,
        "total_rows": len(rows),
        "rows_json": rows,
        "meta": meta,
    }
    if xlsx_url:
        payload["xlsx_url"] = xlsx_url
        payload["columns_order"] = meta.get("columns_order")

    data = sb().table("dpd_runs").insert(payload).execute().data
    return data[0]["id"]
# ---------- Changes ----------
def stage_changes(run_id: int, added, removed, modified) -> None:
    """
    Bulk insert into dpd_changes:
      id, run_id, din, change_type ('added'|'removed'|'modified'), before (jsonb), after (jsonb), approved (bool default false)
    """
    payload = []
    for x in added:
        payload.append({
            "run_id": run_id,
            "din": x["din"],
            "change_type": "added",
            "before": None,
            "after": x["after"],
        })
    for x in removed:
        payload.append({
            "run_id": run_id,
            "din": x["din"],
            "change_type": "removed",
            "before": x["before"],
            "after": None,
        })
    for x in modified:
        payload.append({
            "run_id": run_id,
            "din": x["din"],
            "change_type": "modified",
            "before": x["before"],
            "after": x["after"],
        })
    if payload:
        sb().table("dpd_changes").insert(payload).execute()


# ---------- Apply approvals ----------
def apply_approved_changes_to_baseline_and_HC_products() -> None:
    """
    Applies approved changes to:
      1) dpd_baseline (upsert / delete)
      2) HC products (mirror fields; adjust mapping to your schema)
      3) change_logs (append audit row)

    Make sure the destination tables/columns exist.
    """
    s = sb()
    changes = s.table("dpd_changes").select("*").eq("approved", True).execute().data or []
    for ch in changes:
        din = ch["din"]
        typ = ch["change_type"]
        before = ch.get("before")
        after  = ch.get("after")

        # (1) dpd_baseline
        if typ == "removed":
            s.table("dpd_baseline").delete().eq("din", din).execute()
        else:
            s.table("dpd_baseline").upsert({
                "din": din,
                "row": after,
                "row_hash": row_hash(after),
            }, on_conflict="din").execute()

        # (2) HC products (adapt to your columns)
        if typ == "removed":
            s.table("HC products").delete().eq("DIN", din).execute()
        else:
            hc = {
                "DIN": din,
                "Product": after.get("Product"),
                "Company": after.get("Company"),
                "Class": after.get("Class"),
                "Schedule": after.get("Schedule"),
                "Dosage form": after.get("Dosage form"),
                "Route(s) of administration": after.get("Route(s) of administration"),
                "American Hospital Formulary Service (AHFS)": after.get("American Hospital Formulary Service (AHFS)"),
                "Anatomical Therapeutic Chemical (ATC)": after.get("Anatomical Therapeutic Chemical (ATC)"),
                "Active ingredient group (AIG) number": after.get("Active ingredient group (AIG) number"),
                "Labelling": after.get("Labelling"),
                "Product Monograph/Veterinary Date": after.get("Product Monograph/Veterinary Date"),
                "List of active ingredient": after.get("List of active ingredient"),
                "Biosimilar Biologic Drug": after.get("Biosimilar Biologic Drug") or "No",
                "LastUpdated": dt.datetime.utcnow().isoformat() + "Z",
            }
            s.table("HC products").upsert(hc, on_conflict="DIN").execute()

        # (3) change_logs
        s.table("change_logs").insert({
            "entity": "HC products",
            "entity_key": din,
            "change_type": typ.upper(),
            "before": before,
            "after": after,
            "changed_at": dt.datetime.utcnow().isoformat() + "Z",
        }).execute()
