import os, time, json, datetime as dt
from dotenv import load_dotenv
from supabase import create_client, Client
from .normalize import row_hash

load_dotenv()
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
ARTIFACTS_BUCKET = os.getenv("SUPABASE_ARTIFACTS_BUCKET", "dpd-artifacts")
RUN_ROWS_MODE = (os.getenv("RUN_ROWS_MODE", "json") or "json").lower()  # 'json' | 'storage'

def sb() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def ensure_bucket(bucket: str) -> None:
    try:
        buckets = sb().storage.list_buckets()  # may raise if missing permission
        names = {getattr(b, "name", getattr(b, "id", "")) for b in buckets}
        if bucket not in names:
            sb().storage.create_bucket(bucket, public=True)
    except Exception:
        try:
            sb().storage.create_bucket(bucket, public=True)
        except Exception:
            pass

def _upload_text_to_storage(text: str, key: str) -> str:
    ensure_bucket(ARTIFACTS_BUCKET)
    sb().storage.from_(ARTIFACTS_BUCKET).upload(
        key, text.encode("utf-8"),
        {"content-type": "application/json; charset=utf-8", "upsert": "true"}
    )
    return sb().storage.from_(ARTIFACTS_BUCKET).get_public_url(key)

def upload_styled_xlsx(xlsx_path: str) -> str:
    ensure_bucket(ARTIFACTS_BUCKET)
    key = f"{time.strftime('%Y%m%d_%H%M%S')}/snapshot.xlsx"
    with open(xlsx_path, "rb") as f:
        sb().storage.from_(ARTIFACTS_BUCKET).upload(
            key, f,
            {"content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "upsert": "true"}
        )
    return sb().storage.from_(ARTIFACTS_BUCKET).get_public_url(key)

def load_baseline() -> dict[str, dict]:
    rows = sb().table("dpd_baseline").select("din,row").execute().data or []
    return {r["din"]: r["row"] for r in rows}

def write_run(rows: list[dict], meta: dict, label: str | None = None, xlsx_url: str | None = None) -> int:
    """
    Inserts a dpd_runs row. Compatible with:
      - schema that has NOT NULL rows_json (RUN_ROWS_MODE=json)
      - schema that uses rows_json_url (RUN_ROWS_MODE=storage)
    """
    payload = {
        "run_label": label,
        "total_rows": len(rows),
        "meta": meta,
    }
    if xlsx_url:
        payload["xlsx_url"] = xlsx_url
        if "columns_order" in meta:
            payload["columns_order"] = meta.get("columns_order")

    if RUN_ROWS_MODE == "storage":
        # upload rows JSON and save URL
        key = f"{time.strftime('%Y%m%d_%H%M%S')}/rows.json"
        url = _upload_text_to_storage(json.dumps(rows, ensure_ascii=False), key)
        payload["rows_json_url"] = url
    else:
        # default: inline JSON (most compatible)
        payload["rows_json"] = rows

    # do the insert
    res = sb().table("dpd_runs").insert(payload).execute()
    data = getattr(res, "data", None) or []
    return data[0]["id"]

# ---------- Changes ----------
def stage_changes(run_id: int, added, removed, modified) -> None:
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
    s = sb()
    changes = s.table("dpd_changes").select("*").eq("approved", True).execute().data or []
    for ch in changes:
        din = ch["din"]; typ = ch["change_type"]
        before = ch.get("before"); after = ch.get("after")

        if typ == "removed":
            s.table("dpd_baseline").delete().eq("din", din).execute()
            s.table("HC products").delete().eq("DIN", din).execute()
        else:
            s.table("dpd_baseline").upsert({
                "din": din,
                "row": after,
                "row_hash": row_hash(after),
            }, on_conflict="din").execute()
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

        s.table("change_logs").insert({
            "entity": "HC products",
            "entity_key": din,
            "change_type": typ.upper(),
            "before": before,
            "after": after,
            "changed_at": dt.datetime.utcnow().isoformat() + "Z",
        }).execute()
