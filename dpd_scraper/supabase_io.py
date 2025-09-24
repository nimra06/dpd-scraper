import os, datetime as dt
from dotenv import load_dotenv

from supabase import create_client, Client
from .normalize import row_hash
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

def sb() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def load_baseline() -> dict[str, dict]:
    rows = sb().table("dpd_baseline").select("din,row").execute().data or []
    return {r["din"]: r["row"] for r in rows}

def write_run(rows: list[dict], meta: dict, label: str = None) -> int:
    data = sb().table("dpd_runs").insert({
        "run_label": label,
        "total_rows": len(rows),
        "rows_json": rows,
        "meta": meta,
    }).execute().data
    return data[0]["id"]

def stage_changes(run_id: int, added, removed, modified):
    payload = []
    for x in added:
        payload.append({"run_id": run_id, "din": x["din"], "change_type":"added","before":None,"after":x["after"]})
    for x in removed:
        payload.append({"run_id": run_id, "din": x["din"], "change_type":"removed","before":x["before"],"after":None})
    for x in modified:
        payload.append({"run_id": run_id, "din": x["din"], "change_type":"modified","before":x["before"],"after":x["after"]})
    if payload:
        sb().table("dpd_changes").insert(payload).execute()

def apply_approved_changes_to_baseline_and_HC_products():
    s = sb()
    changes = s.table("dpd_changes").select("*").eq("approved", True).execute().data or []
    for ch in changes:
        din = ch["din"]
        typ = ch["change_type"]
        before = ch.get("before")
        after  = ch.get("after")

        # 1) update dpd_baseline
        if typ == "removed":
            s.table("dpd_baseline").delete().eq("din", din).execute()
        else:
            s.table("dpd_baseline").upsert({
                "din": din,
                "row": after,
                "row_hash": row_hash(after),
            }, on_conflict="din").execute()

        # 2) mirror to HC products (adapt field mapping here if your columns differ)
        # Example mapping (adjust to your actual columns in "HC products"):
        if typ == "removed":
            s.table("HC products").delete().eq("DIN", din).execute()
        else:
            # Minimal upsert example:
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

        # 3) append to change_logs (shape may differ – adapt keys to your table)
        s.table("change_logs").insert({
          "entity": "HC products",
          "entity_key": din,
          "change_type": typ.upper(),
          "before": before,
          "after": after,
          "changed_at": dt.datetime.utcnow().isoformat() + "Z"
        }).execute()
