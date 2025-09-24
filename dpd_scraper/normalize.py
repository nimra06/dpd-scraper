import json, hashlib

CANON_KEYS = ["Status","DIN URL","DIN","Company","Product","Class","PM See footnote1","Schedule",
    "# See footnote2","A.I. name See footnote3","Strength",
    "Current status date","Original market date","Address","City","state","Country","Zipcode",
    "Number of active ingredient(s)","Biosimilar Biologic Drug",
    "American Hospital Formulary Service (AHFS)","Anatomical Therapeutic Chemical (ATC)",
    "Active ingredient group (AIG) number","Labelling","Product Monograph/Veterinary Date",
    "List of active ingredient","Dosage form","Route(s) of administration"]

def canon_row(row: dict) -> dict:
    out = {}
    for k in CANON_KEYS:
        v = row.get(k) or ""
        if isinstance(v, str):
            v = v.replace("\r\n","\n").replace("\r","\n").strip()
        out[k] = v
    if not out.get("Biosimilar Biologic Drug"):
        out["Biosimilar Biologic Drug"] = "No"
    return out

def row_hash(row: dict) -> str:
    s = json.dumps(canon_row(row), ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()
