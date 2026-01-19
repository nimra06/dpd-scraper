# scripts/enrich_from_list_csv.py
from __future__ import annotations
from typing import Dict, List, Tuple
import os, re, time, csv, threading
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# -----------------------
# Config via env (defaults work fine)
# -----------------------
IN_PATH  = os.getenv("IN_PATH",  "artifacts/checkpoints/dpd_list_054274_20250926_061748.csv")
OUT_PATH = os.getenv("OUT_PATH", "artifacts/checkpoints/dpd_enriched.csv")
CHECKPOINT_EVERY = int(os.getenv("ENRICH_CHECKPOINT_EVERY", "2000"))
CHECKPOINT_DIR   = os.getenv("ENRICH_CHECKPOINT_DIR", "artifacts/checkpoints/enriched")
MAX_WORKERS      = int(os.getenv("ENRICH_WORKERS", "4"))  # 1=sequential; 4..8 OK
TIMEOUT          = int(os.getenv("SCRAPER_TIMEOUT", "45"))
RETRIES          = int(os.getenv("SCRAPER_RETRIES", "4"))
RETRY_SLEEP      = float(os.getenv("SCRAPER_RETRY_SLEEP", "1.0"))
DEBUG            = os.getenv("SCRAPER_DEBUG_VERBOSE", "1") not in ("0", "", "false", "False")

BASE     = "https://health-products.canada.ca"
FORM_URL = f"{BASE}/dpd-bdpp/?lang=eng"

# -----------------------
# Columns (keep your order)
# -----------------------
DETAIL_COLS = [
    "Status","DIN URL","DIN","Company","Product","Class","PM See footnote1","Schedule",
    "# See footnote2","A.I. name See footnote3","Strength",
    "Current status date","Original market date",
    "Address","City","state","Country","Zipcode",
    "Number of active ingredient(s)","Biosimilar Biologic Drug",
    "American Hospital Formulary Service (AHFS)","Anatomical Therapeutic Chemical (ATC)",
    "Active ingredient group (AIG) number","Labelling","Product Monograph/Veterinary Date",
    "List of active ingredient","Dosage form","Route(s) of administration",
]

# -----------------------
# Debug helper
# -----------------------
def dbg(*args):
    if DEBUG:
        print("[DEBUG]", *args)

SPACES_RX = re.compile(r"[ \t\r\f\v]+")
DATE_RX   = re.compile(r"\b(19|20)\d{2}-\d{2}-\d{2}\b")

def norm(x: str | None) -> str:
    if not x: return ""
    x = x.replace("\xa0"," ").replace("\u200b"," ")
    return SPACES_RX.sub(" ", x).strip()

def _canon_din_display(v: str) -> str:
    s = "".join(ch for ch in str(v or "") if ch.isdigit())
    return s or (v or "")

# -----------------------
# Thread-local session (requests Session isn't strictly thread-safe to share)
# -----------------------
_tls = threading.local()
def get_session() -> requests.Session:
    s = getattr(_tls, "sess", None)
    if s is not None:
        return s
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; dpd-enricher/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": FORM_URL,
        "Connection": "keep-alive",
    })
    retry = Retry(
        total=RETRIES,
        backoff_factor=RETRY_SLEEP,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=64)
    sess.mount("https://", adapter); sess.mount("http://", adapter)
    _tls.sess = sess
    return sess

# -----------------------
# Robust Active Ingredients extraction
# -----------------------
def _extract_ai_lines(soup: BeautifulSoup) -> list[str]:
    """
    Returns lines like ["SODIUM CHLORIDE : 0.9 %", ...] from the detail page,
    supporting table caption, headers, definition blocks, bullet lists, and <br>-text.
    Handles EN + FR labels.
    """
    lines: list[str] = []

    def add(name: str, strength: str):
        name = norm(name); strength = norm(strength)
        if not name: return
        low = name.lower()
        if low in ("name","active ingredient","ingrédient actif","active ingredient(s)","ingrédient(s) actif(s)"):
            return
        # Filter footnote placeholders
        if "footnote" in low: 
            return
        lines.append(f"{name} : {strength}" if strength else name)

    # 1) Captioned table
    for cap in soup.find_all("caption"):
        cap_txt = norm(cap.get_text(" "))
        if "active ingredient" in cap_txt.lower() or "ingrédient actif" in cap_txt.lower():
            table = cap.find_parent("table")
            if table:
                for tr in table.find_all("tr"):
                    tds = tr.find_all(["td","th"])
                    if len(tds) >= 2:
                        add(tds[0].get_text(" "), tds[1].get_text(" "))
            if lines: return lines

    # 2) Any table whose header mentions it
    for table in soup.find_all("table"):
        head_txt = norm((table.find("thead") or table).get_text(" "))
        if "active ingredient" in head_txt.lower() or "ingrédient actif" in head_txt.lower():
            for tr in table.find_all("tr"):
                tds = tr.find_all(["td","th"])
                if len(tds) >= 2:
                    add(tds[0].get_text(" "), tds[1].get_text(" "))
            if lines: return lines

    # 3) Definition-style block <dt>/<dd> or <th>/<td>
    for dt_tag in soup.find_all(["dt","th"]):
        lab = norm(dt_tag.get_text(" "))
        if "active ingredient" in lab.lower() or "ingrédient actif" in lab.lower():
            dd = dt_tag.find_next("dd") or dt_tag.find_next("td")
            if dd:
                parts = [norm(li.get_text(" ")) for li in dd.find_all("li")]
                if not parts:
                    raw = dd.decode_contents()
                    parts = [norm(p) for p in re.split(r"<br\s*/?>|\n", raw, flags=re.I)]
                for p in parts:
                    if not p: 
                        continue
                    # split "NAME : STRENGTH"
                    if " : " in p:
                        nm, st = p.split(" : ", 1)
                        add(nm, st)
                    else:
                        # try two+ spaces split
                        m = re.match(r"^(.*?)[\s]{2,}(.*)$", p)
                        if m: add(m.group(1), m.group(2))
                        else: add(p, "")
            if lines: return lines

    # 4) Paragraph mentioning it with <br> separations
    for p in soup.find_all("p"):
        text = p.get_text("\n", strip=True)
        if "active ingredient" in text.lower() or "ingrédient actif" in text.lower():
            parts = [norm(x) for x in text.split("\n") if norm(x)]
            for x in parts:
                if " : " in x:
                    nm, st = x.split(" : ", 1)
                    add(nm, st)
                else:
                    add(x, "")
            if lines: return lines

    return lines

# -----------------------
# Detail parser
# -----------------------
def fetch_detail_fields(din_url: str) -> Dict[str, str]:
    out = {k: "" for k in DETAIL_COLS}
    if not din_url:
        return out

    try:
        sess = get_session()
        r = sess.get(din_url, timeout=TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        dbg(f"[DETAIL ERR] {din_url} :: {e!r}")
        return out

    def gr(label: str) -> str:
        lab = label.lower()
        # Each row looks like: <div class="row"><p class="col-sm-4"><strong>Label</strong></p><p class="col-sm-8">Value</p></div>
        for row in soup.select("div.row"):
            left = row.select_one("p.col-sm-4 strong")
            right = row.select_one("p.col-sm-8")
            if not left or not right: 
                continue
            if lab in left.get_text(" ").lower():
                return norm(right.get_text(" ").replace("\xa0", " "))
        return ""

    # Core fields (do NOT overwrite list "Company/Product" later)
    out["Status"] = gr("Status")
    out["Class"]  = gr("Class")
    out["Schedule"] = gr("Schedule")

    out["Current status date"]  = gr("Current status date")
    out["Original market date"] = gr("Original market date")
    out["Dosage form"] = gr("Dosage form")
    out["Route(s) of administration"] = gr("Route")
    out["Number of active ingredient(s)"] = gr("Number of active ingredient")
    out["American Hospital Formulary Service (AHFS)"] = gr("American Hospital Formulary Service")
    out["Anatomical Therapeutic Chemical (ATC)"] = gr("Anatomical Therapeutic Chemical")
    out["Active ingredient group (AIG) number"] = gr("Active ingredient group")

    # Address block (under the same "Company" section, but split into spans)
    comp_row = None
    for row in soup.select("div.row"):
        left = row.select_one("p.col-sm-4 strong")
        if left and "company" in left.get_text(" ").lower():
            comp_row = row
            break
    if comp_row:
        right = comp_row.select_one("p.col-sm-8")
        spans = [norm(s.get_text(" ")) for s in (right.select("span") if right else [])]
        # common pattern: [Address, City, Province/State, Country, Postal/Zip]
        if len(spans) >= 1: out["Address"] = spans[0]
        if len(spans) >= 2: out["City"]    = spans[1]
        if len(spans) >= 3: out["state"]   = spans[2]
        if len(spans) >= 4: out["Country"] = spans[3]
        if len(spans) >= 5: out["Zipcode"] = spans[4]

    # Labelling URL + date
    lab_url, lab_date = "", ""
    for row in soup.select("div.row"):
        left = row.select_one("p.col-sm-4 strong")
        right = row.select_one("p.col-sm-8")
        if not left or not right:
            continue
        label = left.get_text(" ", strip=True).lower()
        if any(key in label for key in (
            "product monograph/veterinary labelling",
            "product monograph", "labelling", "veterinary labelling"
        )):
            m = DATE_RX.search(right.get_text(" ", strip=True))
            if m: lab_date = m.group(0)
            a = right.find("a", href=True)
            if a and a["href"]:
                href = a["href"]
                lab_url = href if href.startswith("http") else urljoin(BASE, href)
            break
    out["Labelling"] = lab_url
    out["Product Monograph/Veterinary Date"] = lab_date

    # Active ingredient list (robust)
    ai_lines = _extract_ai_lines(soup)
    out["List of active ingredient"] = "\n".join(ai_lines)

    # If the list exists, populate single AI name/Strength if placeholders
    ai_name = out.get("A.I. name See footnote3", "").strip()
    strength = out.get("Strength", "").strip()
    if ai_lines:
        first = ai_lines[0]
        if " : " in first:
            nm, st = first.split(" : ", 1)
            if not ai_name or "footnote" in ai_name.lower() or ai_name.lower().startswith("active ingredient"):
                out["A.I. name See footnote3"] = nm.strip()
            if not strength or strength.lower() == "strength":
                out["Strength"] = st.strip()

    # Biosimilar (normalize to Yes/No)
    bs = gr("Biosimilar Biologic Drug")
    out["Biosimilar Biologic Drug"] = "Yes" if bs.lower().startswith("yes") else ("No" if bs else "No")

    # Done
    filled = sum(1 for v in out.values() if v)
    dbg(f"[DETAIL OK] {din_url} filled {filled}/{len(out)}")
    return out

# -----------------------
# Row merger (list → enriched)
# -----------------------
def merge_list_and_detail(list_row: dict, detail_row: dict) -> dict:
    merged = {k: "" for k in DETAIL_COLS}
    # Start with list values
    for k, v in (list_row or {}).items():
        if v is not None:
            merged[k] = str(v)
    # Never replace non-empty Company/Product from list with the detail "Company" block text
    # Only take the structured address/date/AI/ATC/etc. from detail
    for k, v in (detail_row or {}).items():
        if v:
            if k in ("Company", "Product"):
                # only fill if empty
                if not merged.get(k): merged[k] = v
            else:
                merged[k] = v
    # Ensure DIN normalized display
    merged["DIN"] = _canon_din_display(merged.get("DIN",""))
    # Ensure biosimilar default
    if not merged.get("Biosimilar Biologic Drug"):
        merged["Biosimilar Biologic Drug"] = "No"
    return merged

# -----------------------
# Enrich one row helper
# -----------------------
def enrich_one(list_row: dict) -> dict:
    din_url = (list_row.get("DIN URL") or "").strip()
    details = fetch_detail_fields(din_url) if din_url else {k:"" for k in DETAIL_COLS}
    return merge_list_and_detail(list_row, details)

# -----------------------
# Main
# -----------------------
def main():
    t0 = time.time()
    os.makedirs(os.path.dirname(OUT_PATH) or ".", exist_ok=True)
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    # Load list CSV (keep exactly as read; pandas is fine)
    print(f"[LOAD] {IN_PATH}")
    df = pd.read_csv(IN_PATH, dtype=str, keep_default_na=False)
    # Ensure all columns exist so downstream assignment is easy
    for c in DETAIL_COLS:
        if c not in df.columns:
            df[c] = ""

    # Work
    total = len(df)
    enriched_rows: List[dict] = []
    last_ckpt = 0

    # Optional threaded speedup
    if MAX_WORKERS <= 1:
        for i, row in enumerate(df.to_dict(orient="records"), 1):
            out = enrich_one(row)
            enriched_rows.append(out)
            if i % 100 == 0 or i == total:
                pct = 100.0 * i / max(1,total)
                print(f"[ENRICH] {i}/{total} ({pct:.1f}%) elapsed={time.time()-t0:.1f}s")
            # checkpoint
            if CHECKPOINT_EVERY and i - last_ckpt >= CHECKPOINT_EVERY:
                last_ckpt = i
                ck = pd.DataFrame(enriched_rows, columns=DETAIL_COLS)
                ck_path = os.path.join(CHECKPOINT_DIR, f"dpd_enriched_ckpt_{i:06d}.csv")
                ck.to_csv(ck_path, index=False)
                print(f"[CKPT] wrote {ck_path} rows={len(ck)}")
    else:
        # Threaded
        import concurrent.futures as cf
        records = df.to_dict(orient="records")
        with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = [ex.submit(enrich_one, r) for r in records]
            for i, fut in enumerate(cf.as_completed(futs), 1):
                try:
                    out = fut.result()
                except Exception as e:
                    out = {k:"" for k in DETAIL_COLS}
                    out.update(records[i-1])
                    dbg(f"[WORKER ERR] row {i}: {e!r}")
                enriched_rows.append(out)
                if i % 100 == 0 or i == total:
                    pct = 100.0 * i / max(1,total)
                    print(f"[ENRICH] {i}/{total} ({pct:.1f}%) elapsed={time.time()-t0:.1f}s")
                if CHECKPOINT_EVERY and i - last_ckpt >= CHECKPOINT_EVERY:
                    last_ckpt = i
                    ck = pd.DataFrame(enriched_rows, columns=DETAIL_COLS)
                    ck_path = os.path.join(CHECKPOINT_DIR, f"dpd_enriched_ckpt_{i:06d}.csv")
                    ck.to_csv(ck_path, index=False)
                    print(f"[CKPT] wrote {ck_path} rows={len(ck)}")

    # Write final
    out_df = pd.DataFrame(enriched_rows, columns=DETAIL_COLS)
    out_df.to_csv(OUT_PATH, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"[DONE] wrote {OUT_PATH} with {len(out_df)} rows in {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
