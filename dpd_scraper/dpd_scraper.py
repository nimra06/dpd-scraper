# dpd_scraper/dpd_scraper.py
from __future__ import annotations
from typing import Optional, List, Dict, Tuple, Any

import os, time, re, json, string
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv

import requests
from requests.exceptions import ReadTimeout, ConnectTimeout, Timeout, HTTPError
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
import pandas as pd

load_dotenv()

# ----------------------------------
# Constants & env-driven config
# ----------------------------------
BASE          = "https://health-products.canada.ca"
FORM_URL      = f"{BASE}/dpd-bdpp/?lang=eng"
DISPATCH_URL  = f"{BASE}/dpd-bdpp/dispatch-repartition"           # GET only (don’t POST here)
RESULTS_URL   = f"{BASE}/dpd-bdpp/search-fast-recherche-rapide"   # HTML list view (DataTables)
SEARCH_URL    = f"{BASE}/dpd-bdpp/search-recherche"               # POST target to set filters
GET_PAGE_API  = f"{BASE}/dpd-bdpp/getNextPage"                    # DT JSON endpoint

TIMEOUT       = int(os.getenv("SCRAPER_TIMEOUT", "45"))
RETRIES       = int(os.getenv("SCRAPER_RETRIES", "2"))
RETRY_SLEEP   = float(os.getenv("SCRAPER_RETRY_SLEEP", "1.0"))

DEF_MAX_DEPTH          = int(os.getenv("SCRAPER_MAX_DEPTH", "1"))
DEF_TARGET_MIN_ROWS    = int(os.getenv("SCRAPER_TARGET_MIN_ROWS", "2000"))
DEF_ENRICH_FLUSH_EVERY = int(os.getenv("SCRAPER_ENRICH_FLUSH_EVERY", "50"))
DEF_REQUEST_SLEEP      = float(os.getenv("SCRAPER_REQUEST_SLEEP", "0.05"))
DEF_MAX_ROWS           = int(os.getenv("SCRAPER_MAX_ROWS", "0"))  # 0 = no cap

# Behavior toggles
SCRAPER_BASELINE_PROBE = os.getenv("SCRAPER_BASELINE_PROBE", "0") == "1"
SCRAPER_FORCE_POST_SWEEPS = os.getenv("SCRAPER_FORCE_POST_SWEEPS", "1") == "1"

# Sweep paging
SWEEP_PAGE_LIMIT       = int(os.getenv("SCRAPER_SWEEP_PAGE_LIMIT", "120"))
PAGE_STALL_LIMIT       = int(os.getenv("SCRAPER_PAGE_STALL_LIMIT", "2"))
SCRAPER_SWEEP_MAX_EMPTY= int(os.getenv("SCRAPER_SWEEP_MAX_EMPTY", "0"))   # 0 = never early-stop group
SCRAPER_SWEEP_ORDER    = (os.getenv("SCRAPER_SWEEP_ORDER", "brand-first") or "brand-first").strip().lower()

# Progress / logging
DEBUG                  = True
DEBUG_VERBOSE          = int(os.getenv("SCRAPER_DEBUG_VERBOSE", "1"))   # 0=quiet, 1=normal, 2=chatty
LOG_EVERY_ADDED        = int(os.getenv("SCRAPER_LOG_EVERY_ADDED", "50"))
SWEEP_PREFIX_LOG_EVERY = int(os.getenv("SCRAPER_SWEEP_PREFIX_LOG_EVERY", "1"))

# HTML fallback page keys
PAGE_KEYS = [k.strip() for k in os.getenv(
    "SCRAPER_PAGE_KEYS", "results_page,page,p,start,iDisplayStart"
).split(",") if k.strip()]

_t0_global = time.time()
_last_beat = {"count": 0, "t": _t0_global}

def dbg(*args):
    if DEBUG:
        print("[DEBUG]", *args)

# ----------------------------------
# Columns (kept in your exact order)
# ----------------------------------
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

COLUMNS = DETAIL_COLS[:]  # same order

# ----------------------------------
# Utilities
# ----------------------------------
SPACES_RX = re.compile(r"[ \t\r\f\v]+")
def norm(x: str | None) -> str:
    if not x:
        return ""
    x = x.replace("\xa0", " ").replace("\u200b", " ")
    return SPACES_RX.sub(" ", x).strip()

def _canon_din(d: str) -> str:
    # DINs are numeric; remove all non-digits
    return "".join(ch for ch in (d or "") if ch.isdigit())

def _canon_din_display(v: str) -> str:
    s = "".join(ch for ch in str(v or "") if ch.isdigit())
    return s or (v or "")

def _heartbeat(cum_rows: int, cap: int | None):
    if LOG_EVERY_ADDED <= 0:
        return
    if cum_rows >= _last_beat["count"] + LOG_EVERY_ADDED:
        now = time.time()
        dt = max(1e-6, now - _last_beat["t"])
        rate = (cum_rows - _last_beat["count"]) / dt
        elapsed = now - _t0_global
        msg = f"rows={cum_rows}"
        if cap:
            rem = max(0, cap - cum_rows)
            eta = rem / rate if rate > 0 else float("inf")
            msg += f" / cap={cap} | rate={rate:.1f}/s | elapsed={elapsed:.1f}s | eta~{eta:.1f}s"
        else:
            msg += f" | rate={rate:.1f}/s | elapsed={elapsed:.1f}s"
        dbg("[PROGRESS]", msg)
        _last_beat["count"] = cum_rows
        _last_beat["t"] = now

def _log_prefix_try(kind: str, val: str, ep_name: str, page: int, got: int, cum: int):
    if DEBUG_VERBOSE >= 2 or page == 1:
        dbg(f"[{kind}='{val}'] {ep_name} p{page}: {got} rows (cum={cum})")

# ----------------------------------
# HTTP/session
# ----------------------------------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; dpd-scraper/1.3)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": FORM_URL,
        "Connection": "keep-alive",
    })
    retry = Retry(
        total=RETRIES,
        backoff_factor=RETRY_SLEEP,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET","POST"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=64)
    s.mount("https://", adapter); s.mount("http://", adapter)
    return s

def _with_retries(fn):
    last = None
    for i in range(RETRIES):
        try:
            r = fn()
            r.raise_for_status()
            return r
        except (ReadTimeout, ConnectTimeout, Timeout, HTTPError, requests.exceptions.RequestException) as e:
            last = e
            time.sleep(RETRY_SLEEP * (i + 1))
    if last:
        raise last

def _fetch_page(sess: requests.Session, url: str, method: str, payload: dict,
                headers: dict | None = None) -> tuple[str, str]:
    method = (method or "GET").upper()
    if method == "POST":
        r = _with_retries(lambda: sess.post(url, data=payload, timeout=TIMEOUT, allow_redirects=True, headers=headers))
    else:
        r = _with_retries(lambda: sess.get(url, params=payload, timeout=TIMEOUT, allow_redirects=True, headers=headers))
    return r.text, method

# ----------------------------------
# Form bootstrap
# ----------------------------------
def get_csrf(sess: requests.Session) -> str:
    r = _with_retries(lambda: sess.get(FORM_URL, timeout=TIMEOUT))
    soup = BeautifulSoup(r.text, "html.parser")
    el = soup.select_one("input#_csrf, input[name=_csrf]")
    return el["value"] if el and el.get("value") else ""

def _discover_form(sess: requests.Session) -> tuple[str, dict]:
    r = _with_retries(lambda: sess.get(FORM_URL, timeout=TIMEOUT, allow_redirects=True))
    soup = BeautifulSoup(r.text, "html.parser")

    def is_dpd_form(form) -> bool:
        names = {inp.get("name","") for inp in form.find_all("input")}
        expected = {"brandName","din","companyName","activeIngredient","aigNumber","biosimDrugSearch"}
        return any(k in names for k in expected)

    forms = soup.find_all("form")
    form = None
    for f in forms:
        if is_dpd_form(f):
            form = f; break
    if not form and forms:
        form = forms[0]
    if not form:
        # fallback: still allow a POST to SEARCH_URL with defaults
        return SEARCH_URL, {}

    action = form.get("action") or SEARCH_URL
    action_url = urljoin(BASE, action)

    # avoid GC relay
    if "canada.ca/en/sr/srb.html" in action_url:
        action_url = SEARCH_URL

    defaults = {}
    for inp in form.find_all("input"):
        name = inp.get("name")
        if not name: continue
        t = (inp.get("type") or "").lower()
        if t in ("checkbox","radio"):
            if inp.has_attr("checked"):
                defaults.setdefault(name, inp.get("value", "on"))
        else:
            defaults.setdefault(name, inp.get("value", ""))

    if "_csrf" not in defaults:
        csrf = ""
        try:
            csrf = get_csrf(sess)
        except Exception:
            pass
        if csrf:
            defaults["_csrf"] = csrf

    return action_url, defaults

def submit_search(sess: requests.Session, basic_html: bool = False) -> tuple[str, dict, str]:
    """
    Returns (first_html, base_filters, post_url_for_p1).
    We’ll use post_url_for_p1 as the target for POSTing page1 shard filters.
    """
    post_url, defaults = _discover_form(sess)

    base_filters = {
        "din": "", "atc": "", "companyName": "", "brandName": "",
        "activeIngredient": "", "aigNumber": "", "biosimDrugSearch": "",
    }
    payload = {**defaults, **base_filters}
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": BASE,
        "Referer": FORM_URL,
    }

    # First load
    try:
        r = _with_retries(lambda: sess.post(post_url, data=payload, timeout=TIMEOUT, allow_redirects=True, headers=headers))
        if "canada.ca/en/sr/srb.html" in (r.url or ""):
            raise HTTPError("SRB relay after POST")
        return r.text, base_filters, (SEARCH_URL if post_url.endswith("/dispatch-repartition") else post_url)
    except Exception:
        # last resort: GET listing
        r = _with_retries(lambda: sess.get(RESULTS_URL, params={"lang":"eng","wbdisable":"true"} if basic_html else {"lang":"eng"}, timeout=TIMEOUT))
        return r.text, base_filters, SEARCH_URL

# ----------------------------------
# Results list parsing
# ----------------------------------
def parse_list_page_rows(html: str) -> list[dict]:
    soup = BeautifulSoup(html or "", "html.parser")
    table = soup.find("table", id="results")
    if not table:
        return []
    tbody = table.find("tbody")
    if not tbody:
        return []

    rows_out: list[dict] = []
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 10:
            continue
        status    = tds[0].get_text(strip=True)
        din_cell  = tds[1]
        company   = tds[2].get_text(strip=True)
        product   = tds[3].get_text(strip=True)
        drugclass = tds[4].get_text(strip=True)
        pm        = tds[5].get_text(strip=True)
        schedule  = tds[6].get_text(strip=True)
        ai_num    = tds[7].get_text(strip=True)
        ai_name   = tds[8].get_text(strip=True)
        strength  = tds[9].get_text(strip=True)

        a_tag    = din_cell.find("a", href=True)
        din_text = (a_tag.get_text(strip=True) if a_tag else din_cell.get_text(strip=True))
        din_href = (a_tag["href"] if a_tag else "")
        din_url  = urljoin(BASE, din_href)

        rows_out.append({
            "Status": status,
            "DIN URL": din_url,
            "DIN": _canon_din_display(din_text),
            "Company": company,
            "Product": product,
            "Class": drugclass,
            "PM See footnote1": pm,
            "Schedule": schedule,
            "# See footnote2": ai_num,
            "A.I. name See footnote3": ai_name,
            "Strength": strength,
        })
    return rows_out

def _detect_table_paging(html: str) -> tuple[str, int]:
    """Find DT ajax source + per page from data-wb-tables."""
    try:
        soup = BeautifulSoup(html, "html.parser")
        tbl = soup.find("table", id="results")
        cfg = (tbl.get("data-wb-tables") or "") if tbl else ""
        src = re.search(r'"sAjaxSource"\s*:\s*"([^"]+)"', cfg)
        per = re.search(r'"iDisplayLength"\s*:\s*(\d+)', cfg)
        api = urljoin(BASE, src.group(1)) if src else GET_PAGE_API
        n   = int(per.group(1)) if per else 25
        return api, n
    except Exception:
        return GET_PAGE_API, 25

def _extract_total_entries(html: str) -> int | None:
    m = re.search(r"(?:of|sur)\s+([0-9][0-9\s,\.]*)\s+(?:entries|entrées)", html, flags=re.I)
    if not m: return None
    raw = m.group(1).replace("\u202f"," ").replace("\xa0"," ")
    raw = raw.replace(" ", "").replace(",", "").replace(".", "")
    try:
        return int(raw)
    except Exception:
        return None

# ----------------------------------
# Detail page parsing (minimal, fast)
# ----------------------------------
def _qtext(node) -> str:
    import html as _html
    if node is None: return ""
    txt = node.get_text(" ", strip=True)
    txt = _html.unescape(txt)
    return re.sub(r"\s+", " ", txt).strip()

def _fetch_detail_html(sess: requests.Session, url: str) -> str:
    html, _ = _fetch_page(sess, url, "GET", {}, headers={"Referer": FORM_URL})
    return html

def _parse_detail_page(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    out = {k: "" for k in DETAIL_COLS}

    def _find_row(label_contains: str):
        th = soup.find(lambda t: t and t.name in ("th","dt") and label_contains.lower() in _qtext(t).lower())
        if not th: return ""
        td = th.find_next("td") or th.find_next("dd")
        return _qtext(td)

    out["Status"]   = _find_row("Status")
    out["Company"]  = _find_row("Company")
    out["Product"]  = _find_row("Product")
    out["Class"]    = _find_row("Class")
    out["Schedule"] = _find_row("Schedule")
    out["PM See footnote1"] = _find_row("PM")
    out["Current status date"]  = _find_row("Current status date")
    out["Original market date"] = _find_row("Original market date")

    out["Address"] = _find_row("Address")
    out["City"]    = _find_row("City")
    out["state"]   = _find_row("Province") or _find_row("State")
    out["Country"] = _find_row("Country")
    out["Zipcode"] = _find_row("Postal") or _find_row("Zip")

    out["Number of active ingredient(s)"] = _find_row("Number of active ingredient")
    out["Biosimilar Biologic Drug"]       = _find_row("Biosimilar")
    out["American Hospital Formulary Service (AHFS)"] = _find_row("AHFS")
    out["Anatomical Therapeutic Chemical (ATC)"]      = _find_row("ATC")
    out["Active ingredient group (AIG) number"]       = _find_row("AIG")

    def _first_href_near(label_contains: str) -> str:
        th = soup.find(lambda t: t and t.name in ("th","dt") and label_contains.lower() in _qtext(t).lower())
        if not th: return ""
        a = th.find_next("a")
        if a and a.get("href"):
            href = a["href"]
            return href if href.startswith("http") else BASE.rstrip("/") + href
        return ""
    out["Labelling"] = _first_href_near("Labelling") or _first_href_near("Product Monograph")
    out["Product Monograph/Veterinary Date"] = _find_row("Product Monograph Date") or _find_row("Veterinary Date")
    out["Dosage form"] = _find_row("Dosage form")
    out["Route(s) of administration"] = _find_row("Route")

    # Active ingredients (multiline)
    ai_table = soup.find(lambda t: t and t.name == "table" and "Active ingredient" in _qtext(t.find("caption") or t))
    ai_lines = []
    if ai_table:
        for tr in ai_table.find_all("tr"):
            tds = tr.find_all(["td","th"])
            if len(tds) >= 2:
                name = _qtext(tds[0]); strength = _qtext(tds[1])
                if name and strength and name.lower() not in ("name","active ingredient"):
                    ai_lines.append(f"{name} : {strength}")
    out["List of active ingredient"] = "\n".join(ai_lines)
    if ai_lines:
        first = ai_lines[0]
        if " : " in first:
            nm, st = first.split(" : ", 1)
            out["A.I. name See footnote3"] = nm
            out["Strength"] = st

    return out

def fetch_detail_fields(sess: requests.Session, din_url: str, sleep: float=0.0) -> Dict[str, str]:
    if not din_url:
        return {}
    r = _with_retries(lambda: sess.get(din_url, timeout=TIMEOUT))
    soup = BeautifulSoup(r.text, "html.parser")
    def gr(label: str) -> str:
        lab = label.lower()
        for row in soup.select("div.row"):
            left = row.select_one("p.col-sm-4 strong")
            right = row.select_one("p.col-sm-8")
            if not left or not right: continue
            if lab in left.get_text(" ").lower():
                return norm(right.get_text(" ").replace("\xa0"," "))
        return ""

    # fast subset; we already parse most via _parse_detail_page when enriching
    data = {
        "Current status date": gr("Current status date"),
        "Original market date": gr("Original market date"),
        "Class": gr("Class"),
        "Dosage form": gr("Dosage form"),
        "Route(s) of administration": gr("Route"),
        "Number of active ingredient(s)": gr("Number of active ingredient"),
        "Schedule": gr("Schedule"),
    }
    if sleep and sleep > 0:
        time.sleep(sleep)
    return data

# ----------------------------------
# Excel helper
# ----------------------------------
def save_styled_excel(df: pd.DataFrame, xlsx_path: str) -> None:
    from openpyxl.utils import get_column_letter
    from openpyxl.styles import Alignment, Font
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="DPD")
        ws = xw.book["DPD"]
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions
        ws.row_dimensions[1].height = 36
        for c in ws[1]:
            c.font = Font(bold=True)
            c.alignment = Alignment(vertical="top", wrap_text=True)
        for row in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
            for c in row:
                c.alignment = Alignment(vertical="top", wrap_text=True)
        for i, col in enumerate(df.columns, start=1):
            series = df[col].astype(str).fillna("")
            est = max(len(col), int(series.str.len().quantile(0.85)))
            est = max(12, min(est, 60))
            ws.column_dimensions[get_column_letter(i)].width = est

# ----------------------------------
# Sharding
# ----------------------------------
BRAND_SYMBOL_PREFIXES = list("()[]{}#&+-,.'/")
def build_brand_prefixes() -> list[str]:
    # A-Z, 0-9, then common symbol starters
    return list(string.ascii_uppercase) + list(string.digits) + BRAND_SYMBOL_PREFIXES

def build_din_prefixes() -> list[str]:
    return list(string.digits)

# ----------------------------------
# Collect list rows (paging + shard sweeps)
# ----------------------------------
def collect_all_list_rows(
    sess: requests.Session,
    first_html: str,
    base_filters: dict,
    endpoint_url: str,
    endpoint_method: str,
    min_rows: int,
    sleep: float,
    sample_n: Optional[int] = None,
    max_rows: int = 0,
    post_endpoint_url: Optional[str] = None,
) -> List[Dict]:
    """
    Strategy:
      • Try baseline probe (optional) on the initial endpoint.
      • Then shard by brandName (and DIN). For each prefix:
          p1: POST /search-recherche (to set server filter in session)
          p>=2: GET /getNextPage (DT JSON legacy→modern) with the filter; else HTML fallback
        Continue pages until 0 rows or PAGE_STALL_LIMIT pages with 0 new uniques.
      • Never jump to next prefix until the current one is exhausted.
    """
    import html as _html

    # Clean/unwrap DT cells
    TAG_RE = re.compile(r"<[^>]+>")
    WS_RE  = re.compile(r"\s+")
    def _clean_cell(v: Any) -> str:
        s = "" if v is None else _html.unescape(str(v))
        s = TAG_RE.sub("", s)
        return WS_RE.sub(" ", s).strip()

    def _dt_rows_to_common_rows(aa: list) -> list[dict]:
        rows: list[dict] = []
        if not aa:
            return rows
        if isinstance(aa[0], dict):
            for d in aa:
                din_raw = d.get("DIN") or d.get("din") or d.get("Din")
                if din_raw is None and len(d) >= 2:
                    try: din_raw = list(d.values())[1]
                    except Exception: din_raw = ""
                rows.append({"DIN": _clean_cell(din_raw)})
        else:
            for row in aa:
                din_raw = row[1] if len(row) > 1 else ""
                rows.append({"DIN": _clean_cell(din_raw)})
        return rows

    # preferred POST target for page1
    preferred_post_url = (post_endpoint_url or SEARCH_URL or "").strip()
    if preferred_post_url.endswith("/dispatch-repartition"):
        preferred_post_url = SEARCH_URL
    if "canada.ca/en/sr/srb.html" in preferred_post_url:
        preferred_post_url = SEARCH_URL

    dbg(f"[SWEEP] preferred_post_url={preferred_post_url!r} | force_post_sweeps={bool(SCRAPER_FORCE_POST_SWEEPS)}")

    # Page 1 parse
    rows_all: List[Dict] = []
    page1_rows = parse_list_page_rows(first_html)
    rows_all.extend(page1_rows)
    dbg("Page 1 rows:", len(page1_rows))

    page_api_url, per_page_dt = _detect_table_paging(first_html)
    per_page = per_page_dt if page1_rows else 25
    dbg(f"Detected paging API: {page_api_url} | per_page={per_page}")

    if sample_n:
        return rows_all[:sample_n]

    def _hit_cap() -> bool:
        return max_rows > 0 and len(rows_all) >= max_rows

    if _hit_cap():
        return rows_all[:max_rows]

    total = _extract_total_entries(first_html)
    dbg("Total (from info):", total if total else "not shown; will probe")
    per_page = len(page1_rows) if page1_rows else (per_page_dt or 25)
    max_pages = (total + per_page - 1) // per_page if total else 100

    # 1) Optional baseline GET probe
    if SCRAPER_BASELINE_PROBE and page1_rows:
        paging_keys = [
            lambda p: {"results_page": p},
            lambda p: {"page": p},
            lambda p: {"p": p},
            lambda p: {"start": (p - 1) * per_page},
        ]
        used_key = None
        used_method = endpoint_method
        for strat in paging_keys:
            payload = {**base_filters, **strat(2), "lang":"eng","wbdisable":"true"}
            try:
                html2, used_method = _fetch_page(sess, endpoint_url, used_method, payload)
                r2 = parse_list_page_rows(html2)
                new_vs_p1 = len({r.get("DIN","") for r in r2} - {r.get("DIN","") for r in page1_rows})
                dbg(f"Probe {list(strat(2).keys())[0]} ({used_method}) -> {len(r2)} rows; new vs p1: {new_vs_p1}")
                if r2 and new_vs_p1 > 0:
                    used_key = strat
                    seen = {_canon_din(r.get("DIN","")) for r in rows_all}
                    added = 0
                    for rr in r2:
                        din = _canon_din(rr.get("DIN",""))
                        if din and din not in seen:
                            rows_all.append(rr); seen.add(din); added += 1
                    dbg(f"Baseline accepted; +{added} (cum={len(rows_all)})")
                    _heartbeat(len(rows_all), max_rows or None)
                    if _hit_cap(): return rows_all[:max_rows]
                    break
            except Exception as e:
                dbg(f"Baseline probe failed: {e}")
            time.sleep(sleep)

        if used_key:
            stall = 0
            for page in range(3, max_pages + 1):
                payload = {**base_filters, **used_key(page), "lang":"eng","wbdisable":"true"}
                try:
                    htmlp, used_method = _fetch_page(sess, endpoint_url, used_method, payload)
                except Exception as e:
                    dbg(f"Page {page} fetch failed: {e}"); break
                chunk = parse_list_page_rows(htmlp)
                if not chunk: break
                seen = {_canon_din(r.get("DIN","")) for r in rows_all}
                added = 0
                for rr in chunk:
                    din = _canon_din(rr.get("DIN",""))
                    if din and din not in seen:
                        rows_all.append(rr); seen.add(din); added += 1
                if added == 0:
                    stall += 1
                    if stall >= PAGE_STALL_LIMIT:
                        dbg("Baseline pagination stalled; stop.")
                        break
                else:
                    stall = 0
                _heartbeat(len(rows_all), max_rows or None)
                if _hit_cap(): return rows_all[:max_rows]
                time.sleep(sleep)

            if total or len(rows_all) >= min_rows:
                return rows_all if not _hit_cap() else rows_all[:max_rows]

    # 2) Sharded sweeps
    dbg("Falling back to sharded sweeps (POST→DT JSON→HTML)…")

    def _with_csrf(extra: dict) -> dict:
        try:
            token = get_csrf(sess)
        except Exception:
            token = base_filters.get("_csrf","")
        data = {**base_filters, **extra}
        if token: data["_csrf"] = token
        data.setdefault("lang","eng"); data.setdefault("wbdisable","true")
        return data

    def _post_then_get(filter_key: str, value: str, page: int) -> tuple[list[dict], str]:
        """
        p=1: POST /search-recherche with filter to set session → parse HTML rows
        p>=2: GET DT JSON (legacy→modern) including the filter in query;
              fallback to HTML GET with page keys.
        """
        # --- p1: POST ---
        if page == 1:
            url = preferred_post_url or SEARCH_URL
            headers = {"Content-Type":"application/x-www-form-urlencoded","Origin":BASE,"Referer":FORM_URL}
            payload = _with_csrf({filter_key: value})
            rows = []
            try:
                dbg(f"[POST try] {url} {filter_key}='{value}'")
                r = _with_retries(lambda: sess.post(url, data=payload, timeout=TIMEOUT, allow_redirects=True, headers=headers))
                if "canada.ca/en/sr/srb.html" in (r.url or ""):
                    dbg("[POST bounce] SRB relay")
                else:
                    rows = parse_list_page_rows(r.text)
                    dbg(f"[POST got] {len(rows)} rows")
            except Exception as e:
                dbg(f"[POST err] {e!r}")

            if rows:
                return rows, "POST"

            # hard fallback
            html, _ = _fetch_page(sess, RESULTS_URL, "GET", {filter_key:value,"lang":"eng","wbdisable":"true"})
            rows = parse_list_page_rows(html)
            dbg(f"[GET p1 fallback] {len(rows)} rows")
            return rows, "GET(HTML)"

        # --- p>=2: DT JSON ---
        api = page_api_url or GET_PAGE_API
        start = (page - 1) * per_page

        def _dt_legacy() -> list[dict]:
            DT_COLS = ["status","din","company","brand","drugClass","pm","schedule","aiNum","majorAI","AIStrength"]
            if not hasattr(_post_then_get, "_echo"): _post_then_get._echo = 1
            echo = _post_then_get._echo; _post_then_get._echo += 1

            params = {
                "sEcho": str(echo),
                "iColumns": str(len(DT_COLS)),
                "sColumns": ",".join(DT_COLS),
                "iDisplayStart": str(start),
                "iDisplayLength": str(per_page),
                "iSortingCols": "1",
                "iSortCol_0": "3", "sSortDir_0": "asc",
                "sSearch": "", "bRegex": "false",
                "lang": "eng", "wbdisable": "true",
                "_": str(int(time.time()*1000)),
                filter_key: value,
            }
            for i, name in enumerate(DT_COLS):
                params[f"mDataProp_{i}"] = name
                params[f"sSearch_{i}"]   = ""
                params[f"bRegex_{i}"]    = "false"
                params[f"bSearchable_{i}"] = "true"
                params[f"bSortable_{i}"]   = "false" if name == "AIStrength" else "true"

            headers = {"Referer": FORM_URL, "X-Requested-With":"XMLHttpRequest"}
            dbg(f"[DT legacy] {api} start={start} len={per_page} filter={filter_key}:{value}")
            r = _with_retries(lambda: sess.get(api, params=params, timeout=TIMEOUT, headers=headers))
            try: j = r.json()
            except Exception: j = json.loads(r.text)
            aa = j.get("aaData") or j.get("data") or []
            return _dt_rows_to_common_rows(aa)

        def _dt_modern() -> list[dict]:
            params = {
                "draw": str(int(time.time()*1000) % 100000),
                "start": str(start), "length": str(per_page),
                "search[value]": "", "search[regex]": "false",
                "lang":"eng","wbdisable":"true",
                filter_key: value,
            }
            headers = {"Referer": FORM_URL, "X-Requested-With":"XMLHttpRequest"}
            dbg(f"[DT modern] {api} start={start} length={per_page} filter={filter_key}:{value}")
            r = _with_retries(lambda: sess.get(api, params=params, timeout=TIMEOUT, headers=headers))
            try: j = r.json()
            except Exception: j = json.loads(r.text)
            aa = j.get("data") or j.get("aaData") or []
            return _dt_rows_to_common_rows(aa)

        try:
            rows = _dt_legacy()
            if not rows:
                rows = _dt_modern()
            if rows:
                dbg(f"[DT got] {len(rows)} rows @ page={page}")
                return rows, "GET(JSON)"
        except Exception as e:
            dbg(f"[DT error] {e!r}")

        # --- HTML fallback ---
        base = {filter_key: value, "lang":"eng","wbdisable":"true"}
        for key in PAGE_KEYS:
            payload2 = dict(base)
            payload2[key] = page if key not in ("start","iDisplayStart") else start
            dbg(f"[HTML try] {RESULTS_URL} {key}={payload2[key]} filter={filter_key}:{value}")
            try:
                html, _ = _fetch_page(sess, RESULTS_URL, "GET", payload2)
                rows = parse_list_page_rows(html)
                if rows:
                    dbg(f"[HTML got] {len(rows)} rows @ page={page}")
                    return rows, "GET(HTML)"
            except Exception as e:
                dbg(f"[HTML err] {e!r}")

        return [], "EMPTY"

    def _sweep_one(filter_key: str, value: str) -> Tuple[int, bool]:
        """Drain all pages for one prefix before moving on."""
        added_total = 0
        saw_any = False

        # Page 1
        rows1, meth1 = _post_then_get(filter_key, value, 1)
        _log_prefix_try(filter_key, value, "POST" if meth1 == "POST" else meth1, 1, len(rows1), len(rows_all))
        if not rows1:
            return 0, False
        saw_any = True
        seen = {_canon_din(r.get("DIN","")) for r in rows_all}
        add = 0
        for rr in rows1:
            din = _canon_din(rr.get("DIN",""))
            if din and din not in seen:
                rows_all.append(rr); seen.add(din)
                added_total += 1; add += 1
                _heartbeat(len(rows_all), max_rows or None)
                if _hit_cap(): return added_total, True
        if add:
            dbg(f"[{filter_key}='{value}'] p1 +{add} (cum={len(rows_all)})")

        # Pages 2..N
        dbg(f"[{filter_key}='{value}'] paging up to {SWEEP_PAGE_LIMIT}")
        stall = 0
        for p in range(2, SWEEP_PAGE_LIMIT + 1):
            rows_p, origin = _post_then_get(filter_key, value, p)
            _log_prefix_try(filter_key, value, origin, p, len(rows_p), len(rows_all))
            if not rows_p:
                dbg(f"[{filter_key}='{value}'] page {p} returned 0 rows → stop")
                break
            added_this = 0
            for rr in rows_p:
                din = _canon_din(rr.get("DIN",""))
                if din and din not in seen:
                    rows_all.append(rr); seen.add(din)
                    added_total += 1; added_this += 1
                    _heartbeat(len(rows_all), max_rows or None)
                    if _hit_cap(): return added_total, True
            if added_this == 0:
                stall += 1
                dbg(f"[{filter_key}='{value}'] page {p} had 0 new uniques (stall={stall})")
                if stall >= PAGE_STALL_LIMIT:
                    dbg(f"[{filter_key}='{value}'] stopping after {stall} consecutive empty pages")
                    break
            else:
                stall = 0
            time.sleep(max(sleep, 0.02))

        if added_total:
            dbg(f"[{filter_key}='{value}'] +{added_total} new (cum={len(rows_all)})")
        return added_total, saw_any

    # Shard order (A-Z, 0-9, symbols) then DIN (0-9)
    brand_prefixes = build_brand_prefixes()
    din_prefixes   = build_din_prefixes()

    def _run_prefix_group(kind: str, values: List[str]) -> bool:
        empty_streak = 0
        for i, v in enumerate(values, 1):
            if SWEEP_PREFIX_LOG_EVERY and i % SWEEP_PREFIX_LOG_EVERY == 0:
                dbg(f"[{kind}] prefix {i}/{len(values)} (cum={len(rows_all)})")
            added, saw_any = _sweep_one(kind, v)
            if not saw_any:
                empty_streak += 1
                if SCRAPER_SWEEP_MAX_EMPTY and empty_streak >= SCRAPER_SWEEP_MAX_EMPTY:
                    dbg(f"[{kind}] early stop after {empty_streak} empty prefixes")
                    return True
            else:
                empty_streak = 0
            if _hit_cap():
                return True
            time.sleep(max(sleep, 0.02))
        return False

    # Run shards
    if SCRAPER_SWEEP_ORDER == "brand-first":
        if _run_prefix_group("brandName", brand_prefixes): return rows_all[:max_rows] if _hit_cap() else rows_all
        if _run_prefix_group("din", din_prefixes):         return rows_all[:max_rows] if _hit_cap() else rows_all
    else:
        if _run_prefix_group("din", din_prefixes):         return rows_all[:max_rows] if _hit_cap() else rows_all
        if _run_prefix_group("brandName", brand_prefixes): return rows_all[:max_rows] if _hit_cap() else rows_all

    return rows_all if not _hit_cap() else rows_all[:max_rows]

# ----------------------------------
# Enrichment (kept minimal for speed)
# ----------------------------------
def enrich_rows_with_details(sess: requests.Session, rows_all: list[dict]) -> list[dict]:
    enriched: list[dict] = []
    for r in rows_all:
        base = {k: "" for k in DETAIL_COLS}
        din_url = (r.get("DIN URL") or "").strip()
        base["DIN URL"] = din_url
        base["DIN"]     = _canon_din_display(r.get("DIN") or r.get("din") or "")
        base["Status"]  = (r.get("Status") or "").strip()
        base["Company"] = (r.get("Company") or "").strip()
        base["Product"] = (r.get("Product") or "").strip()
        base["Class"]   = (r.get("Class") or "").strip()
        base["PM See footnote1"] = (r.get("PM See footnote1") or r.get("pm") or "").strip()
        base["Schedule"] = (r.get("Schedule") or "").strip()
        base["# See footnote2"] = (r.get("# See footnote2") or r.get("aiNum") or "").strip()
        base["A.I. name See footnote3"] = (r.get("A.I. name See footnote3") or r.get("majorAI") or "").strip()
        base["Strength"] = (r.get("Strength") or r.get("AIStrength") or "").strip()

        # Optional detail scrape (fast subset)
        if din_url:
            try:
                det = fetch_detail_fields(sess, din_url, sleep=0.0)
                for k, v in (det or {}).items():
                    if v is None: continue
                    base[k] = v
            except Exception as e:
                dbg(f"[DETAIL] {din_url} error: {e!r}")

        for col in DETAIL_COLS:
            base[col] = "" if base.get(col) is None else str(base.get(col))
        if not base.get("Biosimilar Biologic Drug"):
            base["Biosimilar Biologic Drug"] = "No"

        enriched.append(base)
    return enriched

# ----------------------------------
# Entrypoint
# ----------------------------------
def run_full_scrape(
    max_depth: int = DEF_MAX_DEPTH,
    target_min_rows: int = DEF_TARGET_MIN_ROWS,
    enrich_flush_every: int = DEF_ENRICH_FLUSH_EVERY,
    request_sleep: float = DEF_REQUEST_SLEEP,
    max_rows: int = DEF_MAX_ROWS,
) -> Tuple[List[Dict], Dict]:
    t0 = time.time()
    sess = make_session()

    first_html, base_filters, post_url = submit_search(sess, basic_html=True)

    base_rows = collect_all_list_rows(
        sess=sess,
        first_html=first_html,
        base_filters=base_filters,
        endpoint_url=RESULTS_URL,
        endpoint_method="GET",
        min_rows=target_min_rows,
        sleep=request_sleep,
        max_rows=max_rows,
        post_endpoint_url=post_url,
    )
    dbg("List collected:", len(base_rows))

    # Light enrichment (kept fast)
    enriched: List[Dict] = []
    for i, r in enumerate(base_rows, 1):
        try:
            det = fetch_detail_fields(sess, r.get("DIN URL",""), sleep=0.0)
        except Exception as e:
            dbg(f"Detail fetch failed for DIN {r.get('DIN','?')}: {e}")
            det = {}
        merged = {col: "" for col in COLUMNS}
        merged.update(r)
        for k, v in (det or {}).items():
            if v is None: continue
            merged[k] = v
        if not merged.get("Biosimilar Biologic Drug"):
            merged["Biosimilar Biologic Drug"] = "No"
        for col in COLUMNS:
            merged.setdefault(col, "")
        enriched.append(merged)
        if i % enrich_flush_every == 0:
            dbg(f"Enriched {i}/{len(base_rows)}")

    elapsed = round(time.time() - t0, 2)
    meta = {
        "strategy": "baseline(optional) + shard-sweeps(POST→DT→HTML)",
        "elapsed_sec": elapsed,
        "rows": len(enriched),
        "request_sleep": request_sleep,
        "max_depth": max_depth,
        "target_min_rows": target_min_rows,
        "max_rows": max_rows,
        "post_action": post_url,
    }
    dbg("Done. Rows:", len(enriched), "Elapsed(s):", elapsed)
    return enriched, meta

# ----------------------------------
# CLI (optional)
# ----------------------------------
if __name__ == "__main__":
    rows, meta = run_full_scrape()
    print({"rows": len(rows), **meta})
