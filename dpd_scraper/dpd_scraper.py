# dpd_scraper/dpd_scraper.py
from __future__ import annotations
from typing import Optional, List, Dict, Tuple
from dpd_scraper.columns import DETAIL_COLS
import pandas as pd

import os, time, re, json, string
from urllib.parse import urljoin
from dotenv import load_dotenv

import requests
from requests.exceptions import ReadTimeout, ConnectTimeout, Timeout, HTTPError
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup

load_dotenv()

# ----------------------------
# Config & constants
# ----------------------------
BASE          = "https://health-products.canada.ca"
FORM_URL      = f"{BASE}/dpd-bdpp/?lang=eng"
DISPATCH_URL  = f"{BASE}/dpd-bdpp/dispatch-repartition"
RESULTS_URL   = f"{BASE}/dpd-bdpp/search-fast-recherche-rapide"   # GET listing (DataTables-like)
SEARCH_URL    = f"{BASE}/dpd-bdpp/search-recherche"               # Regular POST action

TIMEOUT       = int(os.getenv("SCRAPER_TIMEOUT", "90"))
RETRIES       = int(os.getenv("SCRAPER_RETRIES", "5"))
RETRY_SLEEP   = float(os.getenv("SCRAPER_RETRY_SLEEP", "1.2"))

DEF_MAX_DEPTH          = int(os.getenv("SCRAPER_MAX_DEPTH", "1"))
DEF_TARGET_MIN_ROWS    = int(os.getenv("SCRAPER_TARGET_MIN_ROWS", "2000"))
DEF_ENRICH_FLUSH_EVERY = int(os.getenv("SCRAPER_ENRICH_FLUSH_EVERY", "50"))
DEF_REQUEST_SLEEP      = float(os.getenv("SCRAPER_REQUEST_SLEEP", "0.08"))

DEF_MAX_ROWS           = int(os.getenv("SCRAPER_MAX_ROWS", "0"))  # 0 = no cap

SCRAPER_SWEEP_ENDPOINT = os.getenv("SCRAPER_SWEEP_ENDPOINT", "").strip()
SCRAPER_SWEEP_ORDER    = (os.getenv("SCRAPER_SWEEP_ORDER", "din-first") or "din-first").strip().lower()
SCRAPER_SWEEP_MAX_EMPTY= int(os.getenv("SCRAPER_SWEEP_MAX_EMPTY", "0"))
SCRAPER_BASELINE       = os.getenv("SCRAPER_BASELINE", "").strip()

SCRAPER_FORCE_POST_SWEEPS = int(os.getenv("SCRAPER_FORCE_POST_SWEEPS", "1"))
PAGE_KEYS = [k.strip() for k in os.getenv(
    "SCRAPER_PAGE_KEYS",
    "results_page,page,p,start,iDisplayStart"
).split(",") if k.strip()]


ENDPOINTS_FOR_SWEEP: List[Tuple[str, str, str]] = [
    ("RESULTS_URL",  RESULTS_URL,  "GET"),
    ("DISPATCH_URL", DISPATCH_URL, "GET"),
]

DEBUG = True
DEBUG_VERBOSE          = int(os.getenv("SCRAPER_DEBUG_VERBOSE", "1"))   # 0=quiet, 1=normal, 2=chatty
LOG_EVERY_ADDED        = int(os.getenv("SCRAPER_LOG_EVERY_ADDED", "50"))
SWEEP_PAGE_LIMIT       = int(os.getenv("SCRAPER_SWEEP_PAGE_LIMIT", "8"))
SWEEP_PREFIX_LOG_EVERY = int(os.getenv("SCRAPER_SWEEP_PREFIX_LOG_EVERY", "1"))
PAGE_STALL_LIMIT       = int(os.getenv("SCRAPER_PAGE_STALL_LIMIT", "2"))

_t0_global = time.time()
_last_beat = {"count": 0, "t": _t0_global}

def dbg(*args):
    if DEBUG:
        print("[DEBUG]", *args)

# ----------------------------
# Columns
# ----------------------------

DETAIL_COLS = [
    "Status","DIN URL","DIN","Company","Product","Class","PM See footnote1","Schedule",
    "# See footnote2","A.I. name See footnote3","Strength",
    "Current status date","Original market date",
    "Address","City","state","Country","Zipcode",
    "Number of active ingredient(s)","Biosimilar Biologic Drug",
    "American Hospital Formulary Service (AHFS)","Anatomical Therapeutic Chemical (ATC)",
    "Active ingredient group (AIG) number","Labelling","Product Monograph/Veterinary Date",
    "List of active ingredient","Dosage form","Route(s) of administration"
]

COLUMNS = [
    "Status","DIN URL","DIN","Company","Product","Class","PM See footnote1","Schedule",
    "# See footnote2","A.I. name See footnote3","Strength",
    "Current status date","Original market date","Address","City","state","Country","Zipcode",
    "Number of active ingredient(s)","Biosimilar Biologic Drug",
    "American Hospital Formulary Service (AHFS)","Anatomical Therapeutic Chemical (ATC)",
    "Active ingredient group (AIG) number","Labelling","Product Monograph/Veterinary Date",
    "List of active ingredient","Dosage form","Route(s) of administration"
]

# ----------------------------
# Helpers
# ----------------------------

def save_styled_excel(df, xlsx_path: str) -> None:
    from openpyxl.utils import get_column_letter
    from openpyxl.styles import Alignment
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="DPD")
        ws = xw.book["DPD"]

        # Freeze header + Autofilter
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions

        # Header style
        ws.row_dimensions[1].height = 36
        for c in ws[1]:
            c.alignment = Alignment(vertical="top", wrap_text=True)

        # Body cells
        for row in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
            for c in row:
                c.alignment = Alignment(vertical="top", wrap_text=True)

        # Column widths
        for i, col in enumerate(df.columns, start=1):
            series = df[col].astype(str).fillna("")
            est = max(len(col), int(series.str.len().quantile(0.85)))
            est = max(12, min(est, 60))
            ws.column_dimensions[get_column_letter(i)].width = est


def _qtext(node) -> str:
    import re, html
    if node is None:
        return ""
    txt = node.get_text(" ", strip=True)
    txt = html.unescape(txt)
    return re.sub(r"\s+", " ", txt).strip()

def _fetch_detail_html(sess: requests.Session, url: str) -> str:
    headers = {"Referer": FORM_URL}
    html, _ = _fetch_page(sess, url, "GET", {}, headers=headers)
    return html

def _parse_detail_page(html: str) -> dict:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    out = {k: "" for k in DETAIL_COLS}

    def _find_row(label_contains: str):
        th = soup.find(lambda t: t and t.name in ("th","dt") and label_contains.lower() in _qtext(t).lower())
        if not th: return ""
        td = th.find_next("td") or th.find_next("dd")
        return _qtext(td)

    # Basics
    out["Status"]   = _find_row("Status")
    out["Company"]  = _find_row("Company")
    out["Product"]  = _find_row("Product")
    out["Class"]    = _find_row("Class")
    out["Schedule"] = _find_row("Schedule")
    out["PM See footnote1"] = _find_row("PM")

    # Dates
    out["Current status date"]  = _find_row("Current status date")
    out["Original market date"] = _find_row("Original market date")

    # Address pieces
    out["Address"] = _find_row("Address")
    out["City"]    = _find_row("City")
    out["state"]   = _find_row("Province") or _find_row("State")
    out["Country"] = _find_row("Country")
    out["Zipcode"] = _find_row("Postal") or _find_row("Zip")

    # Counts/flags/classifications
    out["Number of active ingredient(s)"] = _find_row("Number of active ingredient")
    out["Biosimilar Biologic Drug"]       = _find_row("Biosimilar")
    out["American Hospital Formulary Service (AHFS)"] = _find_row("AHFS")
    out["Anatomical Therapeutic Chemical (ATC)"]      = _find_row("ATC")
    out["Active ingredient group (AIG) number"]       = _find_row("AIG")

    # PM / Labelling link + date
    def _first_href_near(label_contains: str) -> str:
        th = soup.find(lambda t: t and t.name in ("th","dt") and label_contains.lower() in _qtext(t).lower())
        if not th: return ""
        a = th.find_next("a")
        if a and a.get("href"):
            href = a["href"]
            return href if href.startswith("http") else BASE.rstrip("/") + href
        return ""
    out["Labelling"] = _first_href_near("Labelling") or _first_href_near("Veterinary Labelling") or _first_href_near("Product Monograph")
    out["Product Monograph/Veterinary Date"] = _find_row("Product Monograph Date") or _find_row("Veterinary Date")

    # Dosage / Route
    out["Dosage form"] = _find_row("Dosage form")
    out["Route(s) of administration"] = _find_row("Route")

    # Active ingredients table → multi-line
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

    # First AI + strength for list-style fields
    if ai_lines:
        first = ai_lines[0]
        if " : " in first:
            nm, st = first.split(" : ", 1)
            out["A.I. name See footnote3"] = nm
            out["Strength"] = st

    return out

def enrich_rows_with_details(sess: requests.Session, rows_all: list[dict]) -> list[dict]:
    """
    Takes list-page rows and returns fully-enriched rows in the exact DETAIL_COLS shape.
    - Builds DIN URL if missing (from _info_code).
    - Prefers values scraped from the detail page when available.
    - Guarantees every column in DETAIL_COLS exists (string, trimmed).
    """
    enriched: list[dict] = []

    for r in rows_all:
        # start with all desired columns blank
        base = {k: "" for k in DETAIL_COLS}

        # --- list-page fallbacks ---
        din_url = (r.get("DIN URL") or "").strip()
        if not din_url:
            code = (r.get("_info_code") or "").strip()
            if code:
                din_url = f"{BASE.rstrip('/')}/dpd-bdpp/info?lang=eng&code={code}"

        base["DIN URL"] = din_url
        base["DIN"]     = _canon_din_display(r.get("DIN") or r.get("din") or "")

        base["Status"]   = (r.get("Status")   or r.get("status")    or "").strip()
        base["Company"]  = (r.get("Company")  or r.get("company")   or "").strip()
        base["Product"]  = (r.get("Product")  or r.get("brand")     or "").strip()
        base["Class"]    = (r.get("Class")    or r.get("drugClass") or "").strip()
        base["PM See footnote1"] = (r.get("PM See footnote1") or r.get("pm") or "").strip()
        base["Schedule"] = (r.get("Schedule") or r.get("schedule")  or "").strip()
        base["# See footnote2"]        = (r.get("# See footnote2")        or r.get("aiNum")      or "").strip()
        base["A.I. name See footnote3"] = (r.get("A.I. name See footnote3") or r.get("majorAI") or "").strip()
        base["Strength"]               = (r.get("Strength")               or r.get("AIStrength") or "").strip()

        if din_url:
            try:
                html = _fetch_detail_html(sess, din_url)
                det  = _parse_detail_page(html) or {}
                for k, v in det.items():
                    if v is None:
                        continue
                    if isinstance(v, str):
                        v = v.replace("\r\n", "\n").replace("\r", "\n").strip()
                    if k in ("A.I. name See footnote3", "Strength"):
                        if not base[k] and v:
                            base[k] = v
                    else:
                        base[k] = v
            except Exception as e:
                dbg(f"[DETAIL] error for {din_url}: {e}")

        for col in DETAIL_COLS:
            val = base.get(col, "")
            base[col] = "" if val is None else (str(val) if not isinstance(val, str) else val)

        enriched.append(base)

    return enriched


def _canon_din_display(v: str) -> str:
    s = "".join(ch for ch in str(v or "") if ch.isdigit())
    return s.lstrip("0") or s  # keep non-empty

# DataTables (legacy) server-side paging for the DPD table
DT_MDATA = ["status","din","company","brand","drugClass","pm","schedule","aiNum","majorAI","AIStrength"]

def build_dt_params(start: int, length: int = 25, echo: int = 1,
                    sort_col: int = 3, sort_dir: str = "asc") -> dict:
    """
    Returns the legacy DataTables server-side GET params that DPD expects.
    - sort_col=3 because page HTML says order: [3, "asc"] (brand column)
    """
    p = {
        "sEcho": str(echo),
        "iColumns": str(len(DT_MDATA)),
        "sColumns": ",".join(DT_MDATA),
        "iDisplayStart": str(start),
        "iDisplayLength": str(length),
        "iSortingCols": "1",
        "iSortCol_0": str(sort_col),
        "sSortDir_0": sort_dir,
        "sSearch": "",
        "bRegex": "false",
        "lang": "eng",
        "wbdisable": "true",
    }
    for i, name in enumerate(DT_MDATA):
        p[f"mDataProp_{i}"] = name
    return p


GET_PAGE_API = f"{BASE}/dpd-bdpp/getNextPage"

def _detect_table_paging(html: str) -> tuple[str, int]:
    """
    Inspect data-wb-tables config on the results page to find the server-side
    paging API and per-page size. Fallback to defaults if not found.
    """
    try:
        soup = BeautifulSoup(html, "html.parser")
        tbl = soup.find("table", id="results")
        if not tbl:
            return GET_PAGE_API, 25
        cfg_raw = tbl.get("data-wb-tables") or ""
        # cheap parse: pull sAjaxSource and iDisplayLength
        src_m = re.search(r'"sAjaxSource"\s*:\s*"([^"]+)"', cfg_raw)
        len_m = re.search(r'"iDisplayLength"\s*:\s*(\d+)', cfg_raw)
        api = urljoin(BASE, src_m.group(1)) if src_m else GET_PAGE_API
        per = int(len_m.group(1)) if len_m else 25
        return api, per
    except Exception:
        return GET_PAGE_API, 25


SPACES_RX = re.compile(r"[ \t\r\f\v]+")
DATE_RX   = re.compile(r"\b(19|20)\d{2}-\d{2}-\d{2}\b")

def norm(x: str | None) -> str:
    if not x:
        return ""
    x = x.replace("\xa0", " ").replace("\u200b"," ")
    x = SPACES_RX.sub(" ", x).strip()
    return x

def _canon_din(d: str) -> str:
    # DINs are numeric; remove all non-digits
    return "".join(ch for ch in (d or "") if ch.isdigit())

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

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; dpd-scraper/1.2; +https://example.org)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
        "Referer": FORM_URL,
    })
    retry = Retry(
        total=RETRIES,
        backoff_factor=RETRY_SLEEP,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET","POST"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=40)
    s.mount("https://", adapter); s.mount("http://", adapter)
    s.timeout = TIMEOUT
    return s

def _with_retries(fn):
    last = None
    for i in range(RETRIES):
        try:
            resp = fn()
            resp.raise_for_status()
            return resp
        except (ReadTimeout, ConnectTimeout, Timeout, HTTPError, requests.exceptions.RequestException) as e:
            last = e
            time.sleep(RETRY_SLEEP * (i + 1))
    if last:
        raise last

def _extract_total_entries(html: str) -> int | None:
    m = re.search(r"(?:of|sur)\s+([0-9][0-9\s,\.]*)\s+(?:entries|entrées)", html, flags=re.I)
    if not m:
        return None
    try:
        raw = m.group(1).replace("\u202f", " ").replace("\xa0", " ")
        raw = raw.replace(" ", "").replace(",", "").replace(".", "")
        return int(raw)
    except Exception:
        return None

def _fetch_page(sess: requests.Session, url: str, method: str, payload: dict) -> tuple[str, str]:
    method = (method or "GET").upper()
    if method == "POST":
        r = _with_retries(lambda: sess.post(url, data=payload, timeout=TIMEOUT, allow_redirects=True))
    else:
        r = _with_retries(lambda: sess.get(url, params=payload, timeout=TIMEOUT, allow_redirects=True))
    return r.text, method

# ----------------------------
# Form / initial hit
# ----------------------------
def get_csrf(sess: requests.Session) -> str:
    r = _with_retries(lambda: sess.get(FORM_URL, timeout=TIMEOUT))
    soup = BeautifulSoup(r.text, "html.parser")
    el = soup.select_one("input#_csrf, input[name=_csrf]")
    return el["value"] if el and el.get("value") else ""

def _discover_form(sess: requests.Session) -> tuple[str, dict]:
    r = _with_retries(lambda: sess.get(FORM_URL, timeout=TIMEOUT, allow_redirects=True))
    r.raise_for_status()
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
        raise RuntimeError("Search form not found on FORM_URL")

    action = form.get("action") or SEARCH_URL
    action_url = urljoin(BASE, action)

    # Avoid GC search relay; force a known-good POST action instead
    if "canada.ca/en/sr/srb.html" in action_url or BASE not in action_url:
        action_url = DISPATCH_URL  # safe POST target

    defaults = {}
    for inp in form.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        t = (inp.get("type") or "").lower()
        if t in ("checkbox","radio"):
            if inp.has_attr("checked"):
                defaults.setdefault(name, inp.get("value", "on"))
        else:
            defaults.setdefault(name, inp.get("value", ""))

    if "_csrf" not in defaults:
        try:
            csrf = get_csrf(sess)
            if csrf:
                defaults["_csrf"] = csrf
        except Exception:
            pass

    return action_url, defaults

def parse_list_page_rows(html: str) -> list[dict]:
    """
    Parse the list (results) table rows from a DPD results page.
    Returns a list of dicts with the baseline columns we later enrich.
    """
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
        a_tag = din_cell.find("a", href=True)           
        din_text = (a_tag.get_text(strip=True) if a_tag else din_cell.get_text(strip=True))
        din_href = (a_tag["href"] if a_tag else "")
        din_url  = urljoin("https://health-products.canada.ca", din_href)

        din_norm = din_text.strip()

        rows_out.append({
            "Status": status,
            "DIN URL": din_url,
            "DIN": din_norm,
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
def _baseline_candidates():
    if SCRAPER_BASELINE:
        try:
            name, method = SCRAPER_BASELINE.split(":")
            url = RESULTS_URL if name.strip() == "RESULTS_URL" else DISPATCH_URL
            yield (name.strip(), url, method.strip().upper())
        except Exception:
            pass
    for ep in ENDPOINTS_FOR_SWEEP:
        yield ep

def _sweep_endpoint_candidates(baseline_url: str | None = None):
    """
    Endpoint order for FILTERED sweeps:
    1) explicit SCRAPER_SWEEP_ENDPOINT if set,
    2) the working baseline (if RESULTS/ DISPATCH),
    3) the remaining default endpoint.
    """
    seen = set()
    if SCRAPER_SWEEP_ENDPOINT:
        try:
            name, meth = SCRAPER_SWEEP_ENDPOINT.split(":")
            url = RESULTS_URL if name.strip() == "RESULTS_URL" else DISPATCH_URL
            yield (name.strip(), url, meth.strip().upper()); seen.add(url)
        except Exception:
            pass
    if baseline_url in (DISPATCH_URL, RESULTS_URL) and baseline_url not in seen:
        name = "DISPATCH_URL" if baseline_url == DISPATCH_URL else "RESULTS_URL"
        yield (name, baseline_url, "GET"); seen.add(baseline_url)
    for nm, url, meth in [("RESULTS_URL", RESULTS_URL, "GET"), ("DISPATCH_URL", DISPATCH_URL, "GET")]:
        if url not in seen:
            yield (nm, url, meth)

def submit_search(sess: requests.Session, basic_html: bool = False) -> tuple[str, dict, str]:
    """
    Return (first_html, base_filters, post_url_to_use)
    post_url_to_use will be a safe POST target (search-recherche or dispatch-repartition).
    """
    post_url, defaults = _discover_form(sess)

    base_filters = {
        "din": "", "atc": "", "companyName": "", "brandName": "", "activeIngredient": "", "aigNumber": "",
        "biosimDrugSearch": "",
    }
    payload = {**defaults, **base_filters}
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": BASE,
        "Referer": FORM_URL,
    }
    try:
        r = _with_retries(lambda: sess.post(post_url, data=payload, timeout=TIMEOUT, allow_redirects=True, headers=headers))
        r.raise_for_status()
        if "canada.ca/en/sr/srb.html" in (r.url or ""):
            raise HTTPError("SRB relay detected")
        return r.text, base_filters, post_url
    except Exception as e:
        dbg(f"submit_search: POST {post_url} failed/bounced ({e}); trying DISPATCH_URL")

    try:
        r = _with_retries(lambda: sess.post(DISPATCH_URL, data=payload, timeout=TIMEOUT, allow_redirects=True, headers=headers))
        r.raise_for_status()
        return r.text, base_filters, DISPATCH_URL
    except Exception as e:
        dbg(f"submit_search: POST DISPATCH_URL failed ({e}); last-resort GET")

    params = {"lang": "eng"} | ({"wbdisable": "true"} if basic_html else {})
    r = _with_retries(lambda: sess.get(RESULTS_URL, params=params, timeout=TIMEOUT, allow_redirects=True))
    return r.text, base_filters, DISPATCH_URL

# ----------------------------
# Detail page parsing
# ----------------------------
def get_right_value_for_label_contains(soup: BeautifulSoup, label_substring: str) -> str:
    needle = label_substring.lower()
    for row in soup.select("div.row"):
        left = row.select_one("p.col-sm-4 strong")
        right = row.select_one("p.col-sm-8")
        if not left or not right:
            continue
        if needle in left.get_text(" ").lower():
            return norm(right.get_text(" ").replace("\xa0"," "))
    return ""

def parse_company_block(soup: BeautifulSoup) -> Dict[str,str]:
    out = {"Company":"", "Address":"", "City":"", "state":"", "Country":"", "Zipcode":""}
    for row in soup.select("div.row"):
        left = row.select_one("p.col-sm-4 strong")
        if not left:
            continue
        if "company" in left.get_text(" ").lower():
            right = row.select_one("p.col-sm-8")
            if not right:
                break
            a = right.select_one("a#company")
            out["Company"] = norm(a.get_text(" ")) if a else out["Company"]
            spans = [norm(s.get_text(" ")) for s in right.select("span")]
            if len(spans) >= 1: out["Address"] = spans[0]
            if len(spans) >= 2: out["City"]    = spans[1]
            if len(spans) >= 3: out["state"]   = spans[2]
            if len(spans) >= 4: out["Country"] = spans[3]
            if len(spans) >= 5: out["Zipcode"] = spans[4]
            break
    return out

def parse_labelling_link_and_date(soup: BeautifulSoup) -> Tuple[str, str]:
    lab_url, lab_date = "", ""
    for row in soup.select("div.row"):
        left = row.select_one("p.col-sm-4 strong")
        right = row.select_one("p.col-sm-8")
        if not left or not right:
            continue
        label = left.get_text(" ", strip=True).lower()
        if "product monograph/veterinary labelling" in label:
            text = right.get_text(" ", strip=True)
            m = DATE_RX.search(text)
            if m: lab_date = m.group(0)
            a = right.select_one('a[href$=".PDF"], a[href$=".pdf"]')
            if a and a.get("href"):
                lab_url = urljoin(BASE, a["href"])
            break
    if not lab_url:
        for row in soup.select("div.row"):
            left = row.select_one("p.col-sm-4 strong")
            right = row.select_one("p.col-sm-8")
            if not left or not right:
                continue
            if "din" in left.get_text(" ", strip=True).lower():
                msg = right.get_text(" ", strip=True)
                if "Electronic product monograph is not available" in msg:
                    lab_url = ""
                break
    return lab_url, lab_date

def parse_active_ingredients_multiline(soup: BeautifulSoup) -> str:
    table = None
    for cap in soup.find_all("caption"):
        if "List of active ingredient(s)" in cap.get_text():
            table = cap.find_parent("table"); break
    if not table: return ""
    lines = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) != 2: continue
        name = norm(tds[0].get_text(" "))
        strength = norm(tds[1].get_text(" "))
        if name:
            lines.append(f"{name} : {strength}" if strength else name)
    return "\n".join(lines)

def parse_biosimilar_flag(soup: BeautifulSoup) -> str:
    for row in soup.select("div.row"):
        left = row.select_one("p.col-sm-4 strong")
        right = row.select_one("p.col-sm-8")
        if not left or not right:
            continue
        if "biosimilar biologic drug" in left.get_text(" ").lower():
            val = norm(right.get_text(" "))
            return "Yes" if val.lower().startswith("yes") else "No"
    return "No"

def fetch_detail_fields(sess: requests.Session, din_url: str, sleep: float=0.2) -> Dict[str, str]:
    if not din_url:
        return {}
    r = _with_retries(lambda: sess.get(din_url, timeout=TIMEOUT))
    soup = BeautifulSoup(r.text, "html.parser")
    data = {
        "Current status date": get_right_value_for_label_contains(soup, "Current status date"),
        "Original market date": get_right_value_for_label_contains(soup, "Original market date"),
        "Class": get_right_value_for_label_contains(soup, "Class:"),
        "Dosage form": get_right_value_for_label_contains(soup, "Dosage form"),
        "Route(s) of administration": get_right_value_for_label_contains(soup, "Route(s) of administration"),
        "Number of active ingredient(s)": get_right_value_for_label_contains(soup, "Number of active ingredient"),
        "Schedule": get_right_value_for_label_contains(soup, "Schedule"),
        "American Hospital Formulary Service (AHFS)": get_right_value_for_label_contains(soup, "American Hospital Formulary Service"),
        "Anatomical Therapeutic Chemical (ATC)": get_right_value_for_label_contains(soup, "Anatomical Therapeutic Chemical"),
        "Active ingredient group (AIG) number": get_right_value_for_label_contains(soup, "Active ingredient group"),
    }
    data.update(parse_company_block(soup))
    lab_url, lab_date = parse_labelling_link_and_date(soup)
    data["Labelling"] = lab_url
    data["Product Monograph/Veterinary Date"] = lab_date
    data["List of active ingredient"] = parse_active_ingredients_multiline(soup)
    data["Biosimilar Biologic Drug"] = parse_biosimilar_flag(soup)
    if sleep and sleep > 0:
        time.sleep(sleep)
    return data

# ----------------------------
# Sharding helpers
# ----------------------------
BRAND_SYMBOL_PREFIXES = list("()[]{}#&+-,.'/")
def build_brand_prefixes() -> list[str]:
    return list(string.ascii_uppercase) + list(string.digits) + BRAND_SYMBOL_PREFIXES
def build_din_prefixes() -> list[str]:
    return list(string.digits)

# ----------------------------
# Collect list rows (paging + shard sweeps)
# ----------------------------
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
    1) If page 1 has rows, try server-side pagination on the baseline endpoint.
    2) If pagination doesn't advance, sweep brandName & DIN shards using:
       POST preferred_post_url (w/ CSRF) → POST DISPATCH_URL → DT JSON (/getNextPage) → HTML fallback.
    3) Normalize DT JSON cells (strip tags, unescape) and map DIN to "DIN" key to match HTML parser.
    4) Respect max_rows cap and show progress heartbeats.
    """
    import html as _html
    import re as _re
    preferred_post_url = (post_endpoint_url or DISPATCH_URL or "").strip()

    if preferred_post_url.endswith("/dispatch-repartition"):
        dbg("[SWEEP] preferred_post_url pointed at dispatch-repartition (GET-only); switching to /dpd-bdpp/search-recherche")
        preferred_post_url = BASE.rstrip("/") + "/dpd-bdpp/search-recherche"
    if "canada.ca/en/sr/srb.html" in (preferred_post_url or ""):
        preferred_post_url = BASE.rstrip("/") + "/dpd-bdpp/search-recherche"

    dbg(f"[SWEEP] preferred_post_url={preferred_post_url!r} | force_post_sweeps={bool(SCRAPER_FORCE_POST_SWEEPS)}")

    _TAG_RE = _re.compile(r"<[^>]+>")
    _WS_RE  = _re.compile(r"\s+")

    def _clean_cell(v: Any) -> str:
        if v is None:
            return ""
        s = _html.unescape(str(v))
        s = _TAG_RE.sub("", s)
        s = _WS_RE.sub(" ", s).strip()
        return s

    def _dt_rows_to_common_rows(aa: list) -> list[dict]:
        """Return list of dicts with at least a 'DIN' key populated (clean text)."""
        rows: list[dict] = []
        if not aa:
            return rows
        if isinstance(aa[0], dict):
            for d in aa:
                din_raw = d.get("DIN") or d.get("din") or d.get("Din") or list(d.values())[1] if len(d) >= 2 else ""
                rows.append({"DIN": _clean_cell(din_raw)})
        else:
            for row in aa:
                din_raw = row[1] if len(row) > 1 else ""
                rows.append({"DIN": _clean_cell(din_raw)})
        return rows

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

    # ----------------------------
    # 1) Baseline pagination probe (HTML)
    # ----------------------------
    if page1_rows:
        paging_keys = [
            lambda p: {"results_page": p},
            lambda p: {"page": p},
            lambda p: {"p": p},
            lambda p: {"start": (p - 1) * per_page},
        ]
        used_key = None
        used_method = endpoint_method
        for strat in paging_keys:
            payload = {**base_filters, **strat(2), "lang": "eng", "wbdisable": "true"}
            try:
                html2, used_method = _fetch_page(sess, endpoint_url, used_method, payload)
                r2 = parse_list_page_rows(html2)
                new_vs_p1 = len({r.get("DIN","") for r in r2} - {r.get("DIN","") for r in page1_rows})
                dbg(f"Probe strategy {list(strat(2).keys())[0]} ({used_method}) -> {len(r2)} rows; new vs p1: {new_vs_p1}")
                if r2 and new_vs_p1 > 0:
                    used_key = strat
                    seen = {_canon_din(r.get("DIN","")) for r in rows_all}
                    added = 0
                    for rr in r2:
                        din_val = _canon_din(rr.get("DIN",""))
                        if din_val and din_val not in seen:
                            rows_all.append(rr); seen.add(din_val); added += 1
                    dbg(f"Pagination probe accepted; added {added}, cum={len(rows_all)}")
                    _heartbeat(len(rows_all), max_rows or None)
                    if _hit_cap(): return rows_all[:max_rows]
                    break
            except Exception as e:
                dbg(f"Probe on {endpoint_url} failed: {e}")
            time.sleep(sleep)

        if used_key:
            stall = 0
            for page in range(3, max_pages + 1):
                payload = {**base_filters, **used_key(page), "lang": "eng", "wbdisable": "true"}
                try:
                    htmlp, used_method = _fetch_page(sess, endpoint_url, used_method, payload)
                except Exception as e:
                    dbg(f"Page {page} fetch failed: {e}")
                    break
                chunk_rows = parse_list_page_rows(htmlp)
                dbg(f"Page {page} ({used_method}): {len(chunk_rows)} rows")
                if not chunk_rows:
                    break
                seen = {_canon_din(r.get("DIN","")) for r in rows_all}
                added = 0
                for rr in chunk_rows:
                    din_val = _canon_din(rr.get("DIN",""))
                    if din_val and din_val not in seen:
                        rows_all.append(rr); seen.add(din_val); added += 1
                if added == 0:
                    stall += 1
                    if stall >= PAGE_STALL_LIMIT:
                        dbg(f"Pagination stalled ({stall} pages with 0 new); stopping page loop.")
                        break
                else:
                    stall = 0
                dbg(f"Page {page}: added {added} new (cum={len(rows_all)})")
                _heartbeat(len(rows_all), max_rows or None)
                if _hit_cap(): return rows_all[:max_rows]
                time.sleep(sleep)
            if total or len(rows_all) >= min_rows:
                return rows_all if not _hit_cap() else rows_all[:max_rows]

    # ----------------------------
    # 2) Sharded sweeps POST→DT JSON→HTML
    # ----------------------------
    dbg("Falling back to sharded sweeps (POST→GET)...")

    def _with_csrf(extra: dict) -> dict:
        try:
            token = get_csrf(sess)
        except Exception:
            token = base_filters.get("_csrf", "")
        data = {**base_filters, **extra}
        if token:
            data["_csrf"] = token
        data.setdefault("lang", "eng")
        data.setdefault("wbdisable", "true")
        return data

    def _post_then_get(filter_key: str, value: str, page: int) -> tuple[list[dict], str]:
        """
        page=1: POST (set filters into session)
        page>1: GET DT JSON (/getNextPage) with legacy DT params
        fallback: HTML GET on RESULTS_URL with generic page keys
        Returns (rows, origin_label)
        """
        DT_COLS = ["status","din","company","brand","drugClass","pm","schedule","aiNum","majorAI","AIStrength"]

        def _try_post(url: str) -> list[dict]:
            if "dispatch-repartition" in url:
                dbg(f"[POST skip] {url} is GET-only; skipping POST")
                return []

            payload = _with_csrf({filter_key: value})
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": BASE,
                "Referer": FORM_URL,
            }
            dbg(f"[POST try] {url} {filter_key}='{value}' page=1 (no page key)")

            try:
                r = _with_retries(lambda: sess.post(
                    url, data=payload, timeout=TIMEOUT, allow_redirects=True, headers=headers
                ))
                if getattr(r, "status_code", None) in (405, 404):
                    dbg(f"[POST {r.status_code}] {url} — treating as no-op and falling back")
                    return []
            except requests.HTTPError as e:
                resp = getattr(e, "response", None)
                code = getattr(resp, "status_code", None)
                if code in (405, 404):
                    dbg(f"[POST {code}] {url} — skipping POST sweep")
                    return []
                raise  

            if "canada.ca/en/sr/srb.html" in (r.url or ""):
                dbg("[POST bounce] SRB relay detected")
                return []

            rows = parse_list_page_rows(r.text)
            dbg(f"[POST got] {len(rows)} rows from page 1")
            return rows


        # DT JSON path
        api = page_api_url or (BASE.rstrip("/") + "/dpd-bdpp/getNextPage")
        start = (page - 1) * per_page
        if not hasattr(_post_then_get, "_dt_echo"): _post_then_get._dt_echo = 1
        echo = _post_then_get._dt_echo; _post_then_get._dt_echo += 1

        params = {
            "sEcho": str(echo),
            "iColumns": str(len(DT_COLS)),
            "sColumns": ",".join(DT_COLS),
            "iDisplayStart": str(start),
            "iDisplayLength": str(per_page),
            "iSortingCols": "1",
            "iSortCol_0": "3",
            "sSortDir_0": "asc",
            "sSearch": "",
            "bRegex": "false",
            "lang": "eng",
            "wbdisable": "true",
            "_": str(int(time.time() * 1000)),
        }
        for i, name in enumerate(DT_COLS):
            params[f"mDataProp_{i}"] = name
            params[f"sSearch_{i}"] = ""
            params[f"bRegex_{i}"] = "false"
            params[f"bSearchable_{i}"] = "true"
            params[f"bSortable_{i}"] = "false" if name == "AIStrength" else "true"

        headers = {"Referer": FORM_URL, "X-Requested-With": "XMLHttpRequest", "Accept": "application/json, text/javascript, */*; q=0.01"}

        dbg(f"[DT GET] {api} start={start} len={per_page} echo={echo}")
        try:
            r = _with_retries(lambda: sess.get(api, params=params, timeout=TIMEOUT, headers=headers))
            try:
                j = r.json()
            except Exception:
                j = json.loads(r.text)
            aa = j.get("aaData") or j.get("data") or []
            rows = _dt_rows_to_common_rows(aa)
            dbg(f"[DT GET got] {len(rows)} rows from start={start}")
            if rows:
                return rows, "GET(JSON)"
        except Exception as e:
            dbg(f"[DT GET error] {e!r}")

        # HTML fallback
        try:
            base = {filter_key: value, "lang": "eng", "wbdisable": "true"}
            start_off = start
            for key in PAGE_KEYS:
                payload2 = dict(base)
                payload2[key] = page if key not in ("start", "iDisplayStart") else start_off
                dbg(f"[GET try] {RESULTS_URL} {filter_key}='{value}' page={page} via {key}")
                html, _ = _fetch_page(sess, RESULTS_URL, "GET", payload2)
                rows = parse_list_page_rows(html)
                dbg(f"[GET got] {len(rows)} rows via {key}")
                if rows:
                    return rows, "GET(HTML)"
        except Exception as e:
            dbg(f"[GET error] {e!r}")

        return [], "GET(EMPTY)"

    def _sweep_one(filter_key: str, value: str) -> Tuple[int, bool]:
        """Return (added_total, saw_any_rows_at_all) for a single prefix value."""
        added_total = 0
        saw_any = False

        # page 1
        rows, meth = _post_then_get(filter_key, value, 1)
        ep_name = "POST@" + preferred_post_url if meth == "POST" else "RESULTS_URL"
        _log_prefix_try(filter_key, value, ep_name, 1, len(rows), len(rows_all))
        if rows:
            saw_any = True
            seen = {_canon_din(r.get("DIN","")) for r in rows_all}
            for rr in rows:
                din_val = _canon_din(rr.get("DIN",""))
                if din_val and din_val not in seen:
                    rows_all.append(rr); seen.add(din_val); added_total += 1
                    _heartbeat(len(rows_all), max_rows or None)
                    if _hit_cap():
                        return added_total, True

        # paged (DT JSON first, HTML only if empty)
        stall = 0
        for page in range(2, SWEEP_PAGE_LIMIT + 1):
            rows_p, meth_p = _post_then_get(filter_key, value, page)
            ep_name_p = "DT JSON" if meth_p == "GET(JSON)" else ("RESULTS_URL" if "HTML" in meth_p else meth_p)
            _log_prefix_try(filter_key, value, ep_name_p, page, len(rows_p), len(rows_all))
            if not rows_p:
                break
            saw_any = True
            added_this_page = 0
            seen = {_canon_din(r.get("DIN","")) for r in rows_all}
            for rr in rows_p:
                din_val = _canon_din(rr.get("DIN",""))
                if din_val and din_val not in seen:
                    rows_all.append(rr); seen.add(din_val)
                    added_total += 1; added_this_page += 1
                    _heartbeat(len(rows_all), max_rows or None)
                    if _hit_cap():
                        return added_total, True
            if added_this_page == 0:
                stall += 1
                if stall >= PAGE_STALL_LIMIT:
                    dbg(f"[{filter_key}='{value}'] no new uniques for {stall} pages; breaking")
                    break
            else:
                stall = 0
            time.sleep(max(sleep, 0.02))

        if added_total > 0:
            dbg(f"[{filter_key}='{value}'] +{added_total} new (cum={len(rows_all)})")
        return added_total, saw_any

    brand_prefixes = build_brand_prefixes()
    din_prefixes   = build_din_prefixes()

    def _run_prefix_group(kind: str, values: List[str]) -> bool:
        """Return True if we should stop (hit cap or early-stop), else False."""
        empty_streak = 0
        for i, v in enumerate(values, 1):
            if SWEEP_PREFIX_LOG_EVERY and i % SWEEP_PREFIX_LOG_EVERY == 0:
                dbg(f"[{kind}] prefix {i}/{len(values)} (cum={len(rows_all)})")
            added, saw_any = _sweep_one(kind, v)
            if not saw_any:
                empty_streak += 1
                if SCRAPER_SWEEP_MAX_EMPTY and empty_streak >= SCRAPER_SWEEP_MAX_EMPTY:
                    dbg(f"[{kind}] early stop after {empty_streak} empty prefixes in a row")
                    return True
            else:
                empty_streak = 0
            if _hit_cap():
                return True
            time.sleep(max(sleep, 0.02))
        return False

    # Sweep order
    if SCRAPER_SWEEP_ORDER == "brand-first":
        if _run_prefix_group("brandName", brand_prefixes): return rows_all[:max_rows] if _hit_cap() else rows_all
        if _run_prefix_group("din", din_prefixes):         return rows_all[:max_rows] if _hit_cap() else rows_all
    else:
        if _run_prefix_group("din", din_prefixes):         return rows_all[:max_rows] if _hit_cap() else rows_all
        if _run_prefix_group("brandName", brand_prefixes): return rows_all[:max_rows] if _hit_cap() else rows_all

    return rows_all if not _hit_cap() else rows_all[:max_rows]

# ----------------------------
# Public entrypoint
# ----------------------------
def run_full_scrape(
    max_depth: int = DEF_MAX_DEPTH,
    target_min_rows: int = DEF_TARGET_MIN_ROWS,
    enrich_flush_every: int = DEF_ENRICH_FLUSH_EVERY,
    request_sleep: float = DEF_REQUEST_SLEEP,
    max_rows: int = DEF_MAX_ROWS,   # 0 = no cap
) -> Tuple[List[Dict], Dict]:
    t0 = time.time()
    sess = make_session()

    first_html, base_filters, post_url = submit_search(sess, basic_html=True)

    # Baseline GET endpoint for a first pass (we’ll sweep via POST later)
    baseline_endpoint_url = RESULTS_URL
    baseline_endpoint_method = "GET"

    base_rows = collect_all_list_rows(
        sess=sess,
        first_html=first_html,
        base_filters=base_filters,
        endpoint_url=baseline_endpoint_url,
        endpoint_method=baseline_endpoint_method,
        min_rows=target_min_rows,
        sleep=request_sleep,
        max_rows=max_rows,
        post_endpoint_url=post_url,   # discovered POST action (or dispatcher)
    )
    dbg("List collected:", len(base_rows))


    # --- Enrich detail pages ---
    enriched: List[Dict] = []
    for i, r in enumerate(base_rows, 1):
        try:
            detail = fetch_detail_fields(sess, r.get("DIN URL",""), sleep=request_sleep)
        except Exception as e:
            dbg(f"Detail fetch failed for DIN {r.get('DIN','?')}: {e}")
            detail = {}

        merged = {col: "" for col in COLUMNS}
        merged.update(r)
        for k, v in (detail or {}).items():
            if v is None:
                continue
            if isinstance(v, str):
                v = v.replace("\r\n", "\n").replace("\r", "\n")
            merged[k] = v

        if not merged.get("Biosimilar Biologic Drug"):
            merged["Biosimilar Biologic Drug"] = "No"

        din = merged.get("DIN","")
        din_url = merged.get("DIN URL","")

        for col in COLUMNS:
            merged.setdefault(col, "")

        enriched.append(merged)
        if i % enrich_flush_every == 0:
            dbg(f"Enriched {i}/{len(base_rows)}")

    elapsed = round(time.time() - t0, 2)
    meta = {
        "strategy": "baseline-probe + shard-sweeps(POST→GET)",
        "elapsed_sec": elapsed,
        "rows": len(enriched),
        "request_sleep": request_sleep,
        "max_depth": max_depth,
        "target_min_rows": target_min_rows,
        "max_rows": max_rows,
        "baseline": {"endpoint": baseline_endpoint_url, "method": baseline_endpoint_method},
        "post_action": post_url,
    }
    dbg("Done. Rows:", len(enriched), "Elapsed(s):", elapsed)
    return enriched, meta

# ----------------------------
# CLI (sample / full)
# ----------------------------
if __name__ == "__main__":
    import argparse, pandas as pd
    from openpyxl.styles import Alignment
    from openpyxl.utils import get_column_letter

    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=int, default=0, help="Grab only N rows from the first results page.")
    parser.add_argument("--xlsx", type=str, default="dpd_output.xlsx", help="Where to write the Excel file.")
    parser.add_argument("--out-json", type=str, default="", help="Optional: also write JSON snapshot here.")
    parser.add_argument("--max-rows", type=int, default=DEF_MAX_ROWS, help="Hard cap on collected rows (0=no cap).")
    args = parser.parse_args()

    if args.sample > 0:
        sess = make_session()
        first_html, base_filters, post_url = submit_search(sess, basic_html=False)
        list_rows = collect_all_list_rows(
            sess=sess,
            first_html=first_html,
            base_filters=base_filters,
            endpoint_url=RESULTS_URL,
            endpoint_method="GET",
            min_rows=args.sample,
            sleep=DEF_REQUEST_SLEEP,
            sample_n=args.sample,
            max_rows=args.max_rows,
            post_endpoint_url=post_url,
        )
        enriched: List[Dict] = []
        for i, r in enumerate(list_rows, 1):
            detail = fetch_detail_fields(sess, r.get("DIN URL",""))
            merged = {col: "" for col in COLUMNS}; merged.update(r)
            for k, v in (detail or {}).items():
                if v is None: continue
                if isinstance(v, str): v = v.replace("\r\n","\n").replace("\r","\n")
                merged[k] = v
            if not merged.get("Biosimilar Biologic Drug"): merged["Biosimilar Biologic Drug"] = "No"
            din = merged.get("DIN",""); din_url = merged.get("DIN URL","")
            for col in COLUMNS: merged.setdefault(col,"")
            enriched.append(merged)
            if i % 5 == 0: dbg(f"[SAMPLE] Enriched {i}/{len(list_rows)}")

        _write_xlsx(enriched, args.xlsx, sheet_name="sample")
        print(f"[SAMPLE] Wrote {len(enriched)} rows to {args.xlsx}")
        if args.out_json:
            os.makedirs(os.path.dirname(args.out_json) or ".", exist_ok=True)
            with open(args.out_json, "w", encoding="utf-8") as f:
                json.dump({"meta": {"mode":"sample","max_rows":args.max_rows}, "rows": enriched}, f, ensure_ascii=False, indent=2)
            print(f"[SAMPLE] Wrote JSON snapshot to {args.out_json}")

    else:
        rows, meta = run_full_scrape(
            target_min_rows=DEF_TARGET_MIN_ROWS,
            max_depth=DEF_MAX_DEPTH,
            request_sleep=DEF_REQUEST_SLEEP,
            max_rows=DEF_MAX_ROWS,
        )
        if os.getenv("OUT_JSON",""):
            path = os.getenv("OUT_JSON")
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"meta": meta, "rows": rows}, f, ensure_ascii=False, indent=2)
        print({"rows": len(rows), **meta})
