# dpd_scraper/dpd_scraper.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Optional, List, Dict, Tuple

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
RESULTS_URL   = f"{BASE}/dpd-bdpp/search-fast-recherche-rapide"

TIMEOUT       = int(os.getenv("SCRAPER_TIMEOUT", "90"))
RETRIES       = int(os.getenv("SCRAPER_RETRIES", "5"))
RETRY_SLEEP   = float(os.getenv("SCRAPER_RETRY_SLEEP", "1.2"))

DEF_MAX_DEPTH          = int(os.getenv("SCRAPER_MAX_DEPTH", "1"))
DEF_TARGET_MIN_ROWS    = int(os.getenv("SCRAPER_TARGET_MIN_ROWS", "2000"))
DEF_ENRICH_FLUSH_EVERY = int(os.getenv("SCRAPER_ENRICH_FLUSH_EVERY", "50"))
DEF_REQUEST_SLEEP      = float(os.getenv("SCRAPER_REQUEST_SLEEP", "0.08"))

# 💡 New: hard cap for how many rows to collect (dev=5000, prod≈59000)

DEF_MAX_ROWS           = int(os.getenv("SCRAPER_MAX_ROWS", "0"))  # already present
SCRAPER_SWEEP_ENDPOINT = os.getenv("SCRAPER_SWEEP_ENDPOINT", "").strip()  # NEW
SCRAPER_SWEEP_ORDER    = os.getenv("SCRAPER_SWEEP_ORDER", "din-first").strip().lower()  # NEW
SCRAPER_SWEEP_MAX_EMPTY= int(os.getenv("SCRAPER_SWEEP_MAX_EMPTY", "0"))  # NEW (0 = no early stop)


# optional override like: SCRAPER_BASELINE=DISPATCH_URL:GET
SCRAPER_BASELINE = os.getenv("SCRAPER_BASELINE", "").strip()

ENDPOINTS_FOR_SWEEP: list[tuple[str, str, str]] = [
    ("RESULTS_URL",  RESULTS_URL,  "GET"),
    ("DISPATCH_URL", DISPATCH_URL, "GET"),
]

DEBUG = True
def dbg(*args):
    if DEBUG: print("[DEBUG]", *args)

# ----------------------------
# Columns
# ----------------------------
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
SPACES_RX = re.compile(r"[ \t\r\f\v]+")
DATE_RX   = re.compile(r"\b(19|20)\d{2}-\d{2}-\d{2}\b")

def norm(x: str | None) -> str:
    if not x: return ""
    x = x.replace("\xa0", " ").replace("\u200b"," ")
    x = SPACES_RX.sub(" ", x).strip()
    return x

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
    if last: raise last

def _extract_total_entries(html: str) -> int | None:
    m = re.search(r"(?:of|sur)\s+([0-9][0-9\s,\.]*)\s+(?:entries|entrées)", html, flags=re.I)
    if not m: return None
    try:
        raw = m.group(1).replace("\u202f", " ").replace("\xa0", " ")
        raw = raw.replace(" ", "").replace(",", "").replace(".", "")
        return int(raw)
    except Exception:
        return None

def _fetch_page(sess: requests.Session, url: str, method: str, payload: dict) -> tuple[str, str]:
    method = method.upper()
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
    soup = BeautifulSoup(r.text, "html.parser")
    form = soup.find("form")
    if not form:
        return RESULTS_URL, {}
    action_url = urljoin(BASE, form.get("action") or RESULTS_URL)
    defaults = {}
    for inp in soup.find_all("input"):
        name = inp.get("name")
        if not name: continue
        t = (inp.get("type") or "").lower()
        if t in ("checkbox","radio"):
            if inp.has_attr("checked"):
                defaults.setdefault(name, inp.get("value","on"))
        else:
            defaults.setdefault(name, inp.get("value",""))
    if "_csrf" not in defaults:
        csrf = get_csrf(sess)
        if csrf: defaults["_csrf"] = csrf
    return action_url, defaults

def parse_list_page_rows(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", id="results")
    if not table:
        return []
    rows = []
    for tr in table.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) >= 10:
            din_cell = tds[1]
            a = din_cell.find("a", href=True)
            din_url = urljoin(BASE, a["href"]) if a else ""
            rows.append({
                "Status": norm(tds[0].get_text(" ")),
                "DIN URL": din_url,
                "DIN": norm(din_cell.get_text(" ")),
                "Company": norm(tds[2].get_text(" ")),
                "Product": norm(tds[3].get_text(" ")),
                "Class": norm(tds[4].get_text(" ")),
                "PM See footnote1": norm(tds[5].get_text(" ")),
                "Schedule": norm(tds[6].get_text(" ")),
                "# See footnote2": norm(tds[7].get_text(" ")),
                "A.I. name See footnote3": norm(tds[8].get_text(" ")),
                "Strength": norm(tds[9].get_text(" ")),
                "Biosimilar Biologic Drug": "No",
            })
    return rows

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
    # 1) explicit override
    if SCRAPER_SWEEP_ENDPOINT:
        try:
            name, meth = SCRAPER_SWEEP_ENDPOINT.split(":")
            url = RESULTS_URL if name.strip() == "RESULTS_URL" else DISPATCH_URL
            yield (name.strip(), url, meth.strip().upper()); seen.add(url)
        except Exception:
            pass
    # 2) baseline
    if baseline_url in (DISPATCH_URL, RESULTS_URL) and baseline_url not in seen:
        name = "DISPATCH_URL" if baseline_url == DISPATCH_URL else "RESULTS_URL"
        yield (name, baseline_url, "GET"); seen.add(baseline_url)
    # 3) the other one
    for nm, url, meth in [("RESULTS_URL", RESULTS_URL, "GET"), ("DISPATCH_URL", DISPATCH_URL, "GET")]:
        if url not in seen:
            yield (nm, url, meth)

def submit_search(sess: requests.Session, basic_html: bool = True) -> tuple[str, dict, str, str]:
    """
    Returns: (first_html, base_filters, endpoint_url, method_used)
    We probe endpoints if page-1 is empty.
    """
    action_url, defaults = _discover_form(sess)
    base_filters = {
        "din":"", "atc":"", "companyName":"", "brandName":"", "activeIngredient":"", "aigNumber":"",
        "biosimDrugSearch":"", "lang":"eng", "wbdisable":"true" if basic_html else "true",
    }
    payload = {**defaults, **base_filters}

    try:
        html, method = _fetch_page(sess, action_url, "POST", payload)
    except Exception:
        html, method = _fetch_page(sess, RESULTS_URL, "GET", {"lang":"eng","wbdisable":"true"})

    rows = parse_list_page_rows(html)
    if rows:
        return html, base_filters, action_url, method

    dbg("Page 1 is empty; probing endpoints to select baseline...")
    for name, url, meth in _baseline_candidates():
        try:
            html2, meth2 = _fetch_page(sess, url, meth, base_filters)
            r2 = parse_list_page_rows(html2)
            dbg(f"Probe baseline {name}:{meth2} -> rows={len(r2)}")
            if r2:
                dbg(f"Baseline endpoint selected: {name} ({meth2})")
                return html2, base_filters, url, meth2
        except Exception as e:
            dbg(f"Baseline probe {name}:{meth} failed: {e}")

    return html, base_filters, RESULTS_URL, "GET"

# ----------------------------
# Detail page parsing (unchanged)
# ----------------------------
def get_right_value_for_label_contains(soup: BeautifulSoup, label_substring: str) -> str:
    needle = label_substring.lower()
    for row in soup.select("div.row"):
        left = row.select_one("p.col-sm-4 strong")
        right = row.select_one("p.col-sm-8")
        if not left or not right: continue
        if needle in left.get_text(" ").lower():
            return norm(right.get_text(" ").replace("\xa0"," "))
    return ""

def parse_company_block(soup: BeautifulSoup) -> Dict[str,str]:
    out = {"Company":"", "Address":"", "City":"", "state":"", "Country":"", "Zipcode":""}
    for row in soup.select("div.row"):
        left = row.select_one("p.col-sm-4 strong")
        if not left: continue
        if "company" in left.get_text(" ").lower():
            right = row.select_one("p.col-sm-8")
            if not right: break
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
        if not left or not right: continue
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
            if not left or not right: continue
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
        if not left or not right: continue
        if "biosimilar biologic drug" in left.get_text(" ").lower():
            val = norm(right.get_text(" "))
            return "Yes" if val.lower().startswith("yes") else "No"
    return "No"

def fetch_detail_fields(sess: requests.Session, din_url: str, sleep: float=0.2) -> Dict[str, str]:
    if not din_url: return {}
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
    if sleep and sleep > 0: time.sleep(sleep)
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
# Collect list rows (paging + shard sweeps) — now with max_rows hard cap
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
) -> List[Dict]:
    rows_all: List[Dict] = []
    page1_rows = parse_list_page_rows(first_html)
    rows_all.extend(page1_rows)
    dbg("Page 1 rows:", len(page1_rows))

    if sample_n:
        return rows_all[:sample_n]

    def _hit_cap() -> bool:
        return max_rows > 0 and len(rows_all) >= max_rows

    if _hit_cap():
        return rows_all[:max_rows]

    total = _extract_total_entries(first_html)
    dbg("Total (from info):", total if total else "not shown; will probe")
    per_page = len(page1_rows) if page1_rows else 25
    max_pages = (total + per_page - 1) // per_page if total else 100

    # ----------------------------
    # 1) Try server-side pagination only if page1 had rows
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
                new_vs_p1 = len({r["DIN"] for r in r2} - {r["DIN"] for r in page1_rows})
                dbg(f"Probe strategy {list(strat(2).keys())[0]} ({used_method}) -> {len(r2)} rows; new vs p1: {new_vs_p1}")
                if r2 and new_vs_p1 > 0:
                    used_key = strat
                    seen = {r["DIN"] for r in rows_all}
                    for rr in r2:
                        din_val = rr.get("DIN", "")
                        if din_val and din_val not in seen:
                            rows_all.append(rr); seen.add(din_val)
                    if _hit_cap():
                        return rows_all[:max_rows]
                    break
            except Exception as e:
                dbg(f"Probe on {endpoint_url} failed: {e}")
            time.sleep(sleep)

        if used_key:
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
                seen = {r["DIN"] for r in rows_all}
                for rr in chunk_rows:
                    din_val = rr.get("DIN", "")
                    if din_val and din_val not in seen:
                        rows_all.append(rr); seen.add(din_val)
                if _hit_cap():
                    return rows_all[:max_rows]
                time.sleep(sleep)
            if total or len(rows_all) >= min_rows:
                return rows_all if not _hit_cap() else rows_all[:max_rows]

    # ----------------------------
    # 2) Sharded sweeps (GET filters only; no CSRF)
    # ----------------------------
    dbg("Falling back to sharded sweeps (GET)...")
    seen = {r["DIN"] for r in rows_all}

    # local helper: endpoint order for FILTERED sweeps
    def _sweep_endpoint_candidates(baseline_url: Optional[str] = None):
        seen_urls = set()
        if SCRAPER_SWEEP_ENDPOINT:
            try:
                name, meth = SCRAPER_SWEEP_ENDPOINT.split(":")
                url = RESULTS_URL if name.strip() == "RESULTS_URL" else DISPATCH_URL
                yield (name.strip(), url, meth.strip().upper()); seen_urls.add(url)
            except Exception:
                pass
        if baseline_url in (DISPATCH_URL, RESULTS_URL) and baseline_url not in seen_urls:
            name = "DISPATCH_URL" if baseline_url == DISPATCH_URL else "RESULTS_URL"
            yield (name, baseline_url, "GET"); seen_urls.add(baseline_url)
        for nm, url, meth in [("RESULTS_URL", RESULTS_URL, "GET"), ("DISPATCH_URL", DISPATCH_URL, "GET")]:
            if url not in seen_urls:
                yield (nm, url, meth)

    # try one prefix; return (added_count, saw_any_rows_on_any_endpoint)
    def _sweep_one(filter_key: str, value: str) -> Tuple[int, bool]:
        added_total = 0
        saw_any = False
        for ep_name, ep_url, _ in _sweep_endpoint_candidates(endpoint_url):
            try:
                payload = {**base_filters, filter_key: value, "lang": "eng", "wbdisable": "true"}
                html, _ = _fetch_page(sess, ep_url, "GET", payload)
                chunk = parse_list_page_rows(html)
                if chunk:
                    saw_any = True
                else:
                    continue  # try next endpoint immediately

                for rr in chunk:
                    din_val = rr.get("DIN", "")
                    if din_val and din_val not in seen:
                        rows_all.append(rr); seen.add(din_val); added_total += 1
                        if _hit_cap():
                            return added_total, True

                # light paging for this filter
                for page in range(2, 8):
                    paged = payload | {"results_page": page}
                    html2, _ = _fetch_page(sess, ep_url, "GET", paged)
                    chunk2 = parse_list_page_rows(html2)
                    if not chunk2:
                        break
                    for rr in chunk2:
                        din_val = rr.get("DIN", "")
                        if din_val and din_val not in seen:
                            rows_all.append(rr); seen.add(din_val); added_total += 1
                            if _hit_cap():
                                return added_total, True

                if added_total > 0:
                    dbg(f"[{ep_name}:{filter_key}='{value}'] +{added_total} (cum={len(rows_all)})")
                    break  # stop trying other endpoints for this value once it worked
            except Exception as e:
                dbg(f"[{ep_name}:{filter_key}='{value}'] error: {e}")
        return added_total, saw_any

    brand_prefixes = build_brand_prefixes()
    din_prefixes   = build_din_prefixes()

    def _run_prefix_group(kind: str, values: List[str]) -> bool:
        """
        Returns True to stop overall sweep (cap/early-stop), False to continue.
        Empty streak only counts when *no* endpoint returned any rows for that prefix.
        """
        empty_streak = 0
        for v in values:
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

    # adaptive order: start with requested order; if first 3 shards all empty across endpoints, switch
    order = (SCRAPER_SWEEP_ORDER or "din-first").lower()
    def _probe_three(kind: str, values: List[str]) -> Tuple[int, int]:
        # returns (added_total, nonempty_prefixes)
        added_sum, nonempty = 0, 0
        for v in values[:3]:
            a, saw_any = _sweep_one(kind, v)
            added_sum += a
            nonempty += 1 if saw_any else 0
            if _hit_cap():
                break
            time.sleep(max(sleep, 0.02))
        return added_sum, nonempty

    if order == "din-first":
        added_probe, nonempty = _probe_three("din", din_prefixes)
        if _hit_cap():
            return rows_all[:max_rows]
        if nonempty == 0:
            dbg("DIN shards look empty; switching to brand-first")
            if _run_prefix_group("brandName", brand_prefixes):
                return rows_all[:max_rows] if _hit_cap() else rows_all
            if _run_prefix_group("din", din_prefixes[3:]):
                return rows_all[:max_rows] if _hit_cap() else rows_all
        else:
            # continue with remaining DIN, then brands
            if _run_prefix_group("din", din_prefixes[3:]):
                return rows_all[:max_rows] if _hit_cap() else rows_all
            if _run_prefix_group("brandName", brand_prefixes):
                return rows_all[:max_rows] if _hit_cap() else rows_all
    else:
        added_probe, nonempty = _probe_three("brandName", brand_prefixes)
        if _hit_cap():
            return rows_all[:max_rows]
        if nonempty == 0:
            dbg("Brand shards look empty; switching to din-first")
            if _run_prefix_group("din", din_prefixes):
                return rows_all[:max_rows] if _hit_cap() else rows_all
            if _run_prefix_group("brandName", brand_prefixes[3:]):
                return rows_all[:max_rows] if _hit_cap() else rows_all
        else:
            if _run_prefix_group("brandName", brand_prefixes[3:]):
                return rows_all[:max_rows] if _hit_cap() else rows_all
            if _run_prefix_group("din", din_prefixes):
                return rows_all[:max_rows] if _hit_cap() else rows_all

    return rows_all if not _hit_cap() else rows_all[:max_rows]

# ----------------------------
# Public entrypoint — now accepts max_rows
# ----------------------------
def run_full_scrape(
    max_depth: int = DEF_MAX_DEPTH,
    target_min_rows: int = DEF_TARGET_MIN_ROWS,
    enrich_flush_every: int = DEF_ENRICH_FLUSH_EVERY,
    request_sleep: float = DEF_REQUEST_SLEEP,
    max_rows: int = DEF_MAX_ROWS,    # 👈 NEW
) -> Tuple[List[Dict], Dict]:
    t0 = time.time()
    sess = make_session()

    first_html, base_filters, endpoint_url, endpoint_method = submit_search(sess, basic_html=True)
    base_rows = collect_all_list_rows(
        sess=sess,
        first_html=first_html,
        base_filters=base_filters,
        endpoint_url=endpoint_url,
        endpoint_method=endpoint_method,
        min_rows=target_min_rows,
        sleep=request_sleep,
        max_rows=max_rows,
    )
    dbg("List collected:", len(base_rows))

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
            if v is None: continue
            if isinstance(v, str):
                v = v.replace("\r\n","\n").replace("\r","\n")
            merged[k] = v

        if not merged.get("Biosimilar Biologic Drug"):
            merged["Biosimilar Biologic Drug"] = "No"

        din = merged.get("DIN",""); din_url = merged.get("DIN URL","")

        for col in COLUMNS: merged.setdefault(col, "")

        enriched.append(merged)
        if i % enrich_flush_every == 0:
            dbg(f"Enriched {i}/{len(base_rows)}")

    elapsed = round(time.time() - t0, 2)
    meta = {
        "strategy": "baseline-probe + shard-sweeps(GET)",
        "elapsed_sec": elapsed,
        "rows": len(enriched),
        "request_sleep": request_sleep,
        "max_depth": max_depth,
        "target_min_rows": target_min_rows,
        "max_rows": max_rows,
        "baseline": {"endpoint": endpoint_url, "method": endpoint_method},
    }
    dbg("Done. Rows:", len(enriched), "Elapsed(s):", elapsed)
    return enriched, meta

# ----------------------------
# CLI
# ----------------------------
if __name__ == "__main__":
    import argparse, pandas as pd
    from openpyxl.styles import Alignment
    from openpyxl.utils import get_column_letter
    import os

    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=int, default=0, help="Grab only N rows from the first results page.")
    parser.add_argument("--xlsx", type=str, default="dpd_output.xlsx", help="Where to write the Excel file.")
    parser.add_argument("--out-json", type=str, default="", help="Optional: also write JSON snapshot here.")
    parser.add_argument("--max-rows", type=int, default=DEF_MAX_ROWS, help="Hard cap on collected rows (0=no cap).")
    args = parser.parse_args()

    def _write_xlsx(rows: List[Dict], path: str, sheet_name: str):
        import pandas as pd
        df = pd.DataFrame(rows, columns=COLUMNS)
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with pd.ExcelWriter(path, engine="openpyxl") as xlw:
            df.to_excel(xlw, index=False, sheet_name=sheet_name)
            ws = xlw.sheets[sheet_name]
            wrap_cols = {
                "List of active ingredient","Labelling","Product Monograph/Veterinary Date",
                "American Hospital Formulary Service (AHFS)","Anatomical Therapeutic Chemical (ATC)",
                "Address","A.I. name See footnote3","Strength",
            }
            header = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
            from openpyxl.utils import get_column_letter
            from openpyxl.styles import Alignment
            for idx, col_name in enumerate(header, start=1):
                ws.column_dimensions[get_column_letter(idx)].width = min(max(len(col_name) * 1.1, 18), 60)
                if col_name in wrap_cols:
                    for cell in ws.iter_cols(min_col=idx, max_col=idx, min_row=2, max_row=ws.max_row):
                        cell[0].alignment = Alignment(wrap_text=True, vertical="top")

    if args.sample > 0:
        sess = make_session()
        first_html, base_filters, endpoint_url, endpoint_method = submit_search(sess, basic_html=False)
        list_rows = collect_all_list_rows(
            sess=sess, first_html=first_html, base_filters=base_filters,
            endpoint_url=endpoint_url, endpoint_method=endpoint_method,
            min_rows=args.sample, sleep=DEF_REQUEST_SLEEP, sample_n=args.sample, max_rows=args.max_rows
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
            max_rows=DEF_MAX_ROWS
        )
        _write_xlsx(rows, "dpd_output.xlsx", sheet_name="full")
        print(f"[FULL] Wrote {len(rows)} rows to dpd_output.xlsx")
        if os.getenv("OUT_JSON",""):
            path = os.getenv("OUT_JSON")
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"meta": meta, "rows": rows}, f, ensure_ascii=False, indent=2)
        print({"rows": len(rows), **meta})
