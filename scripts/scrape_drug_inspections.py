#!/usr/bin/env python3
"""
Fetches Drug & Health Product inspection records from Health Canada's
public GMP database. Retrieves the first N listing rows and augments
each record with its inspection detail payload.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from openpyxl import Workbook

import requests
from requests import RequestException

BASE_URL = "https://www.drug-inspections.canada.ca/gmp"
SEARCH_ENDPOINT = f"{BASE_URL}/controller/searchResult.ashx"
DETAIL_ENDPOINT = f"{BASE_URL}/controller/inspectionDetail.ashx"

SEARCH_PARAMS: Dict[str, Optional[str]] = {
    "estName": "",
    "ref": "",
    "site": "",
    "rate": "",
    "term": "",
    "lic": "",
    "startDate": "",
    "endDate": "",
    "eType": "",
    "prov": "",
    "licNum": "",
    "act": "",
    "actCat": "",
    "cat": "",
    "pType": "GMP",
    "lang": "en",
}


class ScraperError(RuntimeError):
    """Domain specific error raised for scraping issues."""


def parse_ms_date(value: Optional[str]) -> Optional[str]:
    """
    Convert serialized MS JSON date (/Date(…)/) to ISO 8601.

    Returns a string in UTC (YYYY-MM-DDTHH:MM:SSZ) or None when conversion
    is not possible.
    """

    if not value or not value.startswith("/Date("):
        return None

    # Strip wrapper: /Date(1700000000000-0500)/
    payload = value[len("/Date(") : -2]  # trim leading "/Date(" and trailing ")/"
    match = re.fullmatch(r"(-?\d+)([+-]\d{4})?", payload)
    if not match:
        raise ScraperError(f"Unparsable MS date format: {value}")

    millis_part, offset_part = match.groups()
    millis = int(millis_part)

    if offset_part:
        sign = 1 if offset_part[0] == "+" else -1
        hours = int(offset_part[1:3])
        minutes = int(offset_part[3:5])
        offset_delta = timedelta(hours=hours, minutes=minutes) * sign
        tzinfo = timezone(offset_delta)
    else:
        tzinfo = timezone.utc

    dt = datetime.fromtimestamp(millis / 1000, tz=tzinfo)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")



def annotate_dates(record: Any) -> Any:
    """
    Recursively scan mapping/list and add *_iso companions
    for MS JSON date strings.
    """
    if isinstance(record, dict):
        updated: Dict[str, Any] = {}
        for key, value in record.items():
            updated[key] = annotate_dates(value)
            if isinstance(value, str):
                iso_value = parse_ms_date(value)
                if iso_value:
                    updated[f"{key}_iso"] = iso_value
        return updated

    if isinstance(record, list):
        return [annotate_dates(item) for item in record]

    return record


# -----------------------------------------------------------------------------
# HTTP helpers
# -----------------------------------------------------------------------------

def fetch_json(
    url: str,
    params: Dict[str, Any],
    session: Optional[requests.Session] = None,
) -> Any:
    http = session or requests
    try:
        response = http.get(url, params=params, timeout=60)
    except RequestException as exc:
        raise ScraperError(f"Request failed for {url}: {exc}") from exc
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise ScraperError(f"HTTP error for {url}: {exc}") from exc
    return response.json()


def fetch_detail(
    ins_number: int,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    params = {"lang": "en", "insNumber": ins_number}
    detail = fetch_json(DETAIL_ENDPOINT, params, session=session)
    if not isinstance(detail, dict):
        raise ScraperError(f"Unexpected detail payload for {ins_number}")
    return detail


def stream_listing_dataset(
    session: requests.Session,
    destination: Path,
    *,
    timeout: Tuple[float, float],
    chunk_size: int,
    max_retries: int,
    retry_delay: float,
) -> None:
    """Download the full listing dataset with streaming and retries."""

    attempt = 1
    while attempt <= max_retries:
        try:
            with session.get(
                SEARCH_ENDPOINT,
                params=SEARCH_PARAMS,
                timeout=timeout,
                stream=True,
            ) as resp:
                resp.raise_for_status()
                os.makedirs(destination.parent, exist_ok=True)
                size_bytes = 0
                started = time.time()
                print(
                    f"Streaming listing dataset (attempt {attempt}/{max_retries})...",
                    flush=True,
                )
                with open(destination, "wb") as fh:
                    for i, chunk in enumerate(resp.iter_content(chunk_size=chunk_size), 1):
                        if not chunk:
                            continue
                        fh.write(chunk)
                        size_bytes += len(chunk)
                        if i % 200 == 0:
                            elapsed = time.time() - started
                            print(
                                f"  {size_bytes/1_048_576:.2f} MB downloaded in {elapsed:.1f}s",
                                flush=True,
                            )
                print(
                    f"Listing download finished: {size_bytes/1_048_576:.2f} MB saved to {destination}",
                    flush=True,
                )
                return
        except (RequestException, OSError) as exc:
            print(
                f"Listing download error on attempt {attempt}/{max_retries}: {exc}",
                flush=True,
            )
            if attempt == max_retries:
                raise ScraperError(
                    f"Unable to download listing after {max_retries} attempts"
                ) from exc
            print(f"Retrying in {retry_delay} seconds...", flush=True)
            time.sleep(retry_delay)
            attempt += 1


def retrieve_listing_rows(
    session: requests.Session,
    limit: Optional[int],
    *,
    cache_path: Path,
    max_retries: int,
    retry_delay: float,
    stream_timeout: Tuple[float, float],
    stream_chunk_size: int,
) -> List[Dict[str, Any]]:
    """
    Retrieve listing rows either directly for small limits or via streaming cache.
    """

    if limit is not None and limit <= 1000:
        last_error: Optional[Exception] = None
        for attempt in range(1, max_retries + 1):
            try:
                payload = fetch_json(SEARCH_ENDPOINT, SEARCH_PARAMS, session=session)
                data = payload.get("data", [])
                if not isinstance(data, list):
                    raise ScraperError("Unexpected payload structure from listing API")
                return data[:limit]
            except ScraperError as exc:
                last_error = exc
                if attempt == max_retries:
                    break
                print(
                    f"Retry {attempt}/{max_retries} fetching listing data after error: {exc}. "
                    f"Waiting {retry_delay}s...",
                    flush=True,
                )
                time.sleep(retry_delay)
        raise ScraperError(
            f"Failed to retrieve listing data after {max_retries} attempts"
        ) from last_error

    if cache_path.exists():
        print(f"Using cached listing at {cache_path}", flush=True)
    else:
        stream_listing_dataset(
            session=session,
            destination=cache_path,
            timeout=stream_timeout,
            chunk_size=stream_chunk_size,
            max_retries=max_retries,
            retry_delay=retry_delay,
        )

    with open(cache_path, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    data = payload.get("data", [])
    if not isinstance(data, list):
        raise ScraperError("Cached listing payload malformed: missing 'data'")

    if limit:
        return data[:limit]
    return data


def save_records(records: Sequence[Dict[str, Any]], output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(list(records), fh, indent=2, ensure_ascii=False)


def flatten_record(record: Dict[str, Any]) -> Dict[str, Any]:
    flat: Dict[str, Any] = {}

    def visit(prefix: str, value: Any) -> None:
        key = prefix
        if isinstance(value, dict):
            for child_key, child_value in value.items():
                next_key = f"{prefix}.{child_key}" if prefix else child_key
                visit(next_key, child_value)
        elif isinstance(value, list):
            if not key:
                raise ScraperError("Cannot flatten list at root level")
            if value and all(not isinstance(item, (dict, list)) for item in value):
                flat[key] = " | ".join(str(item) for item in value)
            else:
                flat[key] = json.dumps(value, ensure_ascii=False)
        else:
            flat[key] = value

    visit("", record)
    return flat


def save_xlsx(records: Sequence[Dict[str, Any]], output_path: str) -> None:
    if not records:
        raise ScraperError("No records to export")

    flattened = [flatten_record(record) for record in records]
    headers: List[str] = []
    seen: Set[str] = set()
    for flat in flattened:
        for key in flat.keys():
            if key not in seen:
                seen.add(key)
                headers.append(key)

    wb = Workbook()
    ws = wb.active
    ws.title = "Inspections"

    ws.append(headers)
    for flat in flattened:
        ws.append([flat.get(header) for header in headers])

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    wb.save(output_path)


def load_records(json_path: str) -> List[Dict[str, Any]]:
    with open(json_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        raise ScraperError(f"JSON root must be a list: {json_path}")
    return data


def export_records(
    records: Sequence[Dict[str, Any]],
    output_prefix: Path,
    write_json: bool,
) -> Dict[str, str]:
    outputs: Dict[str, str] = {}

    if write_json:
        json_path = output_prefix.with_suffix(".json")
        save_records(records, str(json_path))
        outputs["json"] = str(json_path)

    xlsx_path = output_prefix.with_suffix(".xlsx")
    save_xlsx(records, str(xlsx_path))
    outputs["xlsx"] = str(xlsx_path)

    return outputs


def collect_records(
    session: requests.Session,
    listing_rows: Sequence[Dict[str, Any]],
    output_prefix: Path,
    write_json: bool,
    thresholds: Sequence[int],
    status_interval: int,
    max_retries: int,
    retry_delay: float,
) -> Dict[str, List[str]]:
    if not listing_rows:
        raise ScraperError("No listing rows to process")

    total = len(listing_rows)
    thresholds = sorted(set(thresholds))
    if not thresholds or thresholds[-1] != total:
        thresholds = list(thresholds)
        if total not in thresholds:
            thresholds.append(total)
        thresholds.sort()

    next_threshold_index = 0
    next_threshold = thresholds[next_threshold_index]

    outputs: Dict[str, List[str]] = {}
    records: List[Dict[str, Any]] = []

    print(
        f"Starting scrape for {total} records with checkpoints at {thresholds}",
        flush=True,
    )

    for idx, row in enumerate(listing_rows, start=1):
        ins_number = row.get("insepctionNumber") or row.get("inspectionNumber")
        if not ins_number:
            raise ScraperError("Inspection number missing in listing row")

        last_error: Optional[Exception] = None
        detail: Optional[Dict[str, Any]] = None

        for attempt in range(1, max_retries + 1):
            try:
                detail = fetch_detail(int(ins_number), session=session)
                break
            except ScraperError as exc:
                last_error = exc
                if attempt == max_retries:
                    break
                print(
                    f"Retry {attempt}/{max_retries} for inspection {ins_number} "
                    f"after error: {exc}. Waiting {retry_delay}s...",
                    flush=True,
                )
                time.sleep(retry_delay)

        if detail is None:
            raise ScraperError(
                f"Failed to fetch detail for {ins_number} after {max_retries} attempts"
            ) from last_error

        combined = {
            "inspection_number": int(ins_number),
            "listing": annotate_dates(row),
            "detail": annotate_dates(detail),
        }
        records.append(combined)

        if status_interval and (idx % status_interval == 0 or idx == total):
            print(
                f"[{idx}/{total}] Processed inspection {ins_number}",
                flush=True,
            )

        while next_threshold is not None and idx >= next_threshold:
            prefix = output_prefix.parent / f"{output_prefix.name}_batch_{next_threshold}"
            chunk = records[:next_threshold]
            chunk_outputs = export_records(
                chunk,
                prefix,
                write_json=write_json,
            )
            saved = ", ".join(f"{k}: {v}" for k, v in chunk_outputs.items())
            print(
                f"Checkpoint saved at {next_threshold} records -> {saved}",
                flush=True,
            )
            for kind, path in chunk_outputs.items():
                outputs.setdefault(kind, []).append(path)
            next_threshold_index += 1
            next_threshold = (
                thresholds[next_threshold_index]
                if next_threshold_index < len(thresholds)
                else None
            )

    print("Scrape completed.", flush=True)
    return outputs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch or transform Drug Inspection data.")
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of listing records to fetch (default: 10). Ignored when --from-json is used unless --trim is passed.",
    )
    parser.add_argument(
        "--from-json",
        dest="from_json",
        help="Existing JSON file to convert to XLSX without calling the API.",
    )
    parser.add_argument(
        "--output-prefix",
        dest="output_prefix",
        help="Prefix for output files (without extension). Defaults to data/drug_inspections_sample_<limit> when fetching or the source JSON path when converting.",
    )
    parser.add_argument(
        "--trim",
        action="store_true",
        help="When used with --from-json, limits the output to the first --limit records.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=0,
        help="Write incremental checkpoints every N records.",
    )
    parser.add_argument(
        "--status-interval",
        type=int,
        default=100,
        help="Print progress every N processed records (default: 100).",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Number of retries for detail fetches (default: 3).",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=3.0,
        help="Seconds to wait between retries (default: 3.0).",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Fetch the entire dataset (overrides --limit).",
    )
    parser.add_argument(
        "--xlsx-only",
        action="store_true",
        help="Skip writing JSON outputs; generate XLSX exports only.",
    )
    parser.add_argument(
        "--listing-cache",
        dest="listing_cache",
        help="Path to cache the raw listing JSON. Defaults to data/drug_inspections_listing.json.",
    )
    parser.add_argument(
        "--stream-timeout",
        dest="stream_timeout",
        type=float,
        nargs=2,
        metavar=("CONNECT_SEC", "READ_SEC"),
        default=(10.0, 300.0),
        help="Timeouts (connect, read) for streaming the full listing download.",
    )
    parser.add_argument(
        "--stream-chunk-size",
        dest="stream_chunk_size",
        type=int,
        default=65536,
        help="Chunk size in bytes when streaming the full listing download.",
    )
    return parser.parse_args()


def main() -> Dict[str, List[str]]:
    args = parse_args()

    write_json = not args.xlsx_only

    if args.from_json:
        records = load_records(args.from_json)
        if args.trim and args.limit < len(records):
            records = records[: args.limit]

        if args.output_prefix:
            output_prefix = Path(args.output_prefix)
        else:
            output_prefix = Path(args.from_json).with_suffix("")

        total = len(records)
        if args.batch_size and args.batch_size > 0:
            thresholds = [
                t
                for t in range(args.batch_size, total + args.batch_size, args.batch_size)
                if t <= total
            ]
        else:
            thresholds = []
        if total not in thresholds:
            thresholds.append(total)

        outputs: Dict[str, List[str]] = {}
        for threshold in thresholds:
            subset = records[:threshold]
            prefix = output_prefix.parent / f"{output_prefix.name}_batch_{threshold}"
            chunk_outputs = export_records(subset, prefix, write_json=write_json)
            for kind, path in chunk_outputs.items():
                outputs.setdefault(kind, []).append(path)
        return outputs

    limit: Optional[int]
    if args.all:
        limit = None
    else:
        limit = args.limit if args.limit and args.limit > 0 else None

    batch_size = args.batch_size if args.batch_size and args.batch_size > 0 else 0

    listing_cache = (
        Path(args.listing_cache)
        if args.listing_cache
        else Path("data") / "drug_inspections_listing.json"
    )

    with requests.Session() as session:
        listing_rows = retrieve_listing_rows(
            session=session,
            limit=limit,
            cache_path=listing_cache,
            max_retries=args.max_retries,
            retry_delay=args.retry_delay,
            stream_timeout=tuple(args.stream_timeout),
            stream_chunk_size=args.stream_chunk_size,
        )
        total = len(listing_rows)
        if total == 0:
            raise ScraperError("No listing rows returned from search endpoint")

        thresholds: List[int] = []
        if batch_size:
            thresholds = [
                t
                for t in range(batch_size, total + batch_size, batch_size)
                if t <= total
            ]
        if total not in thresholds:
            thresholds.append(total)

        if args.output_prefix:
            output_prefix = Path(args.output_prefix)
        else:
            base_name = (
                f"drug_inspections_full"
                if batch_size or (limit is None or total > 10)
                else f"drug_inspections_sample_{total}"
            )
            output_prefix = Path("data") / base_name

        outputs = collect_records(
            session=session,
            listing_rows=listing_rows,
            output_prefix=output_prefix,
            write_json=write_json,
            thresholds=thresholds,
            status_interval=args.status_interval,
            max_retries=args.max_retries,
            retry_delay=args.retry_delay,
        )

    return outputs


if __name__ == "__main__":
    try:
        paths = main()
        print("Saved outputs:")
        for kind, locations in paths.items():
            for location in locations:
                print(f"  {kind}: {location}")
    except ScraperError as exc:
        raise SystemExit(f"Scraper failed: {exc}") from exc

