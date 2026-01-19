#!/usr/bin/env python3
"""
Create a Supabase table and bulk insert inspection rows from a CSV export.
Requires the Supabase SQL API (service role key) and PostgREST endpoint.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests

DEFAULT_TABLE_NAME = "drug_inspections"
SQL_RPC_PATH = "rest/v1/rpc/sql"


def load_mapping(mapping_path: Path) -> Dict[str, str]:
    with mapping_path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    # Ensure deterministic ordering
    ordered = {k: data[k] for k in data}
    return ordered


def run_sql(
    url: str,
    service_role_key: str,
    query: str,
    *,
    raise_for_status: bool = True,
) -> requests.Response:
    endpoint = f"{url.rstrip('/')}/{SQL_RPC_PATH}"
    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
    }
    response = requests.post(endpoint, headers=headers, json={"query": query})
    if raise_for_status:
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise RuntimeError(f"SQL API error: {response.text}") from exc
    return response


def create_table(
    url: str,
    service_role_key: str,
    table_name: str,
    columns: Iterable[str],
) -> None:
    cols_sql: List[str] = []
    for column in columns:
        if column == "inspection_number":
            cols_sql.append(f'"{column}" bigint')
        else:
            cols_sql.append(f'"{column}" text')

    drop_sql = f'DROP TABLE IF EXISTS public."{table_name}";'
    create_sql = (
        f'CREATE TABLE public."{table_name}" ('
        f'{", ".join(cols_sql)}, PRIMARY KEY ("inspection_number"));'
    )
    print("Dropping table if exists…", flush=True)
    run_sql(url, service_role_key, drop_sql)
    print("Creating table…", flush=True)
    run_sql(url, service_role_key, create_sql)


def chunked(iterable: Iterable[Dict[str, Optional[str]]], size: int) -> Iterable[List[Dict[str, Optional[str]]]]:
    batch: List[Dict[str, Optional[str]]] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def normalize_row(row: Dict[str, str]) -> Dict[str, Optional[str]]:
    normalized: Dict[str, Optional[str]] = {}
    for key, value in row.items():
        if value == "":
            normalized[key] = None
        elif key == "inspection_number":
            try:
                normalized[key] = int(value)  # type: ignore[assignment]
            except ValueError:
                normalized[key] = None
        else:
            normalized[key] = value
    return normalized


def load_rows(csv_path: Path) -> Iterable[Dict[str, Optional[str]]]:
    with csv_path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            yield normalize_row(row)


def fetch_existing_row_uids(
    url: str,
    service_role_key: str,
    table_name: str,
    source_filter: Optional[str] = None,
) -> set[str]:
    """
    Fetch all existing row_uid values from Supabase table.
    Optionally filter by source (e.g., "HC_INSPECTIONS").
    Returns a set of row_uid values for fast lookup.
    """
    endpoint = f"{url.rstrip('/')}/rest/v1/{table_name}"
    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
    }
    
    # Use select query to get only row_uid column
    params = {
        "select": "row_uid",
    }
    
    # Add source filter if provided
    if source_filter:
        params["source"] = f"eq.{source_filter}"
    
    existing_row_uids: set[str] = set()
    offset = 0
    limit = 1000  # Fetch in batches
    
    while True:
        params_with_offset = {**params, "offset": offset, "limit": limit}
        
        try:
            response = requests.get(endpoint, headers=headers, params=params_with_offset, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            if not data or not isinstance(data, list):
                break
                
            for row in data:
                if isinstance(row, dict) and "row_uid" in row:
                    row_uid = row["row_uid"]
                    if row_uid is not None:
                        existing_row_uids.add(str(row_uid))
            
            # If we got fewer results than the limit, we've reached the end
            if len(data) < limit:
                break
                
            offset += limit
            
        except requests.HTTPError as exc:
            # If table doesn't exist yet, return empty set
            if exc.response is not None and exc.response.status_code == 404:
                print("Table does not exist yet, will create it.", flush=True)
                return set()
            error_text = exc.response.text if exc.response is not None else str(exc)
            raise RuntimeError(
                f"Failed fetching existing row_uids: {error_text}"
            ) from exc
    
    print(f"Found {len(existing_row_uids)} existing row_uids in Supabase.", flush=True)
    return existing_row_uids


def fetch_existing_inspection_numbers(
    url: str,
    service_role_key: str,
    table_name: str,
) -> set[int]:
    """
    Fetch all existing inspection numbers from Supabase table.
    Returns a set of inspection numbers for fast lookup.
    DEPRECATED: Use fetch_existing_row_uids instead for nexara_all_source table.
    """
    endpoint = f"{url.rstrip('/')}/rest/v1/{table_name}"
    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
    }
    
    # Use select query to get only inspection_number column
    params = {
        "select": "inspection_number",
    }
    
    existing_numbers: set[int] = set()
    offset = 0
    limit = 1000  # Fetch in batches
    
    while True:
        params_with_offset = {**params, "offset": offset, "limit": limit}
        
        try:
            response = requests.get(endpoint, headers=headers, params=params_with_offset, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            if not data or not isinstance(data, list):
                break
                
            for row in data:
                if isinstance(row, dict) and "inspection_number" in row:
                    ins_num = row["inspection_number"]
                    if ins_num is not None:
                        existing_numbers.add(int(ins_num))
            
            # If we got fewer results than the limit, we've reached the end
            if len(data) < limit:
                break
                
            offset += limit
            
        except requests.HTTPError as exc:
            # If table doesn't exist yet, return empty set
            if exc.response is not None and exc.response.status_code == 404:
                print("Table does not exist yet, will create it.", flush=True)
                return set()
            error_text = exc.response.text if exc.response is not None else str(exc)
            raise RuntimeError(
                f"Failed fetching existing inspection numbers: {error_text}"
            ) from exc
    
    print(f"Found {len(existing_numbers)} existing inspection numbers in Supabase.", flush=True)
    return existing_numbers


def insert_batches(
    url: str,
    service_role_key: str,
    table_name: str,
    rows: Iterable[Dict[str, Optional[str]]],
    *,
    batch_size: int = 500,
) -> None:
    endpoint = f"{url.rstrip('/')}/rest/v1/{table_name}"
    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    total = 0
    start_time = time.time()
    for batch in chunked(rows, batch_size):
        response = requests.post(endpoint, headers=headers, json=batch, timeout=60)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise RuntimeError(
                f"Failed inserting batch at offset {total}: {response.text}"
            ) from exc
        total += len(batch)
        elapsed = time.time() - start_time
        print(f"Inserted {total} rows ({len(batch)} in last batch, {elapsed:.1f}s elapsed)", flush=True)
    print("All rows inserted.", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload inspection CSV to Supabase table.")
    parser.add_argument("--csv", required=True, help="Path to CSV exported by xlsx_to_csv.py")
    parser.add_argument(
        "--mapping",
        required=True,
        help="Path to header mapping JSON file produced during CSV export.",
    )
    parser.add_argument(
        "--table-name",
        default=DEFAULT_TABLE_NAME,
        help=f"Destination table name (default: {DEFAULT_TABLE_NAME}).",
    )
    parser.add_argument(
        "--supabase-url",
        default=os.environ.get("SUPABASE_URL"),
        help="Supabase project URL (default: SUPABASE_URL env var).",
    )
    parser.add_argument(
        "--service-role-key",
        default=os.environ.get("SUPABASE_SERVICE_ROLE_KEY"),
        help="Supabase service role key (default: SUPABASE_SERVICE_ROLE_KEY env var).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of rows per insert batch (default: 500).",
    )
    parser.add_argument(
        "--skip-create-table",
        action="store_true",
        help="Skip dropping/creating the table (assumes it already exists).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.supabase_url or not args.service_role_key:
        sys.exit("Supabase URL and service role key must be provided via arguments or environment")

    csv_path = Path(args.csv)
    mapping_path = Path(args.mapping)
    if not csv_path.exists():
        sys.exit(f"CSV file not found: {csv_path}")
    if not mapping_path.exists():
        sys.exit(f"Mapping file not found: {mapping_path}")

    mapping = load_mapping(mapping_path)
    sanitized_columns = list(mapping.values())

    if args.skip_create_table:
        print("Skipping table creation step.", flush=True)
    else:
        create_table(args.supabase_url, args.service_role_key, args.table_name, sanitized_columns)
    rows = load_rows(csv_path)
    insert_batches(
        args.supabase_url,
        args.service_role_key,
        args.table_name,
        rows,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()

