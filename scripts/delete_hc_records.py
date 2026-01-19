#!/usr/bin/env python3
"""
Delete HC (Health Canada inspection) records from nexara_all_source table
that were created today. Use with caution - this will permanently delete records.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from typing import Optional

import requests


def delete_hc_records_today(
    supabase_url: str,
    service_role_key: str,
    table_name: str,
    dry_run: bool = False,
    date: Optional[str] = None,
) -> None:
    """
    Delete records where source='HC' and created_at is today (or specified date).
    """
    # Use today's date or the specified date
    if date:
        target_date = datetime.strptime(date, "%Y-%m-%d").date()
    else:
        target_date = datetime.now(timezone.utc).date()
    
    # Format for SQL query (YYYY-MM-DD)
    date_str = target_date.strftime("%Y-%m-%d")
    
    print(f"Target date: {date_str}", flush=True)
    
    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{table_name}"
    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    
    # First, count how many records will be deleted
    # Use date range: from start of day to end of day
    count_params = {
        "source": "eq.HC",
        "created_at": f"gte.{date_str}T00:00:00Z",
        "select": "row_uid,created_at",
    }
    
    print("Counting HC records created today to delete...", flush=True)
    response = requests.get(endpoint, headers=headers, params=count_params, timeout=60)
    response.raise_for_status()
    records = response.json()
    
    # Filter to only records created on the target date
    if isinstance(records, list):
        filtered_records = [
            r for r in records
            if r.get("created_at") and r["created_at"].startswith(date_str)
        ]
        count = len(filtered_records)
    else:
        count = 0
    
    if count == 0:
        print(f"No HC records found created on {date_str} to delete.", flush=True)
        return
    
    print(f"Found {count} HC records created on {date_str} to delete.", flush=True)
    
    if dry_run:
        print("DRY RUN: Would delete these records, but --dry-run is set.", flush=True)
        if isinstance(records, list) and len(filtered_records) > 0:
            print("Sample records that would be deleted:", flush=True)
            for i, record in enumerate(filtered_records[:5], 1):
                print(f"  {i}. row_uid: {record.get('row_uid')}, created_at: {record.get('created_at')}", flush=True)
            if len(filtered_records) > 5:
                print(f"  ... and {len(filtered_records) - 5} more", flush=True)
        return
    
    # Confirm deletion
    print(f"\n⚠️  WARNING: This will delete {count} records where source='HC' and created_at='{date_str}'", flush=True)
    print("Type 'DELETE' to confirm: ", end="", flush=True)
    confirmation = input().strip()
    
    if confirmation != "DELETE":
        print("Deletion cancelled.", flush=True)
        return
    
    # Delete by source='HC' and created_at date using SQL RPC endpoint
    # Delete records created on the target date (from 00:00:00 to 23:59:59)
    delete_sql = (
        f'DELETE FROM public."{table_name}" '
        f'WHERE "source" = \'HC\' '
        f'AND DATE("created_at") = \'{date_str}\';'
    )
    
    sql_endpoint = f"{supabase_url.rstrip('/')}/rest/v1/rpc/sql"
    sql_headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
    }
    
    print(f"Executing delete query for records created on {date_str}...", flush=True)
    delete_response = requests.post(
        sql_endpoint,
        headers=sql_headers,
        json={"query": delete_sql},
        timeout=60
    )
    delete_response.raise_for_status()
    
    # Verify deletion
    verify_response = requests.get(endpoint, headers=headers, params=count_params, timeout=60)
    verify_response.raise_for_status()
    remaining = verify_response.json()
    
    # Filter remaining records to only those from target date
    if isinstance(remaining, list):
        remaining_today = [
            r for r in remaining
            if r.get("created_at") and r["created_at"].startswith(date_str)
        ]
        remaining_count = len(remaining_today)
    else:
        remaining_count = 0
    
    deleted_count = count - remaining_count
    print(f"✅ Successfully deleted {deleted_count} HC records created on {date_str}.", flush=True)
    if remaining_count > 0:
        print(f"Warning: {remaining_count} HC records from {date_str} still remain (deletion may have failed).", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Delete HC records created today from nexara_all_source table."
    )
    parser.add_argument(
        "--table-name",
        default="nexara_all_source",
        help="Supabase table name (default: nexara_all_source).",
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
        "--date",
        default=None,
        help="Date to delete records from (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    
    if not args.supabase_url or not args.service_role_key:
        sys.exit(
            "ERROR: Supabase URL and service role key must be provided via arguments or environment variables."
        )
    
    try:
        delete_hc_records_today(
            args.supabase_url,
            args.service_role_key,
            args.table_name,
            dry_run=args.dry_run,
            date=args.date,
        )
    except Exception as exc:
        sys.exit(f"Failed to delete records: {exc}")


if __name__ == "__main__":
    main()
