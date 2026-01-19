#!/usr/bin/env python3
"""
Main orchestration script that:
1. Scrapes drug inspection records
2. Compares with existing Supabase records
3. Only syncs new records to Supabase
4. Designed to run monthly via GitHub Actions or cron
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

# Import functions from other scripts
import sys
from pathlib import Path

# Add scripts directory to path
scripts_dir = Path(__file__).parent
sys.path.insert(0, str(scripts_dir))

# Import from scrape_drug_inspections
from scrape_drug_inspections import ScraperError

# Import from supabase_sync
from supabase_sync import (
    fetch_existing_row_uids,
    insert_batches,
    create_table,
    normalize_row,
)


def load_records_from_json(json_path: Path) -> List[Dict]:
    """Load records from JSON file."""
    with json_path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        raise ValueError(f"JSON root must be a list: {json_path}")
    return data


def map_inspection_to_nexara_format(record: Dict) -> Dict[str, Optional[str]]:
    """
    Map inspection record to nexara_all_source table format.
    """
    inspection_number = record.get("inspection_number")
    listing = record.get("listing", {})
    detail = record.get("detail", {})
    
    # Use detail data if available, otherwise fall back to listing
    data = detail if detail else listing
    
    # Create row_uid in format: HC:{inspection_number}
    row_uid = f"HC:{inspection_number}"
    
    # Extract DIN if available (might be in various fields)
    din_match_key = None
    if isinstance(data, dict):
        # Try to find DIN in various possible fields
        for key in ["din", "DIN", "licenseNumber", "registrationNumber"]:
            if key in data and data[key]:
                din_match_key = str(data[key]).strip()
                break
    
    # Map inspection data to nexara_all_source columns
    # Only HC columns will be populated, KF and MAGI columns will be null
    mapped = {
        "match_bucket": "HC only",
        "source": "HC",
        "row_uid": row_uid,
        "din_match_key": din_match_key or "",
        "Status": data.get("ratingDesc") or data.get("rating") or "",
        "DIN URL": "",
        "DIN": din_match_key or "",
        "Company": data.get("establishmentName") or "",
        "Product": "",
        "Class": data.get("establishmentType") or "",
        "PM See footnote1": "",
        "Schedule": "",
        "# See footnote2": "",
        "A.I. name See footnote3": "",
        "Strength": "",
        "Current status date": data.get("inspectionEndDate_iso") or data.get("inspectionEndDate") or "",
        "Original market date": data.get("inspectionStartDate_iso") or data.get("inspectionStartDate") or "",
        "Address": data.get("street") or "",
        "City": data.get("city") or "",
        "state": data.get("province") or "",
        "Country": data.get("country") or "",
        "Zipcode": data.get("postalCode") or "",
        "Number of active ingredient(s)": "",
        "Biosimilar Biologic Drug": "",
        "American Hospital Formulary Service (AHFS)": "",
        "Anatomical Therapeutic Chemical (ATC)": "",
        "Active ingredient group (AIG) number": "",
        "Labelling": "",
        "Product Monograph/Veterinary Date": "",
        "List of active ingredient": "",
        "Dosage form": "",
        "Route(s) of administration": "",
        "Qty Ordered": "",
        "Item": "",
        "UPC#": "",
        "DIN/NPN": din_match_key or "",
        "Pack Size": "",
        "Product Description": f"Inspection: {data.get('inspectionType') or ''} - {data.get('activity') or ''}",
        "Volume Purchases": "",
        "Min/Mult": "",
        "Extended Dating": "",
        "GST": "",
        "Price": "",
        "Supplier": "",
        "Narcotics": "",
        "picture": "",
        "Inventory status": "",
    }
    
    # Add all magi__ fields as empty strings
    magi_fields = [
        "magi__product_id", "magi__Pro. Name", "magi__STR", "magi__P.S.", "magi__D.F.",
        "magi__MFG", "magi__ACQ.P.", "magi__ORG", "magi__Status", "magi__DIN / NPN number / Local Cod",
        "magi__Lead Time", "magi__Brand / Trade Name", "magi__Manufacturer / Brand Manufacturer *",
        "magi__Unit Of Measurement*", "magi__Active Ingredient(s)", "magi__User indications",
        "magi__Storage Conditions", "magi__Generic name (Short)", "magi__Route of Administration",
        "magi__Warning(s)", "magi__Shelf Life", "magi__product pictures", "magi__Canadian_dollar",
        "magi__Offering_Price", "magi__Customer", "magi__Countries", "magi__Aq Price",
        "magi__C FE price", "magi__Q price", "magi__Inv/SO price", "magi__Correct new c s p",
        "magi__End Sellig Price", "magi__Status/Visibility", "magi__Supplier Name",
        "magi__Buying Price (Bill)", "magi__Supplier Product Price", "magi__Registration Class 1",
        "magi__Registration Class 2", "magi__Therapeutic Class 3 (MOA,Chem,)", "magi__Hospital Formulary Class 4:",
        "magi__Market Class 5(Generic, Brand)", "magi__Active ing. Grp/Generic Drug Code",
        "magi__Alterative(s)", "magi__Also Known As", "magi__Therapeutic Class",
        "magi__Quantity On Hand(Current Stock)", "magi__Re-Order Point", "magi__Lot / Batch Number",
        "magi__Quantity", "magi__Expiration Date", "magi__UPC / GTIN Code", "magi__UPC 10",
        "magi__Harmonized System:", "magi__Dimenstions In Mm:", "magi__Weight (in gram):",
        "magi__Case Size:", "magi__Manufacturer Address:", "magi__Manufacture City:",
        "magi__Manufacture State:", "magi__Manufacture Country:", "magi__Original Market Date:",
        "magi__Current Status Date:", "magi__M.A. Holder:", "magi__M.A. Holder Address:",
        "magi__Internal System Code # :", "magi__Special Handling", "magi__Stamp with time",
        "magi___pictures_json", "magi___customers_json", "magi___suppliers_json", "magi___timeline_json",
        "magi__scraped_at"
    ]
    
    for field in magi_fields:
        mapped[field] = ""
    
    # Store full inspection data as JSON in Product Description or a custom approach
    # Since we don't have a dedicated inspection_data_json field, we'll enhance Product Description
    inspection_summary = {
        "inspection_number": inspection_number,
        "inspection_type": data.get("inspectionType"),
        "activity": data.get("activity"),
        "rating": data.get("rating"),
        "rating_desc": data.get("ratingDesc"),
        "inspection_start": data.get("inspectionStartDate_iso") or data.get("inspectionStartDate"),
        "inspection_end": data.get("inspectionEndDate_iso") or data.get("inspectionEndDate"),
        "certificate_type": data.get("certificateType"),
        "reference_number": data.get("referenceNumber"),
    }
    # Store as JSON in Product Description field (append to existing)
    if mapped["Product Description"]:
        mapped["Product Description"] += f" | Data: {json.dumps(inspection_summary, ensure_ascii=False)}"
    else:
        mapped["Product Description"] = json.dumps(inspection_summary, ensure_ascii=False)
    
    return mapped


def records_to_nexara_rows(
    records: List[Dict],
    existing_row_uids: Set[str],
) -> tuple[List[Dict[str, Optional[str]]], List[str]]:
    """
    Convert inspection records to nexara_all_source format.
    Only includes records that are not already in Supabase (by row_uid).
    """
    new_records = []
    for record in records:
        inspection_number = record.get("inspection_number")
        row_uid = f"HC:{inspection_number}"
        if row_uid not in existing_row_uids:
            new_records.append(record)
    
    print(
        f"Found {len(records)} total records, "
        f"{len(new_records)} new records to sync, "
        f"{len(existing_row_uids)} already in Supabase.",
        flush=True
    )
    
    if not new_records:
        print("No new records to sync.", flush=True)
        return [], []
    
    # Map to nexara format
    mapped_rows = [map_inspection_to_nexara_format(record) for record in new_records]
    
    # Get all column names from the first record (all should have same structure)
    if mapped_rows:
        columns = list(mapped_rows[0].keys())
    else:
        columns = []
    
    return mapped_rows, columns


def sync_new_records(
    records: List[Dict],
    supabase_url: str,
    service_role_key: str,
    table_name: str,
    batch_size: int = 500,
) -> None:
    """
    Sync only new records to Supabase nexara_all_source table.
    """
    print("Fetching existing row_uids from Supabase...", flush=True)
    existing_row_uids = fetch_existing_row_uids(
        supabase_url,
        service_role_key,
        table_name,
        source_filter="HC",
    )
    
    print("Filtering and mapping new records...", flush=True)
    mapped_rows, columns = records_to_nexara_rows(records, existing_row_uids)
    
    if not mapped_rows:
        print("No new records to sync. Exiting.", flush=True)
        return
    
    # Note: We don't create the table automatically for nexara_all_source
    # as it likely has a complex schema with many columns and constraints.
    # The table should already exist. If it doesn't, the insert will fail
    # and the user will need to create it manually.
    
    # Convert to format suitable for insertion (ensure all columns are present)
    # The mapped_rows already have the correct format (Dict[str, Optional[str]])
    # We just need to ensure all rows have the same columns in the same order
    normalized_rows = []
    for row in mapped_rows:
        # Ensure all columns are present, fill missing ones with empty string
        normalized_row: Dict[str, Optional[str]] = {}
        for col in columns:
            value = row.get(col)
            # Convert None to empty string, convert other values to string
            if value is None:
                normalized_row[col] = ""
            else:
                normalized_row[col] = str(value)
        normalized_rows.append(normalized_row)
    
    print(f"Inserting {len(normalized_rows)} new records to Supabase...", flush=True)
    insert_batches(
        supabase_url,
        service_role_key,
        table_name,
        normalized_rows,
        batch_size=batch_size,
    )
    print(f"Successfully synced {len(normalized_rows)} new records to Supabase.", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape drug inspections and sync new records to Supabase."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Number of listing records to fetch (default: all).",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Fetch the entire dataset (overrides --limit).",
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
        "--batch-size",
        type=int,
        default=500,
        help="Number of rows per insert batch (default: 500).",
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
        "--status-interval",
        type=int,
        default=100,
        help="Print progress every N processed records (default: 100).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data"),
        help="Directory to store temporary scraped data (default: data).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    
    if not args.supabase_url or not args.service_role_key:
        sys.exit(
            "Supabase URL and service role key must be provided via arguments or environment"
        )
    
    # Prepare scraper arguments
    limit: Optional[int] = None if args.all else args.limit
    
    # Create temporary directory for scraped data
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        print("Starting scrape...", flush=True)
        
        # Import scraper functions
        from scrape_drug_inspections import (
            retrieve_listing_rows,
            collect_records,
        )
        import requests
        
        listing_cache = output_dir / "drug_inspections_listing.json"
        
        with requests.Session() as session:
            listing_rows = retrieve_listing_rows(
                session=session,
                limit=limit,
                cache_path=listing_cache,
                max_retries=args.max_retries,
                retry_delay=args.retry_delay,
                stream_timeout=(10.0, 300.0),
                stream_chunk_size=65536,
            )
            
            total = len(listing_rows)
            if total == 0:
                raise ScraperError("No listing rows returned from search endpoint")
            
            print(f"Found {total} listing rows. Fetching details...", flush=True)
            
            # Use a temporary output prefix
            output_prefix = output_dir / "temp_scrape"
            
            thresholds = [total]  # Single batch
            outputs = collect_records(
                session=session,
                listing_rows=listing_rows,
                output_prefix=output_prefix,
                write_json=True,
                thresholds=thresholds,
                status_interval=args.status_interval,
                max_retries=args.max_retries,
                retry_delay=args.retry_delay,
            )
            
            # Get the JSON file path
            json_files = outputs.get("json", [])
            if not json_files:
                raise ScraperError("No JSON output file generated")
            
            json_path = Path(json_files[0])
        
        print(f"Scrape completed. Loading records from {json_path}...", flush=True)
        records = load_records_from_json(json_path)
        
        print(f"Loaded {len(records)} records. Syncing to Supabase...", flush=True)
        sync_new_records(
            records,
            args.supabase_url,
            args.service_role_key,
            args.table_name,
            batch_size=args.batch_size,
        )
        
        print("Monthly sync completed successfully!", flush=True)
        
    except ScraperError as exc:
        sys.exit(f"Scraper failed: {exc}")
    except Exception as exc:
        sys.exit(f"Sync failed: {exc}")


if __name__ == "__main__":
    main()
