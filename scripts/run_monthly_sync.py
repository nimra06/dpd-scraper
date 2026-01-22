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
import signal
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

# Import functions from other scripts
import sys
from pathlib import Path

# Add scripts directory to path
scripts_dir = Path(__file__).parent
sys.path.insert(0, str(scripts_dir))

# Add parent directory to path so we can import dpd_scraper
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

# Define ScraperError locally
class ScraperError(RuntimeError):
    """Domain specific error raised for scraping issues."""

# Import from supabase_sync
from supabase_sync import (
    fetch_existing_row_uids,
    insert_batches,
    create_table,
    normalize_row,
)




def map_dpd_product_to_nexara_format(product: Dict[str, str]) -> Dict[str, Optional[str]]:
    """
    Map DPD product record to nexara_all_source table format.
    """
    din = product.get("DIN", "").strip()
    if not din:
        # Skip records without DIN
        return {}
    
    # Create row_uid in format: HC:{DIN}
    row_uid = f"HC:{din}"
    
    # Map DPD product data to nexara_all_source columns
    # Only HC columns will be populated, KF and MAGI columns will be empty
    mapped = {
        "match_bucket": "HC",
        "source": "HC",
        "row_uid": row_uid,
        "din_match_key": din,
        "Status": product.get("Status", ""),
        "DIN URL": product.get("DIN URL", ""),
        "DIN": din,
        "Company": product.get("Company", ""),
        "Product": product.get("Product", ""),
        "Class": product.get("Class", ""),
        "PM See footnote1": product.get("PM See footnote1", ""),
        "Schedule": product.get("Schedule", ""),
        "# See footnote2": product.get("# See footnote2", ""),
        "A.I. name See footnote3": product.get("A.I. name See footnote3", ""),
        "Strength": product.get("Strength", ""),
        "Current status date": product.get("Current status date", ""),
        "Original market date": product.get("Original market date", ""),
        "Address": product.get("Address", ""),
        "City": product.get("City", ""),
        "state": product.get("state", ""),
        "Country": product.get("Country", ""),
        "Zipcode": product.get("Zipcode", ""),
        "Number of active ingredient(s)": product.get("Number of active ingredient(s)", ""),
        "Biosimilar Biologic Drug": product.get("Biosimilar Biologic Drug", ""),
        "American Hospital Formulary Service (AHFS)": product.get("American Hospital Formulary Service (AHFS)", ""),
        "Anatomical Therapeutic Chemical (ATC)": product.get("Anatomical Therapeutic Chemical (ATC)", ""),
        "Active ingredient group (AIG) number": product.get("Active ingredient group (AIG) number", ""),
        "Labelling": product.get("Labelling", ""),
        "Product Monograph/Veterinary Date": product.get("Product Monograph/Veterinary Date", ""),
        "List of active ingredient": product.get("List of active ingredient", ""),
        "Dosage form": product.get("Dosage form", ""),
        "Route(s) of administration": product.get("Route(s) of administration", ""),
        "Qty Ordered": "",
        "Item": "",
        "UPC#": "",
        "DIN/NPN": din,
        "Pack Size": "",
        "Product Description": "",
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
    
    return mapped


def products_to_nexara_rows(
    products: List[Dict[str, str]],
    existing_row_uids: Set[str],
) -> tuple[List[Dict[str, Optional[str]]], List[str]]:
    """
    Convert DPD product records to nexara_all_source format.
    Only includes records that are not already in Supabase (by row_uid).
    """
    new_products = []
    for product in products:
        din = product.get("DIN", "").strip()
        if not din:
            continue  # Skip products without DIN
        row_uid = f"HC:{din}"
        if row_uid not in existing_row_uids:
            new_products.append(product)
    
    import time as time_module
    print(
        f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] 📊 Summary:",
        flush=True
    )
    print(
        f"  • Total products scraped: {len(products)}",
        flush=True
    )
    print(
        f"  • Already in Supabase: {len(existing_row_uids)}",
        flush=True
    )
    print(
        f"  • New products to insert: {len(new_products)}",
        flush=True
    )
    
    if not new_products:
        print("No new products to sync.", flush=True)
        return [], []
    
    # Map to nexara format
    mapped_rows = []
    for product in new_products:
        mapped = map_dpd_product_to_nexara_format(product)
        if mapped:  # Only add if mapping succeeded (has DIN)
            mapped_rows.append(mapped)
    
    # Get all column names from the first record (all should have same structure)
    if mapped_rows:
        columns = list(mapped_rows[0].keys())
    else:
        columns = []
    
    return mapped_rows, columns


def sync_new_records(
    products: List[Dict[str, str]],
    supabase_url: str,
    service_role_key: str,
    table_name: str,
    batch_size: int = 500,
) -> None:
    """
    Sync only new product records to Supabase nexara_all_source table.
    """
    import time as time_module
    
    print("=" * 80, flush=True)
    print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Starting Supabase sync process...", flush=True)
    print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Total products scraped: {len(products)}", flush=True)
    print("=" * 80, flush=True)
    
    print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Fetching existing row_uids from Supabase...", flush=True)
    existing_row_uids = fetch_existing_row_uids(
        supabase_url,
        service_role_key,
        table_name,
        source_filter="HC",
    )
    
    print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Found {len(existing_row_uids)} existing records in Supabase", flush=True)
    print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Filtering and mapping new products...", flush=True)
    mapped_rows, columns = products_to_nexara_rows(products, existing_row_uids)
    
    if not mapped_rows:
        print("=" * 80, flush=True)
        print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] No new records to sync.", flush=True)
        print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] All {len(products)} scraped records already exist in Supabase.", flush=True)
        print("=" * 80, flush=True)
        return
    
    # Note: We don't create the table automatically for nexara_all_source
    # as it likely has a complex schema with many columns and constraints.
    # The table should already exist. If it doesn't, the insert will fail
    # and the user will need to create it manually.
    
    # Convert to format suitable for insertion (ensure all columns are present)
    # The mapped_rows already have the correct format (Dict[str, Optional[str]])
    # We just need to ensure all rows have the same columns in the same order
    print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Preparing {len(mapped_rows)} new records for insertion...", flush=True)
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
    
    print("=" * 80, flush=True)
    print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Inserting {len(normalized_rows)} new records to Supabase...", flush=True)
    print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Using batch size: {batch_size}", flush=True)
    print("=" * 80, flush=True)
    
    sync_start_time = time_module.time()
    insert_batches(
        supabase_url,
        service_role_key,
        table_name,
        normalized_rows,
        batch_size=batch_size,
    )
    
    sync_elapsed = time_module.time() - sync_start_time
    print("=" * 80, flush=True)
    print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] ✅ Successfully synced {len(normalized_rows)} new records to Supabase!", flush=True)
    print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Insertion took {sync_elapsed:.1f}s ({sync_elapsed/60:.1f} minutes)", flush=True)
    print("=" * 80, flush=True)


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
            "ERROR: Supabase URL and service role key must be provided via arguments or environment variables.\n"
            "Please set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in GitHub Actions secrets."
        )
    
    # Validate that the keys are not empty strings
    if args.supabase_url.strip() == "" or args.service_role_key.strip() == "":
        sys.exit(
            "ERROR: Supabase URL or service role key is empty.\n"
            "Please check your GitHub Actions secrets: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY"
        )
    
    # Validate URL format
    if not args.supabase_url.startswith("https://") or ".supabase.co" not in args.supabase_url:
        print(f"WARNING: Supabase URL format looks incorrect: {args.supabase_url[:50]}...", flush=True)
    
    # Validate service role key format (should start with eyJ...)
    if not args.service_role_key.startswith("eyJ"):
        print(
            "WARNING: Service role key format looks incorrect. "
            "It should start with 'eyJ' (JWT token). "
            "Make sure you're using the 'service_role' key, not the 'anon' key.",
            flush=True
        )
    
    # Show first few characters for debugging (safe to show)
    print(f"Supabase URL: {args.supabase_url}", flush=True)
    print(f"Service Role Key (first 20 chars): {args.service_role_key[:20]}...", flush=True)
    print(f"Service Role Key length: {len(args.service_role_key)} characters", flush=True)
    
    # Prepare scraper arguments
    limit: Optional[int] = None if args.all else args.limit
    
    # Create temporary directory for scraped data
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Global variables to store scraped products (for timeout handling)
    # Using a dict to avoid UnboundLocalError with nested functions
    state = {
        'scraped_products': [],
        'sync_args': None
    }
    
    def timeout_handler(signum, frame):
        """Handle timeout signal - sync whatever we've scraped so far"""
        import time as time_module
        import glob
        import csv
        
        print("=" * 80, flush=True)
        print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] ⚠️  TIMEOUT DETECTED - Syncing partial results...", flush=True)
        
        products_to_sync = state['scraped_products'].copy() if state['scraped_products'] else []
        
        # If scraper hasn't finished, try to load from checkpoints
        if not products_to_sync:
            checkpoint_dir = os.getenv("SCRAPER_CHECKPOINT_DIR", "artifacts/checkpoints")
            checkpoint_pattern = os.path.join(checkpoint_dir, "dpd_*.csv")
            checkpoint_files = glob.glob(checkpoint_pattern)
            
            if checkpoint_files:
                # Get the latest checkpoint file
                latest_checkpoint = max(checkpoint_files, key=os.path.getmtime)
                print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Loading products from latest checkpoint: {latest_checkpoint}", flush=True)
                
                try:
                    with open(latest_checkpoint, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        products_to_sync = list(reader)
                    print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Loaded {len(products_to_sync)} products from checkpoint", flush=True)
                except Exception as e:
                    print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Error reading checkpoint: {e}", flush=True)
        
        print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Products to sync: {len(products_to_sync)}", flush=True)
        
        if products_to_sync and state['sync_args']:
            try:
                print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Syncing {len(products_to_sync)} products to Supabase (duplicates will be checked)...", flush=True)
                sync_new_records(
                    products_to_sync,
                    state['sync_args']['supabase_url'],
                    state['sync_args']['service_role_key'],
                    state['sync_args']['table_name'],
                    batch_size=state['sync_args']['batch_size'],
                )
                print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] ✅ Partial sync completed successfully!", flush=True)
            except Exception as e:
                print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] ❌ Error during partial sync: {e}", flush=True)
                import traceback
                traceback.print_exc()
        else:
            print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] No products to sync or sync args not set", flush=True)
        
        print("=" * 80, flush=True)
        sys.exit(0)
    
    # Register signal handler for SIGTERM (GitHub Actions sends this on timeout)
    signal.signal(signal.SIGTERM, timeout_handler)
    
    try:
        print("=" * 80, flush=True)
        print("Starting DPD product scrape...", flush=True)
        print("=" * 80, flush=True)
        
        # Import DPD scraper
        import time as time_module
        from dpd_scraper.dpd_scraper import run_full_scrape
        
        start_time = time_module.time()
        # Set maximum scrape time to 5.5 hours (leaving 30 minutes buffer for sync)
        MAX_SCRAPE_TIME = 5.5 * 60 * 60  # 5.5 hours in seconds
        
        print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Starting DPD scrape (this may take a while)...", flush=True)
        print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Maximum scrape time: {MAX_SCRAPE_TIME/3600:.1f} hours (will auto-sync on timeout)", flush=True)
        print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Fetching all products from Health Canada Drug Product Database...", flush=True)
        
        # Run the DPD scraper
        # Set max_rows to limit if provided, otherwise 0 = no limit (all products)
        max_rows = limit if limit else 0
        
        # Optimize for faster scraping in CI environment
        # Reduce sleep time and increase batch sizes for faster execution
        request_sleep = 0.02  # Reduced from 0.05 for faster scraping
        enrich_flush_every = 100  # Increased batch size for enrichment
        
        # Enable checkpoints to save progress (every 2000 rows by default)
        checkpoint_every = int(os.getenv("SCRAPER_CHECKPOINT_EVERY_ROWS", "2000"))
        
        print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Scraper settings: request_sleep={request_sleep}s, enrich_batch={enrich_flush_every}", flush=True)
        if checkpoint_every > 0:
            print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Checkpoints enabled: saving progress every {checkpoint_every} rows", flush=True)
        
        # Store sync arguments for timeout handler
        state['sync_args'] = {
            'supabase_url': args.supabase_url,
            'service_role_key': args.service_role_key,
            'table_name': args.table_name,
            'batch_size': args.batch_size,
        }
        
        products, meta = run_full_scrape(
            max_depth=1,
            target_min_rows=1000,  # Minimum rows to collect
            enrich_flush_every=enrich_flush_every,
            request_sleep=request_sleep,  # Optimized for speed
            max_rows=max_rows,
        )
        
        # Store products for timeout handler (in case timeout happens during sync)
        state['scraped_products'] = products
        
        elapsed = time_module.time() - start_time
        print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Scrape completed in {elapsed:.1f}s ({elapsed/60:.1f} minutes)", flush=True)
        print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Scraped {len(products)} products", flush=True)
        
        if not products:
            raise ScraperError("No products scraped from DPD")
        
        print(f"[{time_module.strftime('%Y-%m-%d %H:%M:%S')}] Syncing products to Supabase...", flush=True)
        sync_new_records(
            products,
            args.supabase_url,
            args.service_role_key,
            args.table_name,
            batch_size=args.batch_size,
        )
        
        total_elapsed = time_module.time() - start_time
        print("=" * 80, flush=True)
        print(f"Monthly sync completed successfully!", flush=True)
        print(f"Total time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} minutes)", flush=True)
        print("=" * 80, flush=True)
        
    except ScraperError as exc:
        sys.exit(f"Scraper failed: {exc}")
    except Exception as exc:
        sys.exit(f"Sync failed: {exc}")


if __name__ == "__main__":
    main()
