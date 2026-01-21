#!/usr/bin/env python3
"""
Test script to scrape 5 DPD products and save to JSON for verification.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Add parent directory to path to import dpd_scraper
sys.path.insert(0, str(Path(__file__).parent.parent))

from dpd_scraper.dpd_scraper import run_full_scrape


def main() -> None:
    print("=" * 80, flush=True)
    print("Testing DPD scraper - fetching 5 products...", flush=True)
    print("=" * 80, flush=True)
    
    # Run scraper with limit of 5 products
    products, meta = run_full_scrape(
        max_depth=1,
        target_min_rows=5,  # Minimum 5 rows
        enrich_flush_every=5,
        request_sleep=0.05,
        max_rows=5,  # Limit to 5 products
    )
    
    print(f"\nScraped {len(products)} products", flush=True)
    
    if not products:
        print("ERROR: No products scraped!", flush=True)
        sys.exit(1)
    
    # Save to JSON
    output_dir = Path(__file__).parent.parent / "data"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "test_dpd_products.json"
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(products, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Saved {len(products)} products to: {output_file}", flush=True)
    print("\nSample product:", flush=True)
    print(json.dumps(products[0], indent=2, ensure_ascii=False), flush=True)
    
    print("\n" + "=" * 80, flush=True)
    print("Test completed successfully!", flush=True)
    print("=" * 80, flush=True)


if __name__ == "__main__":
    main()
