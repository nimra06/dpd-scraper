from dpd_scraper.supabase_io import apply_approved_changes_to_baseline_and_HC_products
if __name__ == "__main__":
    apply_approved_changes_to_baseline_and_HC_products()
    print("Applied approved changes → baseline + HC products; logged to change_logs.")
