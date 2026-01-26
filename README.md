# Drug Inspection Scraper

A Python-based scraper for Health Canada's Drug & Health Product inspection records with automated monthly syncing to Supabase.

## Features

- **Scraping**: Fetches product records from Health Canada's Drug Product Database (DPD)
- **Monthly goal**: Each run scrapes the latest products from the DPD site; only **new** products (by DIN) are inserted into Supabase—duplicates are skipped
- **Automation**: Runs automatically on the **1st of every month** at 00:00 UTC via GitHub Actions
- **Manual trigger**: Can be run manually from the GitHub Actions tab (workflow_dispatch)

## Setup

### Prerequisites

- Python 3.11+
- Supabase account with a project
- GitHub repository (for automated scheduling)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/nimra06/dpd-scraper.git
cd dpd-scraper
```

2. Install dependencies:
```bash
pip install requests openpyxl
```

### Configuration

1. **Supabase Setup**:
   - Create a Supabase project
   - Get your project URL and service role key from the Supabase dashboard

2. **GitHub Secrets** (for automated runs):
   - Go to your repository Settings → Secrets and variables → Actions
   - Add the following secrets:
     - `SUPABASE_URL`: Your Supabase project URL
     - `SUPABASE_SERVICE_ROLE_KEY`: Your Supabase service role key

## Usage

### Manual Run

Run the monthly sync script manually:

```bash
cd scripts
python run_monthly_sync.py --all \
  --supabase-url YOUR_SUPABASE_URL \
  --service-role-key YOUR_SERVICE_ROLE_KEY
```

Or use environment variables:

```bash
export SUPABASE_URL=your_supabase_url
export SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
cd scripts
python run_monthly_sync.py --all
```

### Automated Monthly Run

The GitHub Actions workflow (`.github/workflows/monthly-sync.yml`):
- Runs **automatically on the 1st of every month** at 00:00 UTC
- Scrapes the latest products from the Health Canada DPD site each run
- Syncs only **new** products to Supabase (existing products by DIN are skipped)
- Can also be triggered manually from the GitHub Actions tab

### Options

- `--all`: Fetch all records (recommended for monthly sync)
- `--limit N`: Fetch only the first N records (for testing)
- `--table-name NAME`: Custom table name (default: `nexara_all_source`)
- `--batch-size N`: Number of rows per insert batch (default: 500)
- `--max-retries N`: Retry attempts for failed requests (default: 3)
- `--status-interval N`: Print progress every N records (default: 100)

## How It Works

1. **Scraping phase**
   - Fetches product listings from Health Canada's Drug Product Database (DPD)
   - Enriches each product with detail and saves checkpoints as it goes
   - Runs up to 5 hours per job; if it times out, partial results are synced from checkpoints

2. **Mapping phase**
   - Maps DPD records into `nexara_all_source` format
   - Uses `source` = `"HC"` and `row_uid` = `"HC:{DIN}"`

3. **Deduplication**
   - Loads existing `row_uid` values from Supabase (source = `"HC"`)
   - Inserts only products whose `row_uid` is not already present (i.e. **new** products)

4. **Sync phase**
   - Batch-inserts only new records into `nexara_all_source`
   - The table must already exist (it is not created by the script)

## Scripts

- `scripts/run_monthly_sync.py`: Main orchestration (scrape + sync to Supabase)
- `scripts/sync_from_checkpoints.py`: Sync partial results from checkpoints (used when scraper times out)
- `scripts/supabase_sync.py`: Supabase sync utilities
- `dpd_scraper/dpd_scraper.py`: DPD site scraper

## Troubleshooting

### Table doesn't exist
The `nexara_all_source` table must already exist in your Supabase database. The script will not auto-create it due to its complex schema. If the table doesn't exist, you'll need to create it manually in Supabase.

### No new records found
If all scraped records already exist in Supabase, the script will exit with a message. This is normal behavior.

### GitHub Actions failures
- Check that secrets are properly configured
- Review workflow logs in the Actions tab
- Ensure Supabase credentials are valid

## License

[Add your license here]
