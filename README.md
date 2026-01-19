# Drug Inspection Scraper

A Python-based scraper for Health Canada's Drug & Health Product inspection records with automated monthly syncing to Supabase.

## Features

- **Scraping**: Fetches inspection records from Health Canada's GMP database
- **Incremental Sync**: Only syncs new records to Supabase (compares with existing data)
- **Monthly Automation**: Runs automatically on the 1st of every month via GitHub Actions
- **Manual Trigger**: Can be triggered manually via GitHub Actions workflow_dispatch

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

The GitHub Actions workflow (`.github/workflows/monthly-sync.yml`) is configured to:
- Run automatically on the 1st of every month at 00:00 UTC
- Can be manually triggered from the GitHub Actions tab

### Options

- `--all`: Fetch all records (recommended for monthly sync)
- `--limit N`: Fetch only the first N records (for testing)
- `--table-name NAME`: Custom table name (default: `nexara_all_source`)
- `--batch-size N`: Number of rows per insert batch (default: 500)
- `--max-retries N`: Retry attempts for failed requests (default: 3)
- `--status-interval N`: Print progress every N records (default: 100)

## How It Works

1. **Scraping Phase**:
   - Fetches listing data from Health Canada's Drug Inspection API
   - For each inspection, fetches detailed information
   - Saves results to JSON format

2. **Mapping Phase**:
   - Transforms inspection records to `nexara_all_source` table format
   - Sets `source` = "HC_INSPECTIONS"
   - Creates `row_uid` = "HC_INSPECTIONS:{inspection_number}"
   - Maps inspection data to available table columns

3. **Comparison Phase**:
   - Fetches all existing `row_uid` values from Supabase (filtered by source="HC_INSPECTIONS")
   - Compares scraped records with existing records
   - Filters out duplicates based on `row_uid`

4. **Sync Phase**:
   - Only new records are inserted into `nexara_all_source` table
   - Uses batch inserts for efficiency
   - Note: The table must already exist (it won't be auto-created)

## Scripts

- `scripts/scrape_drug_inspections.py`: Main scraper script
- `scripts/supabase_sync.py`: Supabase sync utilities
- `scripts/run_monthly_sync.py`: Orchestration script (scrape + sync)
- `scripts/xlsx_to_csv.py`: Utility to convert XLSX to CSV

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
