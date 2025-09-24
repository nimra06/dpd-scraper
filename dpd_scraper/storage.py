# dpd_scraper/storage.py
import os, time
from supabase import create_client
from dotenv import load_dotenv
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")  # service role (private)
SUPABASE_BUCKET = os.environ.get("SUPABASE_BUCKET", "dpd-artifacts")

def upload_xlsx(xlsx_path: str) -> str:
    if not (SUPABASE_URL and SUPABASE_KEY):
        raise RuntimeError("Supabase not configured")

    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    key = f"{time.strftime('%Y%m%d_%H%M%S')}/snapshot.xlsx"
    with open(xlsx_path, "rb") as f:
        client.storage.from_(SUPABASE_BUCKET).upload(
            key,
            f,
            file_options={
                "content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "x-upsert": "true",
            },
        )
    # If bucket is public, you can build a public URL; else sign it server-side
    public_base = os.environ.get("SUPABASE_PUBLIC_STORAGE_URL")  # optional pre-configured CDN URL
    return f"{public_base}/{SUPABASE_BUCKET}/{key}" if public_base else key
