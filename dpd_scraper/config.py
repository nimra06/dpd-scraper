import os
from dotenv import load_dotenv
load_dotenv()
ALPHABET = list("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
TRIE_CAP = int(os.getenv("SCRAPER_TRIE_CAP", "25"))
MAX_DEPTH = int(os.getenv("SCRAPER_MAX_DEPTH", "3"))
TARGET_MIN_ROWS = int(os.getenv("SCRAPER_TARGET_MIN_ROWS", "50000"))
ENRICH_FLUSH_EVERY = int(os.getenv("SCRAPER_ENRICH_FLUSH_EVERY", "50"))
USER_AGENT = os.getenv("SCRAPER_USER_AGENT", "chrono-dpd-scraper/1.0")
REQUEST_SLEEP = float(os.getenv("SCRAPER_REQUEST_SLEEP", "0.03"))
