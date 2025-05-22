from pathlib import Path
from dataclasses import dataclass
from dotenv import load_dotenv
import os, sys, re, time, logging, threading, signal


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


# ─── LOGGING ────────────────────────────────────────

LOGFILE = f"scraper_{…}.log"
LEVEL = logging.DEBUG if "-v" in sys.argv else logging.INFO
logging.basicConfig(…)
log = logging.getLogger(__name__)

LOGFILE = f"scraper_{time.strftime('%Y%m%d_%H%M%S')}.log"
LEVEL = logging.DEBUG if "-v" in sys.argv else logging.INFO
logging.basicConfig(level=LEVEL,
                    format="%(asctime)s | %(levelname)-7s | %(message)s",
                    handlers=[logging.FileHandler(LOGFILE, encoding="utf-8"),
                              logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)



@dataclass
class Settings:
    # ─── ENV / CONFIG ───────────────────────────────────
    API_KEY = os.getenv("GOOGLE_API_KEY"); CX_ID = os.getenv("GOOGLE_CX_ID")

    DEFAULT_PARTS = (
        "contact,about,impress,impressum,kontakt,privacy,sales,"
        "investor,procurement,suppliers,urea,adblue,europe,switzerland"
    )
    PRIORITY_PARTS = [p.strip().lower() for p in os.getenv("PRIORITY_PATH_PARTS", DEFAULT_PARTS).split(",") if p.strip()]
    MAX_FALLBACK_PAGES = int(os.getenv("MAX_FALLBACK_PAGES", "12"))
    PROCESS_PDFS = os.getenv("PROCESS_PDFS", "0").lower() in {"1","true","yes"}
    INSECURE_SSL = os.getenv("ALLOW_INSECURE_SSL", "0").lower() in {"1","true","yes"}
    MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
    GOOGLE_SAFE_INTERVAL = float(os.getenv("GOOGLE_SAFE_INTERVAL", "0.8"))
    GOOGLE_MAX_RETRIES = 5
    _last_google_ts = 0.0; _google_lock = threading.Lock()
    # add more as needed

settings = Settings()

if not settings.api_key or not settings.cx_id:
    raise RuntimeError("GOOGLE_API_KEY and/or GOOGLE_CX_ID not set")
