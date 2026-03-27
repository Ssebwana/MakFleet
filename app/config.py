from pathlib import Path
from dotenv import load_dotenv
import os

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

PG_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "dbname": os.getenv("PG_DB", "makfleet"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", ""),
}

WINDOW_MINUTES = int(os.getenv("WINDOW_MINUTES", "5"))
MODEL_DIR = BASE_DIR / os.getenv("MODEL_DIR", "data/processed")
MODEL_DIR.mkdir(parents=True, exist_ok=True)