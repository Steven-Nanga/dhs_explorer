import os
from pathlib import Path
from urllib.parse import urlparse

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BASE_DIR = Path(__file__).resolve().parent.parent

# Cloud providers (Neon, Supabase, Railway) set DATABASE_URL.
# If present, parse it into individual fields; otherwise use DHS_DB_* vars.
DATABASE_URL = os.getenv("DATABASE_URL", "")

if DATABASE_URL:
    _parsed = urlparse(DATABASE_URL)
    DB_HOST = _parsed.hostname or "localhost"
    DB_PORT = _parsed.port or 5432
    DB_NAME = (_parsed.path or "/dhs").lstrip("/")
    DB_USER = _parsed.username or "postgres"
    DB_PASSWORD = _parsed.password or ""
    DB_SSLMODE = "require"
else:
    DB_HOST = os.getenv("DHS_DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DHS_DB_PORT", "5432"))
    DB_NAME = os.getenv("DHS_DB_NAME", "dhs")
    DB_USER = os.getenv("DHS_DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DHS_DB_PASSWORD", "")
    DB_SSLMODE = os.getenv("DHS_DB_SSLMODE", "prefer")

DATA_DIR = Path(os.getenv("DHS_DATA_DIR", str(BASE_DIR / "data")))
MIGRATIONS_DIR = BASE_DIR / "migrations"

MAX_TYPED_COLUMNS = 1500

CORE_RECODE_TYPES = {"HR", "IR", "PR", "KR", "BR", "MR"}

COUNTRY_NAMES = {
    "MW": "Malawi",
    "KE": "Kenya",
    "TZ": "Tanzania",
    "UG": "Uganda",
    "ZM": "Zambia",
    "ZW": "Zimbabwe",
    "ET": "Ethiopia",
    "NG": "Nigeria",
    "GH": "Ghana",
    "CM": "Cameroon",
    "SN": "Senegal",
    "BF": "Burkina Faso",
    "ML": "Mali",
    "NE": "Niger",
    "BJ": "Benin",
    "TG": "Togo",
    "CD": "DR Congo",
    "MZ": "Mozambique",
    "RW": "Rwanda",
    "BD": "Bangladesh",
    "IN": "India",
    "NP": "Nepal",
    "PK": "Pakistan",
    "PH": "Philippines",
    "KH": "Cambodia",
    "HT": "Haiti",
    "PE": "Peru",
    "CO": "Colombia",
    "EG": "Egypt",
    "JO": "Jordan",
}
