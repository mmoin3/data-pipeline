# Configuration file for ETL pipeline. Contains constants, maps, file paths, and settings.
# Serves as a single source of truth for configuration values used across the project.

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
# ===== Directory Paths =====
ROOT_DIR = Path(__file__).resolve().parent
INBOX_DIR = ROOT_DIR / "data" / "landing" / "inbox"
PROCESSED_DIR = ROOT_DIR / "data" / "landing" / "processed"
FAILED_DIR = ROOT_DIR / "data" / "landing" / "failed"
BRONZE_DIR = ROOT_DIR / "data" / "bronze"
SILVER_DIR = ROOT_DIR / "data" / "silver"
GOLD_DIR = ROOT_DIR / "data" / "gold"

# ===== Database Configuration =====
DUCKDB_FILE = ROOT_DIR / "data" / "FundOperations.duckdb"
DB_CONN_STR = f"duckdb:///{DUCKDB_FILE}"

# ===== Email Configuration =====
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")

# ===== MFT Configuration =====
MFT_BASE_URL = os.getenv("MFT_BASE_URL")
MFT_USERNAME = os.getenv("MFT_USERNAME")
MFT_PASSWORD = os.getenv("MFT_PASSWORD")
MFT_CERT_PATH = ROOT_DIR.parent/"mmoinclient.crt"
MFT_KEY_PATH = ROOT_DIR.parent/"mmoinclient.key"


# ===== Ingestion Mappings =====
# Defines the ingestion workflow: when a file is discovered, its filename is matched against
# this mapping to determine (1) which parser to use, (2) how to load the data (append/replace),
# and (3) where each parsed output goes in the bronze layer.
#
# For example, any file with "Harvest_INAVBSKT_ALL" in its name will:
#   - Be parsed by BasketsParser
#   - Return two DataFrames: "metrics" and "holdings"
#   - "metrics" gets loaded into pcf_creation_bskt_metrics
#   - "holdings" gets loaded into pcf_creation_bskt_holdings
#   - Use append mode (don't replace existing data)
#
# Keys in "outputs" MUST match the keys returned by parser.parse() exactly.

INGESTION_MAPPINGS = {
    "Harvest_INAVBSKT_ALL": {
        "parser": "BasketsParser",
        "load_type": "append",
        "outputs": {
            "metrics": "pcf_creation_bskt_metrics",
            "holdings": "pcf_creation_bskt_holdings"
        }
    },
    "Harvest_BSKT": {
        "parser": "BasketsParser",
        "load_type": "append",
        "outputs": {
            "metrics": "pcf_inav_bskt_metrics",
            "holdings": "pcf_inav_bskt_holdings"
        }
    },
    "All_Positions": {
        "parser": "BaseParser",
        "load_type": "append",
        "outputs": {
            "data": "all_positions"
        }
    },
    "PLF_Positions": {
        "parser": "BaseParser",
        "load_type": "append",
        "outputs": {
            "data": "plf_positions"
        }
    },
}
