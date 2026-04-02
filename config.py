# Configuration file for ETL pipeline. Contains constants, maps, file paths, and settings.
# Serves as a single source of truth for configuration values used across the project.

import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import src.parsers
from functools import partial

# Load environment variables from .env file
load_dotenv()
# ===== Directory Paths =====
ROOT_DIR = Path(__file__).resolve().parent
INBOX_DIR = ROOT_DIR / "data" / "landing" / "inbox"
PROCESSED_DIR = ROOT_DIR / "data" / "landing" / "processed"
FAILED_DIR = ROOT_DIR / "data" / "landing" / "failed"
QUARANTINE_DIR = ROOT_DIR / "data" / "quarantine"
BRONZE_DIR = ROOT_DIR / "data" / "bronze"
SILVER_DIR = ROOT_DIR / "data" / "silver"
GOLD_DIR = ROOT_DIR / "data" / "gold"
LOG_DIR = ROOT_DIR / "logs"

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
# this mapping to determine (1) which parser to use, (2) write mode, and (3) target bronze table.
#
# One file → One parser → One DataFrame → One bronze table

INGESTION_MAPPINGS = {
    "Harvest_INAVBSKT_ALL.": {
        "parser": src.parsers.extract_pcf,
        "rename": False,
        "load_type": "append",
        "bronze_table": "pcf_inav_baskets"
    },
    "Harvest_BSKT_ALL.": {
        "parser": src.parsers.extract_pcf,
        "rename": False,
        "load_type": "append",
        "bronze_table": "pcf_creation_baskets"
    },
    "All_Positions": {
        "parser": pd.read_csv,
        "rename": False,
        "load_type": "append",
        "bronze_table": "all_positions"
    },
    "PLF_Positions": {
        "parser": pd.read_csv,
        "load_type": "append",
        "rename": False,
        "bronze_table": "plf_positions"
    },
    "Accounting_Cash_Statement": {
        "parser": pd.read_csv,
        "rename": True,
        "load_type": "overwrite",
        "bronze_table": "accounting_cash_statements"
    },
    "All_Corporate_Actions": {
        "parser": pd.read_csv,
        "rename": True,
        "load_type": "overwrite",
        "bronze_table": "all_corporate_actions"
    },
    "Cash_Forecast_Transactions": {
        "parser": pd.read_csv,
        "rename": True,
        "load_type": "overwrite",
        "bronze_table": "cash_forecast_transactions"
    },
    "Custody_Positions": {
        "parser": pd.read_csv,
        "rename": True,
        "load_type": "append",
        "bronze_table": "custody_positions"
    },
    "Custody_Transactions": {
        "parser": pd.read_csv,
        "rename": True,
        "load_type": "overwrite",
        "bronze_table": "custody_transactions"
    },
    "Daily_Model_Holdings": {
        "parser": pd.read_csv,
        "rename": True,
        "load_type": "append",
        "bronze_table": "daily_model_holdings"
    },
    "Daily_Net_Asset_Values": {
        "parser": pd.read_csv,
        "rename": True,
        "load_type": "append",
        "bronze_table": "daily_net_asset_values"
    },
    "Distribution_Liability": {
        "parser": pd.read_csv,
        "rename": True,
        "load_type": "append",
        "bronze_table": "distribution_liabilities"
    },
    "Loan_Balances": {
        "parser": pd.read_csv,
        "rename": True,
        "load_type": "append",
        "bronze_table": "loan_balances"
    },
    "Opening_Cash_Balances": {
        "parser": pd.read_csv,
        "rename": True,
        "load_type": "append",
        "bronze_table": "opening_cash_balances"
    },
    "Pending_FX_Accounting": {
        "parser": pd.read_csv,
        "rename": True,
        "load_type": "append",
        "bronze_table": "pending_fx_accounting_records"
    },
    "Top10_FX_Pending": {
        "parser": pd.read_csv,
        "rename": True,
        "load_type": "append",
        "bronze_table": "top10_fx_pending_records"
    },
    "Top10_Net_Asset_Value": {
        "parser": pd.read_csv,
        "rename": True,
        "load_type": "append",
        "bronze_table": "top10_net_asset_values"
    }
}
