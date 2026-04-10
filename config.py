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
WAREHOUSE_DIR = Path(r"C:\Users\mmoin\PYTHON PROJECTS\DataWareHouse")
LOG_DIR = Path(r"C:\Users\mmoin\PYTHON PROJECTS\logs")
RAW_DATA_DIR = WAREHOUSE_DIR / "data" / "0_raw data"
INBOX_DIR = WAREHOUSE_DIR / "landing" / "inbox"
PROCESSED_DIR = WAREHOUSE_DIR / "landing" / "processed"
FAILED_DIR = WAREHOUSE_DIR / "landing" / "failed"
QUARANTINE_DIR = WAREHOUSE_DIR / "data" / "quarantine"
BRONZE_DIR = WAREHOUSE_DIR / "bronze"
SILVER_DIR = WAREHOUSE_DIR / "silver"
GOLD_DIR = WAREHOUSE_DIR / "gold"

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

# ===== API Configuration =====
FIGI_API_KEY = os.getenv("FIGI_API_KEY")


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
    "Harvest_BSKT": {
        "parser": src.parsers.extract_pcf,
        "rename": False,
        "load_type": "append",
        "bronze_table": "pcf_creation_baskets"
    },
    "All_Positions": {
        "parser": partial(pd.read_csv, na_values="", keep_default_na=False),
        "rename": False,
        "load_type": "append",
        "bronze_table": "all_positions"
    },
    "PLF_Positions": {
        "parser": partial(pd.read_csv, na_values="", keep_default_na=False),
        "load_type": "append",
        "rename": False,
        "bronze_table": "plf_positions"
    },
    "Harvest_CIL_ALL.": {
        "parser": src.parsers.extract_cil,
        "rename": False,
        "load_type": "append",
        "bronze_table": "cash_in_lieu_records"
    },
    "Harvest_INKIND.": {
        # skip first row which contains file-level metadata, not column headers
        "parser": partial(pd.read_csv, na_values="", keep_default_na=False, skiprows=1),
        "rename": False,
        "load_type": "append",
        "bronze_table": "inkind_orders"
    },
    "Harvest_NAV_ALL.": {
        "parser": src.parsers.extract_cil,
        "rename": False,
        "load_type": "append",
        "bronze_table": "pcf_nav_records"
    },
    "Harvest_Preburst_INKIND_ALL.": {
        "parser": partial(pd.read_csv, na_values="", keep_default_na=False, skiprows=1),
        "rename": False,
        "load_type": "append",
        "bronze_table": "preburst_inkind_orders"
    },
    "Harvest Price File": {
        "parser": src.parsers.extract_accounting_navs,
        "rename": False,
        "load_type": "append",
        "bronze_table": "accounting_nav_records"
    },
    "Accounting_Cash_Statement": {
        "parser": partial(pd.read_csv, na_values="", keep_default_na=False),
        "rename": True,
        "load_type": "overwrite",
        "bronze_table": "accounting_cash_statements"
    },
    "All_Corporate_Actions": {
        "parser": partial(pd.read_csv, na_values="", keep_default_na=False),
        "rename": True,
        "load_type": "overwrite",
        "bronze_table": "all_corporate_actions"
    },
    "Cash_Forecast_Transactions": {
        "parser": partial(pd.read_csv, na_values="", keep_default_na=False),
        "rename": True,
        "load_type": "overwrite",
        "bronze_table": "cash_forecast_transactions"
    },
    "Custody_Positions": {
        "parser": partial(pd.read_csv, na_values="", keep_default_na=False),
        "rename": True,
        "load_type": "append",
        "bronze_table": "custody_positions"
    },
    "Custody_Transactions": {
        "parser": partial(pd.read_csv, na_values="", keep_default_na=False),
        "rename": True,
        "load_type": "overwrite",
        "bronze_table": "custody_transactions"
    },
    "Daily_Model_Holdings": {
        "parser": partial(pd.read_csv, na_values="", keep_default_na=False),
        "rename": True,
        "load_type": "append",
        "bronze_table": "daily_model_holdings"
    },
    "Daily_Net_Asset_Values": {
        "parser": partial(pd.read_csv, na_values="", keep_default_na=False),
        "rename": True,
        "load_type": "append",
        "bronze_table": "daily_net_asset_values"
    },
    "Distribution_Liability": {
        "parser": partial(pd.read_csv, na_values="", keep_default_na=False),
        "rename": True,
        "load_type": "append",
        "bronze_table": "distribution_liabilities"
    },
    "Loan_Balances": {
        "parser": partial(pd.read_csv, na_values="", keep_default_na=False),
        "rename": True,
        "load_type": "append",
        "bronze_table": "loan_balances"
    },
    "Opening_Cash_Balances": {
        "parser": partial(pd.read_csv, na_values="", keep_default_na=False),
        "rename": True,
        "load_type": "append",
        "bronze_table": "opening_cash_balances"
    },
    "Pending_FX_Accounting": {
        "parser": partial(pd.read_csv, na_values="", keep_default_na=False),
        "rename": True,
        "load_type": "append",
        "bronze_table": "pending_fx_accounting_records"
    },
    "Top10_FX_Pending": {
        "parser": partial(pd.read_csv, na_values="", keep_default_na=False),
        "rename": True,
        "load_type": "append",
        "bronze_table": "top10_fx_pending_records"
    },
    "Top10_Net_Asset_Value": {
        "parser": partial(pd.read_csv, na_values="", keep_default_na=False),
        "rename": True,
        "load_type": "append",
        "bronze_table": "top10_net_asset_values"
    },
    "Harvest_UCF": {
        "parser": partial(pd.read_csv, na_values="", keep_default_na=False),
        "rename": False,
        "load_type": "append",
        "bronze_table": "ucf_records"
    },
    "FPTRAD_report": {
        "parser": partial(pd.read_csv, na_values="", keep_default_na=False, on_bad_lines="skip"),
        "rename": True,
        "load_type": "append",
        "bronze_table": "daily_net_sales"
    },
    "securities.csv": {
        "parser": partial(pd.read_csv, na_values="", keep_default_na=False),
        "rename": False,
        "load_type": "append",
        "bronze_table": "all_securities"
    },
    "exchanges.csv": {
        "parser": partial(pd.read_csv, na_values="", keep_default_na=False),
        "rename": False,
        "load_type": "append",
        "bronze_table": "all_exchanges_codes"
    },
    "Harvest Canadian ETF_": {
        "parser": partial(pd.read_excel, sheet_name="Harvest Canadian ETF", dtype={"Shares": str}),
        "rename": False,
        "load_type": "append",
        "bronze_table": "cds_monthly_participant_reports"
    },
}

# ===== Silver Layer Transformation Mappings =====
# Define SQL transformations for each bronze table.
# Used by src.ingestor.upsert_to_silver() to transform and merge data.
#
# Each mapping has:
#   "transform_sql": SQL query that reads FROM bronze, applies transforms
#   "merge_keys": List of columns to upsert on (optional, omit for overwrite mode)
#
# The SQL query automatically has access to a "bronze" view of the raw bronze table.
# Include all columns you want in silver, renamed/cast as needed.
# Metadata columns (ingested_at, source_file, batch_id) pass through automatically.
#
# Example: Normalize names, cast qty as float, filter nulls
# "pcf_inav_baskets": {
#     "transform_sql": '''
#         SELECT
#             LOWER(TRIM(fundcode)) AS fund_id,
#             LOWER(TRIM(isin)) AS isin_code,
#             TRY_CAST(units AS FLOAT64) AS units_issued,
#             ingested_at,
#             source_file,
#             batch_id
#         FROM bronze
#         WHERE fundcode IS NOT NULL
#     ''',
#     "merge_keys": ["fund_id"],
# }

SILVER_MAPPINGS = {
    # Add table transformations here as you build silver layer
}
