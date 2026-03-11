#======================================================================
# Configuration file for ETL pipeline. Contains constants,maps, file paths, and settings.
# Serves as a single source of truth for configuration values used across the project.
#======================================================================

import os, sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Save Important paths as constants
ROOT_DIR = Path(__file__).parent
RAW_DATA_DIR = os.path.join(ROOT_DIR, "data", "raw")
PROCESSED_DATA_DIR = os.path.join(ROOT_DIR, "data", "processed")
ARCHIVED_DATA_DIR = os.path.join(ROOT_DIR, "data", "archived")

# Define Database connection strings
DB_PATH = os.path.join(ROOT_DIR, "FundOperations.db")
DB_CONN_STR = "sqlite:///{DB_PATH}"

# Load .env variables
load_dotenv()
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")
MFT_USERNAME = os.getenv("MFT_USERNAME")
MFT_PASSWORD = os.getenv("MFT_PASSWORD")

MFT_URL = "https://mft.statestreet.com/auth/login"
MFT_CERT_PATH = str(ROOT_DIR / "certs" / "client.crt")
MFT_KEY_PATH = str(ROOT_DIR / "certs" / "client.key")


# Logger settings
LOG_FILE = os.path.join(ROOT_DIR, "logs", "etl.log")
LOG_LEVEL = "INFO"

NULL_LIKE_VALUES = ["", " ", "NA", "N/A", "NULL", "NONE", "-"]

BOOLEAN_MAP = {
    "TRUE": True,
    "FALSE": False,
    "Y": True,
    "N": False,
    "YES": True,
    "NO": False,
    "1": True,
    "0": False,
}

# Metadata type map for NAV/INAV files
FUND_METRICS_TYPE_MAP = {
    "TRADE_DATE": "datetime64[ns]",
    "SS_LONG_CODE": str,
    "FULL_NAME": str,
    "TICKER": str,
    "BASE_CURRENCY": str,
    "CREATION_UNIT_SIZE": float,
    "DOMICILE": str,
    "ESTIMATED_DIVIDENDS": float,
    "PRODUCT_STRUCTURE": str,
    "EST_INC": float,
    "SETTLEMENT_CYCLE": str,
    "ASSET_CLASS": str,
    "ESTIMATED_EXPENSE": float,
    "CREATE_FEE": float,
    "ESTIMATED_CASH_COMPONENT": float,
    "NAV": float,
    "UNDISTRIBUTED_NET_INCOME_PER_SHARE": float,
    "BASKET_MARKET_VALUE": float,
    "REDEEM_FEE": float,
    "ACTUAL_CASH_COMPONENT": float,
    "NAV_PER_CREATION_UNIT": float,
    "UNDISTRIBUTED_NET_INCOME_PER_CREATION_UNIT": float,
    "BASKET_SHARES": float,
    "CREATE_VARIABLE_FEE": float,
    "NAV_LESS_UNDISTRIBUTED_NET_INCOME": float,
    "ACTUAL_CASH_IN_LIEU": float,
    "ESTIMATED_CASH_IN_LIEU": float,
    "REDEEM_VARIABLE_FEE": float,
    "ETF_SHARES_OUTSTANDING": float,
    "ACTUAL_INTEREST": float,
    "ESTIMATED_INTEREST": float,
    "EXPENSE_RATIO": float,
    "TOTAL_NET_ASSETS": float,
    "ACTUAL_TOTAL_CASH": float,
    "ESTIMATED_TOTAL_CASH": float,
    "THRESHOLD": str,
}

FUND_HOLDINGS_TYPE_MAP = {
    "TRADE_DATE": "datetime64[ns]",
    "SS_LONG_CODE": str,
    "CUSIP": str,
    "TICKER": str,
    "SEDOL": str,
    "ISIN": str,
    "DESCRIPTION": str,
    "CUR": str,
    "ISO": str,
    "SHARES": int,
    "ORIGINAL_FACE": int,
    "INTEREST": float,
    "LOCAL_PRICE": float,
    "LOCAL_MV": float,
    "FOREX": float,
    "BASE_PRICE": float,
    "BASE_MV": float,
    "WEIGHT": float,
    "CIL": str,
    "EST_DIVIDEND": float,
    "LOT": int,
    "NEW": str,
    "SHARE_CHANGE": int,
    "INT_FACTOR": int,
    "PAR_ADJUSTMENT_FACTOR": int,
    "SUPPLEMENTAL_ID_1": int,
    "SUPPLEMENTAL_ID_2": str,
}