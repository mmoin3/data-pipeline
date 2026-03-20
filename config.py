# Configuration file for ETL pipeline. Contains constants, maps, file paths, and settings.
# Serves as a single source of truth for configuration values used across the project.

import os
import logging
from pathlib import Path
from dotenv import load_dotenv
import urllib

# Load environment variables from .env file
load_dotenv()

# ===== Directory Paths =====
ROOT_DIR = Path(__file__).resolve().parent
RAW_DATA_DIR = os.path.join(ROOT_DIR, "data", "raw")
PROCESSED_DATA_DIR = os.path.join(ROOT_DIR, "data", "processed")
QUARANTINED_DATA_DIR = os.path.join(ROOT_DIR, "data", "quarantined")
LOG_DIR = os.path.join(ROOT_DIR, "logs")

# ===== Database Configuration =====
DB_PARAMS = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('DB_SERVER')};"
    f"DATABASE={os.getenv('DB_NAME')};"
    "Trusted_Connection=yes;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)
DB_CONN_STR_SQLSERVER = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(DB_PARAMS)}"

# ===== Logging Configuration =====
LOG_FILE = os.path.join(LOG_DIR, "etl.log")
LOG_LEVEL = "INFO"

# ===== Email Configuration =====
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")

# ===== MFT Configuration =====
MFT_BASE_URL = os.getenv("MFT_BASE_URL")
MFT_USERNAME = os.getenv("MFT_USERNAME")
MFT_PASSWORD = os.getenv("MFT_PASSWORD")
MFT_CERT_PATH = os.getenv("MFT_CERT_PATH")
MFT_KEY_PATH = os.getenv("MFT_KEY_PATH")

# ===== Data Cleaning & Type Mapping =====
NULL_LIKE_VALUES = ["", " ", "NA", "NAN", "N/A", "NULL", "NONE", "-"]

FUND_METRICS_TYPE_MAP = {
    "trade_date": "datetime64[ns]",
    "creation_unit_size": int,
}

FUND_HOLDINGS_TYPE_MAP = {
    "trade_date": "datetime64[ns]",
    "share": int,
    "supplemental_id_1": int,
}