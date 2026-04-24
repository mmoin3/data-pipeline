# Configuration file for ETL pipeline. Contains constants, maps, file paths, and settings.
# Serves as a single source of truth for configuration values used across the project.

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional, Type, Union
import os
import re
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import src.parsers
from functools import partial

# Custom type for percentage values


class Percentage:
    """Marker type for percentage columns (stored as decimal, e.g., 0.05 for 5%)"""
    pass


# Load environment variables from .env file
load_dotenv()
# ===== Directory Paths =====
ROOT_DIR = Path(__file__).resolve().parent
WAREHOUSE_DIR = Path(r"C:\Users\mmoin\PYTHON PROJECTS\DataWareHouse")
LOG_DIR = Path(r"C:\Users\mmoin\PYTHON PROJECTS\logs")
RAW_DATA_DIR = WAREHOUSE_DIR / "data" / "0_raw data"
INBOX_DIR = WAREHOUSE_DIR / "bronze" / "inbox"
PROCESSED_DIR = WAREHOUSE_DIR / "bronze" / "processed"
FAILED_DIR = WAREHOUSE_DIR / "bronze" / "failed"
QUARANTINE_DIR = WAREHOUSE_DIR / "data" / "quarantine"
BRONZE_DIR = WAREHOUSE_DIR / "bronze" / "staging_area"
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
@dataclass
class ColumnMapping:
    # int, str, float, bool, datetime, or "pct" for percentage
    source_dtype: Union[Type, str]
    target_name: str                  # desired column name in silver
    datetime_format: str = None       # for datetime parsing, e.g. "%m/%d/%Y"


@dataclass
class SilverMapping:
    silver_table_name: str            # target silver table name
    table_type: str                   # "reference" | "fact"
    primary_keys: Union[str, tuple]
    columns: dict[str, ColumnMapping] = field(default_factory=dict)
    dedup_timestamp: str = "ingested_at"

    def __post_init__(self):
        """Normalize primary_keys to always be a tuple."""
        if isinstance(self.primary_keys, str):
            self.primary_keys = (self.primary_keys,)


@dataclass
class IngestionMapping:
    parser: object                    # Callable parser function
    bronze_table: str                 # target bronze table name
    rename: bool = False              # rename file with timestamp
    load_type: str = "append"         # "append" or "overwrite"
    # optional silver transformation
    silver_mapping: Optional[SilverMapping] = None


INGESTION_MAPPINGS = {
    # Harvest PCF Files
    re.compile(r"Harvest_INAVBSKT_ALL\.\d{8}\.csv", re.IGNORECASE): IngestionMapping(
        parser=src.parsers.extract_pcf,
        bronze_table="pcf_inav_baskets",
    ),
    re.compile(r"Harvest_BSKT_ALL\.\d{8}\.csv", re.IGNORECASE): IngestionMapping(
        parser=src.parsers.extract_pcf,
        bronze_table="pcf_creation_baskets",
    ),
    # Position Files
    re.compile(r"All_Positions\d{8}\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        bronze_table="all_positions",
    ),
    re.compile(r"PLF_Positions\d{8}\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        bronze_table="plf_positions",
    ),
    # Cash In Lieu & NAV Records
    re.compile(r"Harvest_CIL_ALL\.\d{8}\.(csv|txt)", re.IGNORECASE): IngestionMapping(
        parser=src.parsers.extract_cil,
        bronze_table="cash_in_lieu_records",
    ),
    re.compile(r"Harvest_INKIND\.\d{8}\.txt", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="",
                       keep_default_na=False, skiprows=1),
        bronze_table="inkind_orders",
    ),
    re.compile(r"Harvest_NAV_ALL\.\d{8}\.csv", re.IGNORECASE): IngestionMapping(
        parser=src.parsers.extract_cil,
        bronze_table="pcf_nav_records",
    ),
    re.compile(r"Harvest_Preburst_INKIND_ALL\.\d{8}\.txt", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="",
                       keep_default_na=False, skiprows=1),
        bronze_table="preburst_inkind_orders",
    ),
    # Harvest Price File - Format: "Harvest Price File -MMDDYYYY.XLS"
    re.compile(r"Harvest Price File -\d{8}\.(xls|xlsx)", re.IGNORECASE): IngestionMapping(
        parser=src.parsers.extract_accounting_navs,
        bronze_table="accounting_nav_records",
    ),
    # Daily Reports with timestamp (YYYYMMDD_HHMM)
    re.compile(r"Accounting_Cash_Statement\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        load_type="overwrite",
        bronze_table="accounting_cash_statements",
    ),
    re.compile(r"All_Corporate_Actions\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        load_type="overwrite",
        bronze_table="all_corporate_actions",
    ),
    re.compile(r"Cash_Forecast_Transactions\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        load_type="overwrite",
        bronze_table="cash_forecast_transactions",
    ),
    re.compile(r"Custody_Positions\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="custody_positions",
    ),
    re.compile(r"Custody_Transactions\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        load_type="overwrite",
        bronze_table="custody_transactions",
    ),
    re.compile(r"Daily_Model_Holdings\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="daily_model_holdings",
    ),
    re.compile(r"Daily_Net_Asset_Values\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="daily_net_asset_values",
    ),
    re.compile(r"Distribution_Liability\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="distribution_liabilities",
    ),
    re.compile(r"Loan_Balances\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="loan_balances",
    ),
    re.compile(r"Opening_Cash_Balances\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="opening_cash_balances",
    ),
    re.compile(r"Pending_FX_Accounting\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="pending_fx_accounting_records",
    ),
    re.compile(r"Top10_FX_Pending\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="top10_fx_pending_records",
    ),
    re.compile(r"Top10_Net_Asset_Value\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="top10_net_asset_values",
    ),
    re.compile(r"Harvest_UCF", re.IGNORECASE): IngestionMapping(
        parser=src.parsers.extract_ucf,
        bronze_table="ucf_records",
        silver_mapping=SilverMapping(
            silver_table_name="ucf_records",
            table_type="fact",
            primary_keys=("trade_date", "fund_ticker",
                          "description", "ticker"),
            columns={
                "TRADE_DATE": ColumnMapping(datetime, "trade_date", "%Y%m%d"),
                "FUND_TICKER": ColumnMapping(str, "fund_ticker"),
                "SHARES": ColumnMapping(int, "share_count"),
                "FUND": ColumnMapping(str, "ss_id"),
                "CUSIP": ColumnMapping(str, "cusip_id"),
                "ISIN": ColumnMapping(str, "isin_id"),
                "SEDOL": ColumnMapping(str, "sedol_id"),
                "FOREX": ColumnMapping(float, "forex_rate"),
                "LOCAL_PRICE": ColumnMapping(float, "local_price"),
                "BASE_PRICE": ColumnMapping(float, "base_price"),
                "ORIGINAL_FACE": ColumnMapping(float, "original_face_value"),
                "CIL": ColumnMapping(bool, "is_cash_in_lieu"),
                "SETTLEMENT_DATE": ColumnMapping(datetime, "settlement_date", "%Y%m%d"),
                "SUPPLEMENTAL_ID_1": ColumnMapping(int, "supplemental_id_1"),
                "TOTAL_CREATIONS/REDEMPTIONS": ColumnMapping(int, "total_order_count"),
                "NAV": ColumnMapping(float, "net_asset_value"),
                "NAV_PER_CREATION/REDEMPTION": ColumnMapping(float, "nav_per_creation_redemption"),
                "TOTAL_ETF_SHARES": ColumnMapping(int, "total_etf_share_count"),
                "TOTAL_ETF_VALUE": ColumnMapping(float, "total_etf_value"),
                "BASKET_SHARES": ColumnMapping(int, "basket_share_count"),
                "DIVIDEND_CASH": ColumnMapping(float, "dividend_cash_amount"),
                "CASH_COMPONENT": ColumnMapping(float, "cash_component_amount"),
                "CASH_IN_LIEU": ColumnMapping(float, "cash_in_lieu_amount"),
                "TOTAL_DUE": ColumnMapping(float, "total_due_amount"),
                "FACTORABLE": ColumnMapping(bool, "is_factorable"),
                "SETTLE_DATE": ColumnMapping(datetime, "settle_date", "%Y%m%d"),
                "BASE_NET_AMOUNT": ColumnMapping(float, "base_net_amount"),
                "LOCAL_ACCRUED_INTEREST": ColumnMapping(float, "local_accrued_interest"),
                "BASE_ACCRUED_INTEREST": ColumnMapping(float, "base_accrued_interest"),
                "PAR_ADJUSTMENT_FACTOR": ColumnMapping(float, "par_adjustment_factor"),
            }
        )
    ),
    re.compile(r"FPTRAD_report\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="",
                       keep_default_na=False, on_bad_lines="skip"),
        rename=True,
        bronze_table="daily_net_sales",
        silver_mapping=SilverMapping(
            silver_table_name="daily_net_sales",
            table_type="fact",
            primary_keys=("order_id", "trade_date", "ticker"),
            columns={
                "# Order Number": ColumnMapping(datetime, "trade_date", "%m/%d/%Y"),
                " Trade Date": ColumnMapping(str, "ticker"),
                " Ticker": ColumnMapping(str, "side"),
                " Side": ColumnMapping(int, "quantity"),
                " Quantity": ColumnMapping(str, "currency"),
                " Currency": ColumnMapping(datetime, "settle_date", "%m/%d/%Y"),
                " Settlement Date": ColumnMapping(str, "etf_order_type"),
                "__index_level_0__": ColumnMapping(str, "order_id"),
            }
        )
    ),
    # Reference Data (exact match patterns)
    re.compile(r"^securities\.csv$", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="all_securities",
        silver_mapping=SilverMapping(
            silver_table_name="ref_all_securities",
            table_type="reference",
            primary_keys=("figi_id",),
            columns={
                "isin": ColumnMapping(str, "isin_id"),
                "cusip": ColumnMapping(str, "cusip_id"),
                "sedol": ColumnMapping(str, "sedol_id"),
                "figi": ColumnMapping(str, "figi_id"),
                "cntry_issue_iso": ColumnMapping(str, "iso_country_of_issue"),
                "cntry_of_risk": ColumnMapping(str, "iso_country_of_risk")
            }
        )
    ),
    re.compile(r"^exchanges\.csv$", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="all_exchanges_codes",
        silver_mapping=SilverMapping(
            silver_table_name="ref_exchanges",
            table_type="reference",
            primary_keys=("iso_country", "equity_exch_code"),
            columns={
                "iso_country": ColumnMapping(str, "iso_country"),
                "equity_exch_code": ColumnMapping(str, "equity_exch_code"),
            }
        ),

    ),
    # CDS Monthly Participant Reports
    re.compile(r"Harvest Canadian ETF\.xlsx", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_excel, sheet_name="Harvest Canadian ETF"),
        bronze_table="cds_monthly_participant_reports",
        silver_mapping=SilverMapping(
            silver_table_name="cds_monthly_participant_reports",
            table_type="fact",
            primary_keys=("report_date", "isin_id", "cuid_id"),
            columns={
                "Shares": ColumnMapping(int, "quantity_held"),
                "Date": ColumnMapping(datetime, "report_date", "%m/%d/%Y"),
                "CUID": ColumnMapping(str, "cuid_id"),
                "ISIN": ColumnMapping(str, "isin_id"),
            }
        ),
    ),
    re.compile(r"harvest_fund_identifiers\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="harvest_fund_identifiers",
        silver_mapping=SilverMapping(
            silver_table_name="ref_harvest_fund_identifiers",
            table_type="reference",
            primary_keys=("fund_ticker",),
            columns={
                "fund_inception_date": ColumnMapping(datetime, "fund_inception_date", "%Y-%m-%d"),
            }
        ),
    ),
    re.compile(r"history_all_distributions\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="bbg_history_all_distributions",
    ),
    re.compile(r"bbg_history_all_funds_monthly_navs\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="bbg_history_all_funds_monthly_navs",
    ),
    # NBF Wealth Changes - matches Harvest_YYYY-MM-DD.xlsx or Harvest_YYYY_MM_DD.xlsx
    re.compile(r"Harvest_\d{4}[-_]\d{2}[-_]\d{2}\.xlsx", re.IGNORECASE): IngestionMapping(
        parser=src.parsers.extract_nbf_sales_qq,
        bronze_table="nbf_wealth_changes_qq",
    ),
    re.compile(r"BMO_Q\d{1}_\d{4}\.xlsx", re.IGNORECASE): IngestionMapping(
        parser=src.parsers.extract_bmo_sales_qq,
        bronze_table="bmo_wealth_changes_qq",
        silver_mapping=SilverMapping(
            silver_table_name="bmo_wealth_changes_qq",
            table_type="fact",
            primary_keys=("branch",),
        )
    )
}
