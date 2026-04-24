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
        silver_mapping=SilverMapping(
            silver_table_name="pcf_inav_baskets",
            table_type="fact",
            primary_keys=("trade_date", "fund_ticker",
                          "description", "ticker"),
            columns={
                "CUSIP": ColumnMapping(str, "cusip_id"),
                "TICKER": ColumnMapping(str, "ticker"),
                "SEDOL": ColumnMapping(str, "sedol_id"),
                "ISIN": ColumnMapping(str, "isin_id"),
                "SHARES": ColumnMapping(int, "share_count"),
                "ORIGINAL_FACE": ColumnMapping(float, "original_face_value"),
                "INTEREST": ColumnMapping(float, "interest_amount"),
                "LOCAL_PRICE": ColumnMapping(float, "local_price"),
                "LOCAL_MV": ColumnMapping(float, "local_market_value"),
                "FOREX": ColumnMapping(float, "fx_rate"),
                "BASE_PRICE": ColumnMapping(float, "base_price"),
                "BASE_MV": ColumnMapping(float, "base_market_value"),
                "WEIGHT": ColumnMapping("pct", "pct_weight"),
                "CIL": ColumnMapping(bool, "is_cash_in_lieu"),
                "EST_DIVIDEND": ColumnMapping(float, "estimated_dividend_amount"),
                "LOT": ColumnMapping(str, "lot_size"),
                "NEW": ColumnMapping(bool, "is_new_in_basket"),
                "SHARE_CHANGE": ColumnMapping(int, "share_count_delta"),
                "SUPPLEMENTAL_ID_1": ColumnMapping(int, "total_shares_in_fund"),
                "TRADE_DATE": ColumnMapping(datetime, "trade_date", "%Y%m%d"),
                "TICKER_1": ColumnMapping(str, "fund_ticker"),
                "CREATION_UNIT_SIZE": ColumnMapping(int, "creation_unit_size"),
                "ESTIMATED_DIVIDENDS": ColumnMapping(float, "estimated_dividend_amount"),
                "ESTIMATED_EXPENSE": ColumnMapping(float, "estimated_expense_amount"),
                "ESTIMATED_CASH_COMPONENT": ColumnMapping(float, "estimated_cash_component_amount"),
                "NAV": ColumnMapping(float, "nav_per_unit"),
                "UNDISTRIBUTED_NET_INCOME_PER_SHARE": ColumnMapping(float, "undistributed_net_income_per_share"),
                "BASKET_MARKET_VALUE": ColumnMapping(float, "basket_market_value"),
                "ACTUAL_CASH_COMPONENT": ColumnMapping(float, "actual_cash_component_amount"),
                "NAV_PER_CREATION_UNIT": ColumnMapping(float, "nav_per_creation_unit"),
                "UNDISTRIBUTED_NET_INCOME_PER_CREATION_UNIT": ColumnMapping(float, "undistributed_net_income_per_creation_unit"),
                "BASKET_SHARES": ColumnMapping(int, "share_count"),
                "NAV_LESS_UNDISTRIBUTED_NET_INCOME": ColumnMapping(float, "nav_less_undistributed_net_income"),
                "ACTUAL_CASH_IN_LIEU": ColumnMapping(float, "actual_cash_in_lieu_amount"),
                "ESTIMATED_CASH_IN_LIEU": ColumnMapping(float, "estimated_cash_in_lieu_amount"),
                "ETF_SHARES_OUTSTANDING": ColumnMapping(int, "etf_units_outstanding"),
                "EXPENSE_RATIO": ColumnMapping(float, "fund_expense_ratio"),
                "TOTAL_NET_ASSETS": ColumnMapping(float, "fund_nav"),
                "ACTUAL_TOTAL_CASH": ColumnMapping(float, "actual_total_cash_amount"),
                "ESTIMATED_TOTAL_CASH": ColumnMapping(float, "estimated_total_cash_amount"),
            }
        ),
    ),
    re.compile(r"Harvest_BSKT_ALL\.\d{8}\.csv", re.IGNORECASE): IngestionMapping(
        parser=src.parsers.extract_pcf,
        bronze_table="pcf_creation_baskets",
        silver_mapping=SilverMapping(
            silver_table_name="pcf_creation_baskets",
            table_type="fact",
            primary_keys=("trade_date", "fund_ticker",
                          "description", "ticker"),
            columns={
                "CUSIP": ColumnMapping(str, "cusip_id"),
                "TICKER": ColumnMapping(str, "ticker"),
                "SEDOL": ColumnMapping(str, "sedol_id"),
                "ISIN": ColumnMapping(str, "isin_id"),
                "SHARES": ColumnMapping(int, "share_count"),
                "ORIGINAL_FACE": ColumnMapping(float, "original_face_value"),
                "INTEREST": ColumnMapping(float, "interest_amount"),
                "LOCAL_PRICE": ColumnMapping(float, "local_price"),
                "LOCAL_MV": ColumnMapping(float, "local_market_value"),
                "FOREX": ColumnMapping(float, "fx_rate"),
                "BASE_PRICE": ColumnMapping(float, "base_price"),
                "BASE_MV": ColumnMapping(float, "base_market_value"),
                "WEIGHT": ColumnMapping("pct", "pct_weight"),
                "CIL": ColumnMapping(bool, "is_cash_in_lieu"),
                "EST_DIVIDEND": ColumnMapping(float, "estimated_dividend_amount"),
                "LOT": ColumnMapping(str, "lot_size"),
                "NEW": ColumnMapping(bool, "is_new_in_basket"),
                "SHARE_CHANGE": ColumnMapping(int, "share_count_delta"),
                "SUPPLEMENTAL_ID_1": ColumnMapping(int, "total_shares_in_fund"),
                "TRADE_DATE": ColumnMapping(datetime, "trade_date", "%Y%m%d"),
                "TICKER_1": ColumnMapping(str, "fund_ticker"),
                "CREATION_UNIT_SIZE": ColumnMapping(int, "creation_unit_size"),
                "ESTIMATED_DIVIDENDS": ColumnMapping(float, "estimated_dividend_amount"),
                "ESTIMATED_EXPENSE": ColumnMapping(float, "estimated_expense_amount"),
                "ESTIMATED_CASH_COMPONENT": ColumnMapping(float, "estimated_cash_component_amount"),
                "NAV": ColumnMapping(float, "nav_per_unit"),
                "UNDISTRIBUTED_NET_INCOME_PER_SHARE": ColumnMapping(float, "undistributed_net_income_per_share"),
                "BASKET_MARKET_VALUE": ColumnMapping(float, "basket_market_value"),
                "ACTUAL_CASH_COMPONENT": ColumnMapping(float, "actual_cash_component_amount"),
                "NAV_PER_CREATION_UNIT": ColumnMapping(float, "nav_per_creation_unit"),
                "UNDISTRIBUTED_NET_INCOME_PER_CREATION_UNIT": ColumnMapping(float, "undistributed_net_income_per_creation_unit"),
                "BASKET_SHARES": ColumnMapping(int, "share_count"),
                "NAV_LESS_UNDISTRIBUTED_NET_INCOME": ColumnMapping(float, "nav_less_undistributed_net_income"),
                "ACTUAL_CASH_IN_LIEU": ColumnMapping(float, "actual_cash_in_lieu_amount"),
                "ESTIMATED_CASH_IN_LIEU": ColumnMapping(float, "estimated_cash_in_lieu_amount"),
                "ETF_SHARES_OUTSTANDING": ColumnMapping(int, "etf_units_outstanding"),
                "EXPENSE_RATIO": ColumnMapping(float, "fund_expense_ratio"),
                "TOTAL_NET_ASSETS": ColumnMapping(float, "fund_nav"),
                "ACTUAL_TOTAL_CASH": ColumnMapping(float, "actual_total_cash_amount"),
                "ESTIMATED_TOTAL_CASH": ColumnMapping(float, "estimated_total_cash_amount"),
            }
        ),
    ),
    # Position Files
    re.compile(r"All_Positions\d{8}\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        bronze_table="all_positions",
        silver_mapping=SilverMapping(
            silver_table_name="all_positions",
            table_type="fact",
            primary_keys=("source_file", "ss_id_cust", "ticker"),
            columns={
                "FundID": ColumnMapping(str, "ss_id_cust"),
                "TICKER": ColumnMapping(str, "ticker"),
                "CUSIP": ColumnMapping(str, "cusip_id"),
                "ISIN": ColumnMapping(str, "isin_id"),
                "SEDOL": ColumnMapping(str, "sedol_id"),
                "BASKET_SHARES": ColumnMapping(int, "share_count"),
                "COUNTRY_RESTRICTION": ColumnMapping(bool, "is_restricted"),
                "SUPPLEMENTAL_ID_1": ColumnMapping(int, "total_shares_in_fund"),
                "SUPPLEMENTAL_ID_2": ColumnMapping(str, "bbg_id"),
                "RIC": ColumnMapping(str, "reuters_id"),
                "BBG": ColumnMapping(str, "bbg_ticker"),
            }
        ),
    ),
    re.compile(r"PLF_Positions\d{8}\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        bronze_table="plf_positions",
        silver_mapping=SilverMapping(
            silver_table_name="plf_positions",
            table_type="fact",
            primary_keys=("source_file", "ss_id_cust", "ticker"),
            columns={
                "FundID": ColumnMapping(str, "ss_id_cust"),
                "TICKER": ColumnMapping(str, "ticker"),
                "CUSIP": ColumnMapping(str, "cusip_id"),
                "ISIN": ColumnMapping(str, "isin_id"),
                "SEDOL": ColumnMapping(str, "sedol_id"),
                "BASKET_SHARES": ColumnMapping(int, "share_count"),
                "COUNTRY_RESTRICTION": ColumnMapping(bool, "is_restricted"),
                "SUPPLEMENTAL_ID_1": ColumnMapping(int, "total_shares_in_fund"),
                "SUPPLEMENTAL_ID_2": ColumnMapping(str, "bbg_id"),
                "RIC": ColumnMapping(str, "reuters_id"),
                "BBG": ColumnMapping(str, "bbg_ticker"),
            }
        ),
    ),
    re.compile(r"Harvest_CIL_ALL\.\d{8}\.(csv|txt)", re.IGNORECASE): IngestionMapping(
        parser=src.parsers.extract_cil,
        bronze_table="cash_in_lieu_records",
        silver_mapping=SilverMapping(
            silver_table_name="cash_in_lieu_records",
            table_type="fact",
            primary_keys=("record_date", "fund_ticker", "description"),
            columns={
                "BASKET_CODE": ColumnMapping(str, "ss_id_class"),
                "BASKET_TICKER": ColumnMapping(str, "fund_ticker"),
                "CUSIP": ColumnMapping(str, "cusip_id"),
                "TICKER": ColumnMapping(str, "ticker"),
                "SEDOL": ColumnMapping(str, "sedol_id"),
                "ISIN": ColumnMapping(str, "isin_id"),
                "SHARES": ColumnMapping(int, "share_count"),
                "LOCAL_PRICE": ColumnMapping(float, "local_price"),
                "FOREX": ColumnMapping(float, "fx_rate"),
                "BASE_PRICE": ColumnMapping(float, "base_price"),
                "BASE_INTEREST": ColumnMapping(float, "base_interest_amount"),
                "BASE_MV": ColumnMapping(float, "base_market_value"),
                "INT_FACTOR": ColumnMapping(float, "interest_factor"),
                "PAR_ADJUSTMENT_FACTOR": ColumnMapping(float, "par_adjustment_factor"),
                "DATE": ColumnMapping(datetime, "record_date", "%Y%m%d"),
            }
        ),
    ),
    re.compile(r"Harvest_INKIND\.\d{8}\.txt", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="",
                       keep_default_na=False, skiprows=1),
        bronze_table="inkind_orders",
        silver_mapping=SilverMapping(
            silver_table_name="inkind_orders",
            table_type="fact",
            primary_keys=("trade_date", "ss_id", "description", "ticker"),
            columns={
                "FUND": ColumnMapping(str, "ss_id"),
                "CUSIP": ColumnMapping(str, "cusip_id"),
                "ISIN": ColumnMapping(str, "isin_id"),
                "SEDOL": ColumnMapping(str, "sedol_id"),
                "TICKER": ColumnMapping(str, "ticker"),
                "SHARES": ColumnMapping(int, "share_count"),
                "FOREX": ColumnMapping(float, "fx_rate"),
                "LOCAL_PRICE": ColumnMapping(float, "local_price"),
                "LOCAL_NET_AMOUNT": ColumnMapping(float, "local_net_amount"),
                "BASE_PRICE": ColumnMapping(float, "base_price"),
                "BASE_NET_AMOUNT": ColumnMapping(float, "base_net_amount"),
                "LOCAL_ACCRUED_INTEREST": ColumnMapping(float, "local_accrued_interest"),
                "BASE_ACCRUED_INTEREST": ColumnMapping(float, "base_accrued_interest"),
                "ORIGINAL_FACE": ColumnMapping(float, "original_face_value"),
                "PAR_ADJUSTMENT_FACTOR": ColumnMapping(float, "par_adjustment_factor"),
                "CIL": ColumnMapping(bool, "is_cash_in_lieu"),
                "TRADE_DATE": ColumnMapping(datetime, "trade_date", "%Y%m%d"),
                "SETTLEMENT_DATE": ColumnMapping(datetime, "settlement_date", "%Y%m%d"),
                "CIL_FEE": ColumnMapping(float, "cash_in_lieu_fee"),
                "ORDER_NUMBER": ColumnMapping(str, "order_number"),
                "FACTORABLE": ColumnMapping(bool, "is_factorable"),
                "SUPPLEMENTAL_ID_1": ColumnMapping(int, "total_shares_in_fund"),
                "SUPPLEMENTAL_ID_2": ColumnMapping(str, "bbg_id"),
            }
        ),
    ),
    re.compile(r"Harvest_NAV_ALL\.\d{8}\.csv", re.IGNORECASE): IngestionMapping(
        parser=src.parsers.extract_cil,
        bronze_table="pcf_nav_records",
        silver_mapping=SilverMapping(
            silver_table_name="pcf_nav_records",
            table_type="fact",
            primary_keys=("record_date", "fund_ticker"),
            columns={
                "BASKET_CODE": ColumnMapping(str, "ss_id_class"),
                "CUSIP": ColumnMapping(str, "cusip_id"),
                "TICKER": ColumnMapping(str, "fund_ticker"),
                "NAV_PER_SHR": ColumnMapping(float, "nav_per_unit"),
                "NAV_PER_CU": ColumnMapping(float, "nav_per_creation_unit"),
                "INCOME_PER_CU": ColumnMapping(float, "income_per_creation_unit"),
                "BALANCING_CASH": ColumnMapping(float, "balancing_cash_amount"),
                "ACTUAL_TOTAL_CASH": ColumnMapping(float, "actual_total_cash_amount"),
                "SHRS_OUTSTANDING": ColumnMapping(int, "etf_units_outstanding"),
                "TOTAL_NET_ASSETS": ColumnMapping(float, "fund_nav"),
                "INCOME_DIST": ColumnMapping(float, "fund_dist_amount"),
                "ST_CAP_DIST": ColumnMapping(float, "short_term_cap_dist"),
                "LT_CAP_DIST": ColumnMapping(float, "long_term_cap_dist"),
                "ESTIMATED_TOTAL_CASH": ColumnMapping(float, "estimated_total_cash_amount"),
                "ESTIMATED_CIL": ColumnMapping(float, "estimated_cash_in_lieu_amount"),
                "ESTIMATED_INTEREST": ColumnMapping(float, "estimated_interest_amount"),
                "ACTUAL_CIL": ColumnMapping(float, "actual_cash_in_lieu_amount"),
                "ACTUAL_INTEREST": ColumnMapping(float, "actual_interest_amount"),
                "ESTIMATED_CASH_DIV": ColumnMapping(float, "estimated_cash_dividend_amount"),
                "ESTIMATED_EXPENSE": ColumnMapping(float, "estimated_expense_amount"),
                "ESTIMATED_BKT_MKT": ColumnMapping(float, "estimated_basket_market_value"),
                "ACTUAL_BKT_MKT": ColumnMapping(float, "actual_basket_market_value"),
                "ACTUAL_CASH": ColumnMapping(float, "actual_cash_amount"),
                "ESTIMATED_CASH": ColumnMapping(float, "estimated_cash_amount"),
                "TD_SHRS_OUTSTANDING": ColumnMapping(int, "trade_date_etf_units_outstanding"),
                "TD_TOTAL_NET_ASSETS": ColumnMapping(float, "trade_date_fund_nav"),
                "DATE": ColumnMapping(datetime, "record_date", "%Y%m%d"),
            }
        ),
    ),
    re.compile(r"Harvest_Preburst_INKIND_ALL\.\d{8}\.txt", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="",
                       keep_default_na=False, skiprows=1),
        bronze_table="preburst_inkind_orders",
        silver_mapping=SilverMapping(
            silver_table_name="preburst_inkind_orders",
            table_type="fact",
            primary_keys=("trade_date", "ss_id", "description", "ticker"),
            columns={
                "FUND": ColumnMapping(str, "ss_id"),
                "CUSIP": ColumnMapping(str, "cusip_id"),
                "ISIN": ColumnMapping(str, "isin_id"),
                "SEDOL": ColumnMapping(str, "sedol_id"),
                "TICKER": ColumnMapping(str, "ticker"),
                "SHARES": ColumnMapping(int, "share_count"),
                "FOREX": ColumnMapping(float, "fx_rate"),
                "LOCAL_PRICE": ColumnMapping(float, "local_price"),
                "LOCAL_NET_AMOUNT": ColumnMapping(float, "local_net_amount"),
                "BASE_PRICE": ColumnMapping(float, "base_price"),
                "BASE_NET_AMOUNT": ColumnMapping(float, "base_net_amount"),
                "LOCAL_ACCRUED_INTEREST": ColumnMapping(float, "local_accrued_interest"),
                "BASE_ACCRUED_INTEREST": ColumnMapping(float, "base_accrued_interest"),
                "ORIGINAL_FACE": ColumnMapping(float, "original_face_value"),
                "PAR_ADJUSTMENT_FACTOR": ColumnMapping(float, "par_adjustment_factor"),
                "CIL": ColumnMapping(bool, "is_cash_in_lieu"),
                "TRADE_DATE": ColumnMapping(datetime, "trade_date", "%Y%m%d"),
                "SETTLEMENT_DATE": ColumnMapping(datetime, "settlement_date", "%Y%m%d"),
                "CIL_FEE": ColumnMapping(float, "cash_in_lieu_fee"),
                "ORDER_NUMBER": ColumnMapping(str, "order_number"),
                "FACTORABLE": ColumnMapping(bool, "is_factorable"),
                "SUPPLEMENTAL_ID_1": ColumnMapping(int, "total_shares_in_fund"),
                "SUPPLEMENTAL_ID_2": ColumnMapping(str, "bbg_id"),
            }
        ),
    ),
    re.compile(r"Harvest Price File -\d{8}\.(xls|xlsx)", re.IGNORECASE): IngestionMapping(
        parser=src.parsers.extract_accounting_navs,
        bronze_table="accounting_nav_records",
        silver_mapping=SilverMapping(
            silver_table_name="accounting_nav_records",
            table_type="fact",
            primary_keys=("record_date", "ss_id", "fund_name"),
            columns={
                "Fund ID": ColumnMapping(str, "ss_id"),
                "Date:": ColumnMapping(datetime, "record_date", "%m/%d/%Y"),
                "Vendor Fund Name": ColumnMapping(str, "fund_name"),
                "NAV": ColumnMapping(float, "nav_per_unit"),
                "Prior NAV": ColumnMapping(float, "previous_nav_per_unit"),
                "NAV Change": ColumnMapping(float, "nav_delta"),
                "NAV Pct Change": ColumnMapping(float, "nav_delta_pct"),
                "Capital Stock Shares Outstanding": ColumnMapping(int, "etf_units_outstanding"),
                "Total Net Assets": ColumnMapping(float, "fund_nav"),
                "Prior Total Net Assets": ColumnMapping(float, "previous_fund_nav"),
                "Total Net Assets Change (Current TNA-Prior TNA)": ColumnMapping(float, "fund_nav_delta"),
                "Periodic Income Div Rate": ColumnMapping(float, "previous_fund_dist_amount"),
                "Cap Gains Distribution Factor (Net Rate)": ColumnMapping(float, "cap_gains_dist_factor"),
                "FX Rate USD/CAD": ColumnMapping(float, "fx_rate_usd_cad"),
                "FX Rate CAD/USD": ColumnMapping(float, "fx_rate_cad_usd"),
            }
        ),
    ),
    re.compile(r"Accounting_Cash_Statement\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        load_type="overwrite",
        bronze_table="accounting_cash_statements",
        silver_mapping=SilverMapping(
            silver_table_name="accounting_cash_statements",
            table_type="fact",
            primary_keys=("record_date", "ss_id",
                          "cash_post_description", "cusip_id"),
            columns={
                "Amount Received": ColumnMapping(float, "received_amount"),
                "Disbursed Amount": ColumnMapping(float, "disbursed_amount"),
                "Fund": ColumnMapping(str, "ss_id"),
                "Cash Post Type Description": ColumnMapping(str, "cash_post_description"),
                "Report Date": ColumnMapping(datetime, "record_date", "%m/%d/%Y"),
                "Post Date": ColumnMapping(datetime, "post_date", "%m/%d/%Y"),
                "CUSIP Number": ColumnMapping(str, "cusip_id"),
                "Shares/Par Value": ColumnMapping(float, "shares_par_value"),
                "Report Date Starting Balance": ColumnMapping(float, "starting_balance"),
                "Ending Ledger Balance": ColumnMapping(float, "ending_balance"),
            }
        ),
    ),
    re.compile(r"All_Corporate_Actions\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        load_type="overwrite",
        bronze_table="all_corporate_actions",
        silver_mapping=SilverMapping(
            silver_table_name="all_corporate_actions",
            table_type="fact",
            primary_keys=("notification_date", "ss_id", "event_id"),
            columns={
                "Fund Number": ColumnMapping(str, "ss_id"),
                "Event ID": ColumnMapping(str, "event_id"),
                "CUSIP": ColumnMapping(str, "cusip_id"),
                "SEDOL (Underlying)": ColumnMapping(str, "underlying_sedol_id"),
                "ISIN (Underlying)": ColumnMapping(str, "underlying_isin_id"),
                "Notification Delivery Date": ColumnMapping(datetime, "notification_date", "%Y-%m-%d"),
                "Next Deadline Date": ColumnMapping(datetime, "next_deadline_date", "%Y-%m-%d"),
                "Ex Date": ColumnMapping(datetime, "ex_date", "%Y-%m-%d"),
                "Record Date": ColumnMapping(datetime, "action_record_date", "%Y-%m-%d"),
                "Entitled Position": ColumnMapping(int, "entitled_position"),
                "Response Receipt Date": ColumnMapping(datetime, "response_receipt_date", "%Y-%m-%d"),
                "Response Shares": ColumnMapping(int, "response_share_count"),
            }
        ),
    ),
    re.compile(r"Cash_Forecast_Transactions\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        load_type="overwrite",
        bronze_table="cash_forecast_transactions",
        silver_mapping=SilverMapping(
            silver_table_name="cash_forecast_transactions",
            table_type="fact",
            primary_keys=("record_datetime", "ss_id", "isin_id"),
            columns={
                "Mainframe Time Stamp": ColumnMapping(datetime, "record_datetime", "%d %b %Y %H:%M:%S"),
                "Fund": ColumnMapping(str, "ss_id"),
                "Isin": ColumnMapping(str, "isin_id"),
                "Sedol": ColumnMapping(str, "sedol_id"),
                "Share Quantity": ColumnMapping(int, "share_count"),
                "Net Amount": ColumnMapping(float, "net_amount"),
                "Trade Date": ColumnMapping(datetime, "trade_date", "%d %b %Y"),
                "Pay/Settle Date": ColumnMapping(datetime, "settle_date", "%d %b %Y"),
            }
        ),
    ),
    re.compile(r"Custody_Positions\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="custody_positions",
        silver_mapping=SilverMapping(
            silver_table_name="custody_positions",
            table_type="fact",
            primary_keys=("ingested_at", "ss_id", "security_name"),
            columns={
                "Fund": ColumnMapping(str, "ss_id"),
                "Security Name": ColumnMapping(str, "security_name"),
                "Sedol": ColumnMapping(str, "sedol_id"),
                "Isin": ColumnMapping(str, "isin_id"),
                "Traded": ColumnMapping(int, "shares_traded"),
                "Available": ColumnMapping(int, "shares_available"),
                "Security Asset Class": ColumnMapping(int, "asset_class_id"),
                "GL Asset Class Name": ColumnMapping(str, "asset_class_name"),
                "Isin": ColumnMapping(str, "isin_id"),
                "Maturity Date": ColumnMapping(datetime, "maturity_date", "%d %b %Y"),
                "On loan": ColumnMapping(int, "shares_on_loan"),
            }
        ),
    ),
    re.compile(r"Custody_Transactions\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        load_type="overwrite",
        bronze_table="custody_transactions",
        silver_mapping=SilverMapping(
            silver_table_name="custody_transactions",
            table_type="fact",
            primary_keys=("record_datetime", "ss_id", "ss_trade_id"),
            columns={
                "Fund": ColumnMapping(str, "ss_id"),
                "Share Quantity": ColumnMapping(int, "share_count"),
                "Net Amount": ColumnMapping(float, "net_amount"),
                "Actual Net Amount": ColumnMapping(float, "actual_net_amount"),
                "Interest": ColumnMapping(float, "interest_amount"),
                "Principal": ColumnMapping(float, "principal_amount"),
                "Actual Pay/Settle Date": ColumnMapping(datetime, "actual_settle_date", "%d %b %Y"),
                "Pay/Settle Date": ColumnMapping(datetime, "settle_date", "%d %b %Y"),
                "Trade/Record Date": ColumnMapping(datetime, "trade_date", "%d %b %Y"),
                "Mainframe Time Stamp": ColumnMapping(datetime, "record_datetime", "%d %b %Y %H:%M:%S"),
                "Sedol": ColumnMapping(str, "sedol_id"),
                "Isin": ColumnMapping(str, "isin_id"),
            }
        ),
    ),
    re.compile(r"Daily_Model_Holdings\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="daily_model_holdings",
        silver_mapping=SilverMapping(
            silver_table_name="daily_model_holdings",
            table_type="fact",
            primary_keys=("record_date", "ss_id", "security_name"),
            columns={
                "Period End Date": ColumnMapping(datetime, "record_date", "%m/%d/%Y"),
                "Fund": ColumnMapping(str, "ss_id"),
                "Security Name": ColumnMapping(str, "security_name"),
                "Bloomberg Ticker": ColumnMapping(str, "bbg_ticker"),
                "Bloomberg Expanded Ticker": ColumnMapping(str, "bbg_expanded_ticker"),
                "ISIN Number": ColumnMapping(str, "isin_id"),
                "CUSIP Number": ColumnMapping(str, "cusip_id"),
                "SEDOL Number": ColumnMapping(str, "sedol_id"),
                "Underlying ISIN": ColumnMapping(str, "underlying_isin_id"),
                "Underlying Cusip Number": ColumnMapping(str, "underlying_cusip_id"),
                "Underlying SEDOL": ColumnMapping(str, "underlying_sedol_id"),
                "Expiration Date": ColumnMapping(datetime, "expiration_date", "%m/%d/%Y"),
                "Local Strike Price": ColumnMapping(float, "local_strike_price"),
                "Original Strike Price": ColumnMapping(float, "original_strike_price"),
                "Multiplier": ColumnMapping(float, "multiplier"),
                "Original Multiplier": ColumnMapping(float, "original_multiplier"),
                "Shares/Par Value": ColumnMapping(float, "shares_par_value"),
                "Market Price": ColumnMapping(float, "market_price"),
                "Local Unit Cost": ColumnMapping(float, "local_unit_cost"),
                "Local Average Cost": ColumnMapping(float, "local_average_cost"),
                "Local Market Value": ColumnMapping(float, "local_market_value"),
                "Exchange Rate": ColumnMapping(float, "exchange_rate"),
                "Base Average Cost": ColumnMapping(float, "base_average_cost"),
                "Base Market Value": ColumnMapping(float, "base_market_value"),
                "Base Price Amount": ColumnMapping(float, "base_price_amount"),
                "Base Unit Cost": ColumnMapping(float, "base_unit_cost"),
                "Date Last Priced": ColumnMapping(datetime, "date_last_priced", "%m/%d/%Y"),
                "Interest Rate": ColumnMapping(float, "interest_rate"),
                "Maturity Date": ColumnMapping(datetime, "maturity_date", "%m/%d/%Y"),
            }
        ),
    ),
    re.compile(r"Daily_Net_Asset_Values\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="daily_net_asset_values",
        silver_mapping=SilverMapping(
            silver_table_name="daily_net_asset_values",
            table_type="fact",
            primary_keys=("record_date", "ss_id", "fund_class"),
            columns={
                "Price Date": ColumnMapping(datetime, "record_date", "%m/%d/%Y"),
                "Fund": ColumnMapping(str, "ss_id"),
                "Class Name": ColumnMapping(str, "fund_class"),
                "Class Ratio": ColumnMapping(float, "class_ratio"),
                "Net Asset Value Per Share": ColumnMapping(float, "nav_per_share"),
                "Shares/Units Outstanding": ColumnMapping(int, "shares_outstanding"),
                "Total Net Asset Value": ColumnMapping(float, "total_net_asset_value"),
                "$ Change NAV": ColumnMapping(float, "nav_delta"),
            }
        ),
    ),
    re.compile(r"Distribution_Liability\.CSV", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="distribution_liabilities",
        silver_mapping=SilverMapping(
            silver_table_name="distribution_liabilities",
            table_type="fact",
            primary_keys=("record_date", "ss_id", "gl_account_number"),
            columns={
                "Period End Date": ColumnMapping(datetime, "record_date", "%m/%d/%Y"),
                "Fund": ColumnMapping(str, "ss_id"),
                "GL Account Number": ColumnMapping(str, "gl_account_number"),
                "TB Ending Balance": ColumnMapping(float, "ending_balance"),
            }
        ),
    ),
    re.compile(r"Loan_Balances\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="loan_balances",
        silver_mapping=SilverMapping(
            silver_table_name="loan_balances",
            table_type="fact",
            primary_keys=("record_date", "ss_id", "gl_account_number"),
            columns={
                "Period End Date": ColumnMapping(datetime, "record_date", "%m/%d/%Y"),
                "Fund": ColumnMapping(str, "ss_id"),
                "GL Account Number": ColumnMapping(str, "gl_account_number"),
                "GL Account Starting Balance": ColumnMapping(float, "starting_balance"),
                "GL Account Net Activity": ColumnMapping(float, "net_activity"),
                "GL Account Ending Balance": ColumnMapping(float, "ending_balance"),
            }
        ),
    ),
    re.compile(r"Opening_Cash_Balances\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="opening_cash_balances",
        silver_mapping=SilverMapping(
            silver_table_name="opening_cash_balances",
            table_type="fact",
            primary_keys=("record_date", "ss_id"),
            columns={
                "As of Date": ColumnMapping(datetime, "record_date", "%d %b %Y"),
                "Fund": ColumnMapping(str, "ss_id"),
                "Balance": ColumnMapping(float, "opening_cash_balance"),
            }
        ),
    ),
    re.compile(r"Pending_FX_Accounting\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="pending_fx_accounting_records",
        silver_mapping=SilverMapping(
            silver_table_name="pending_fx_accounting_records",
            table_type="fact",
            primary_keys=("record_date", "ss_id"),
            columns={
                "Period End Date": ColumnMapping(datetime, "record_date", "%m/%d/%Y"),
                "Trade Date": ColumnMapping(datetime, "trade_date", "%m/%d/%Y"),
                "Contractual Settlement Date": ColumnMapping(datetime, "contractual_settlement_date", "%m/%d/%Y"),
                "Client Fund": ColumnMapping(str, "ss_id"),
                "Exchange Rate": ColumnMapping(float, "exchange_rate"),
                "Amount Bought": ColumnMapping(float, "bought_amount"),
                "Amount Sold": ColumnMapping(float, "sold_amount"),
                "Days Past Due": ColumnMapping(int, "days_past_due"),
                "Base Current Value": ColumnMapping(float, "base_current_value"),
                "Unrealized Gain/Loss": ColumnMapping(float, "unrealized_gain_loss"),
            }
        ),
    ),
    re.compile(r"Top10_FX_Pending\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="top10_fx_pending_records",
        silver_mapping=SilverMapping(
            silver_table_name="top10_fx_pending_records",
            table_type="fact",
            primary_keys=("record_date", "ss_id"),
            columns={
                "Accounting Period End Date": ColumnMapping(datetime, "record_date", "%m/%d/%Y"),
                "Trade Date": ColumnMapping(datetime, "trade_date", "%m/%d/%Y"),
                "Contractual Settlement Date": ColumnMapping(datetime, "contractual_settlement_date", "%m/%d/%Y"),
                "Client Fund": ColumnMapping(str, "ss_id"),
                "Exchange Rate": ColumnMapping(float, "exchange_rate"),
                "Amount Bought": ColumnMapping(float, "bought_amount"),
                "Amount Sold": ColumnMapping(float, "sold_amount"),
                "Days Past Due": ColumnMapping(int, "days_past_due"),
                "Base Current Value": ColumnMapping(float, "base_current_value"),
                "Unrealized Gain/Loss": ColumnMapping(float, "unrealized_gain_loss"),
            }
        )
    ),
    re.compile(r"Top10_Net_Asset_Value\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="top10_net_asset_values",
        silver_mapping=SilverMapping(
            silver_table_name="top10_net_asset_values",
            table_type="fact",
            primary_keys=("record_date", "ss_id", "dual_pricing_basis"),
            columns={
                "Price Date": ColumnMapping(datetime, "record_date", "%m/%d/%Y"),
                "Fund": ColumnMapping(str, "ss_id"),
                "Dual Pricing Basis": ColumnMapping(str, "dual_pricing_basis"),
                "Net Asset Value Per Share": ColumnMapping(float, "nav_per_share"),
                "Shares/Units Outstanding": ColumnMapping(int, "shares_outstanding"),
                "Total Net Asset Value": ColumnMapping(float, "total_net_asset_value"),
            }
        )
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
                "SUPPLEMENTAL_ID_1": ColumnMapping(int, "total_shares_in_fund"),
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
                "dda_id": ColumnMapping(int, "dda_id"),
                "fund_inception_date": ColumnMapping(datetime, "fund_inception_date", "%Y-%m-%d"),
            }
        ),
    ),
    re.compile(r"history_all_distributions\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        rename=True,
        bronze_table="bbg_history_all_distributions",
        silver_mapping=SilverMapping(
            silver_table_name="bbg_history_all_distributions",
            table_type="fact",
            primary_keys=("ex_date", "ticker"),
            columns={
                "Declared Date": ColumnMapping(datetime, "declared_date", "%Y-%m-%d"),
                "Ex-Date": ColumnMapping(datetime, "ex_date", "%Y-%m-%d"),
                "Record Date": ColumnMapping(datetime, "record_date", "%Y-%m-%d"),
                "Payable Date": ColumnMapping(datetime, "payment_date", "%Y-%m-%d"),
                "Dividend Amount": ColumnMapping(float, "distribution_amount"),
            }
        ),
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
    ),
    re.compile(r"Branch_Mapping\.csv", re.IGNORECASE): IngestionMapping(
        parser=partial(pd.read_csv, na_values="", keep_default_na=False),
        bronze_table="branch_mappings",
        silver_mapping=SilverMapping(
            silver_table_name="ref_branch_mappings",
            table_type="reference",
            primary_keys=("broker", "branch_name"),
            columns={
                "Broker": ColumnMapping(str, "broker"),
                "Branch": ColumnMapping(str, "branch_name"),
                "Latitude": ColumnMapping(float, "latitude"),
                "Longitude": ColumnMapping(float, "longitude"),
            }
        ),
    ),
}
