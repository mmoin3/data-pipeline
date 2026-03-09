import os
import sys
import logging
from pathlib import Path
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, inspect

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from config import RAW_DATA_DIR, DB_PATH, DB_CONN_STR
from src.parsers.inav_bskt import INAVBskt

logger = logging.getLogger(__name__)


class Extractor:
    """Orchestrate raw data extraction and loading into bronze layer."""
    
    def __init__(self):
        self.raw_dir = Path(RAW_DATA_DIR)
        self.engine = create_engine(DB_CONN_STR)
        self._init_schema()
    
    def _init_schema(self):
        """Initialize bronze layer tables with generic structure."""
        with self.engine.connect() as conn:
            # Fund metrics bronze table - raw untyped data
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bronze_fund_metrics (
                    _file_name TEXT,
                    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Fund holdings bronze table - raw untyped data
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bronze_fund_holdings (
                    _file_name TEXT,
                    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
    
    def extract(self):
        """Scan raw folder and extract files."""
        files = sorted(self.raw_dir.glob("*.csv")) + sorted(self.raw_dir.glob("*.txt"))
        
        if not files:
            logger.info("No files in raw folder")
            return
        
        logger.info(f"Found {len(files)} files to process")
        
        for file in files:
            logger.info(f"Processing: {file.name}")
            self._process_file(file)
        
        logger.info("Extraction complete")
    
    def _process_file(self, file_path):
        """Route file to appropriate parser based on filename."""
        name = file_path.name
        
        # INAV Basket files
        if "INAVBSKT" in name:
            self._load_inav(file_path)
        elif "NAV_ALL" in name:
            logger.info(f"Skipped (NAV parser not implemented): {name}")
        elif "BSKT" in name:
            logger.info(f"Skipped (BSKT parser not implemented): {name}")
        elif "INKIND" in name:
            logger.info(f"Skipped (INKIND parser not implemented): {name}")
        elif "UCF" in name:
            logger.info(f"Skipped (UCF parser not implemented): {name}")
        elif "CIL" in name:
            logger.info(f"Skipped (CIL parser not implemented): {name}")
        # Add more file type mappings as parsers are built
    
    def _load_inav(self, file_path):
        """Extract INAV data and load to bronze."""
        try:
            parser = INAVBskt(str(file_path))
            metrics_df, holdings_df = parser.extract()
            
            if metrics_df is not None and not metrics_df.empty:
                metrics_df["_file_name"] = file_path.name
                metrics_df.to_sql("bronze_fund_metrics", self.engine, if_exists="append", index=False)
                logger.info(f"Loaded {len(metrics_df)} metrics to bronze")
            
            if holdings_df is not None and not holdings_df.empty:
                holdings_df["_file_name"] = file_path.name
                holdings_df.to_sql("bronze_fund_holdings", self.engine, if_exists="append", index=False)
                logger.info(f"Loaded {len(holdings_df)} holdings to bronze")
        
        except Exception as e:
            logger.error(f"Failed to load {file_path.name}: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extractor = Extractor()
    extractor.extract()
