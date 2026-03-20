import logging
import pandas as pd
from .base_parser import BaseParser

logger = logging.getLogger(__name__)

class INAVBskt(BaseParser):
    """
    Parser for INAV and BSKT files.
    
    These files are non-tabular vertical reports separated into blocks, where each block 
    represents a single fund. Each block begins with a 'TRADE_DATE' line.
    
    Structure:
    - Primary metadata: Key-value pairs in the first few lines
    - Holdings table: Starts at the header row (contains 'CUSIP', 'TICKER', or 'DESCRIPTION')
        and continues until the next TRADE_DATE or end of file
    
    Context:
    - Files are generated at end of a trading session
    - "Struck NAV" is the official NAV calculated at ~market close in NYC
    - Holdings data represents fund positions at end of day, used for next day's
      creation/redemption activity and next day's iNAV calculations.
    """
    def extract(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        try:
            parsed_rows = self.read_rows()
            if not parsed_rows:
                return []

            fund_blocks = self.split_row_blocks(parsed_rows, start_marker="TRADE_DATE") or ["TRADE_DATE"]
            
            fund_metrics = pd.DataFrame()
            fund_holdings = pd.DataFrame()

            for block in fund_blocks:
                header_idx = self.find_header_idx(block, markers={"CUSIP", "TICKER", "DESCRIPTION"})
                if header_idx == -1:
                    continue
                metadata_df = self._get_metadata(block[:header_idx])
                holdings_df = self.rows_to_dataframe(block[header_idx:]).reset_index(drop=True)
                holdings_df["TRADE_DATE"] = metadata_df["TRADE_DATE"].iloc[0]
                holdings_df["SS_LONG_CODE"] = metadata_df["SS_LONG_CODE"].iloc[0]

                fund_metrics = pd.concat([fund_metrics, metadata_df], ignore_index=True, sort=False)
                fund_holdings = pd.concat([fund_holdings, holdings_df], ignore_index=True, sort=False)

            return fund_metrics, fund_holdings
        except Exception as e:
            logger.error(f"Failed to extract data: {e}")
            return

    def _get_metadata(self, chunk: list[list[str]]) -> pd.DataFrame:
        """Extract metadata from top lines."""
        metadata = {}
        first_row = chunk[0]

        metadata.update({
            "TRADE_DATE": self.row_value(first_row, 1),
            "SS_LONG_CODE": self.row_value(first_row, 2),
            "FULL_NAME": self.row_value(first_row, 4),
            "TICKER_1": self.row_value(first_row, 5),
            "TICKER_2": self.row_value(first_row, 6),
            "BASE_CURRENCY": self.row_value(first_row, 7),
            "INSTRUCTION_ASSET": self.row_value(first_row, 9)
        })
        metadata.update(self._pairs_to_dict(chunk[1:]))
        
        return pd.DataFrame([metadata])
    
    def _pairs_to_dict(self, rows: list[list[str]], step: int = 2) -> dict[str, str]:
        """Flatten rows into key-value dict."""
        flattened = {}
        for row in rows:
            for offset in range(0, len(row) - 1, step):
                key = row[offset]
                if key:
                    flattened[key] = row[offset + 1]
        return flattened
    