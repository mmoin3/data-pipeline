import pandas as pd
from .base_parser import BaseParser

class INAVBskt(BaseParser):
    """Readable, minimal parser for BSKT-style files (single fund blocks).

    Each block starts with a line beginning with 'TRADE_DATE'. Metadata
    occupies the first few lines of the block (pairs of key,value fields).
    The holdings table begins on the line that contains the header (usually
    includes 'CUSIP' or 'TICKER') and continues until the next TRADE_DATE
    or end of file.
    """
    def extract(self) -> list[dict[str, pd.DataFrame]]:
        """
        Return a list of fund dicts: {'metadata': {...}, 'holdings': DataFrame}.
        Accepts a list of lines (from CSV, Excel, etc.).
        If lines is None, reads from self.path as text lines (default CSV/txt).
        """
        try:
            parsed_rows = self.read_rows()
            if not parsed_rows:
                return []

            fund_blocks = self.split_row_blocks(parsed_rows, start_marker="TRADE_DATE")
            funds_data = []

            for block in fund_blocks:
                header_idx = self.find_header_idx(block, markers={"CUSIP", "TICKER", "DESCRIPTION"})
                if header_idx == -1:
                    continue

                metadata_df = self._extract_metadata(block[:header_idx])
                holdings_df = self.rows_to_dataframe(block[header_idx:]).reset_index(drop=True)
                funds_data.append({"fund_metadata": metadata_df, "fund_holdings": holdings_df})

            return funds_data
        except Exception as e:
            self.logger.error(f"Failed to extract data: {e}")
            return []

    def _extract_metadata(self, chunk: list[list[str]]) -> pd.DataFrame:
        """Extract metadata from fixed-format top lines (no cleaning)."""
        metadata = {}
        if not chunk:
            return pd.DataFrame([metadata])

        first_row = chunk[0]
        metadata.update({
            metadata[self.row_value(first_row, 0)]: self.row_value(first_row, 1),
            "SS_LONG_CODE": self.row_value(first_row, 2),
            "FULL_NAME": self.row_value(first_row, 4),
            "TICKER_1": self.row_value(first_row, 5),
            "TICKER_2": self.row_value(first_row, 6),
            "BASE_CURRENCY": self.row_value(first_row, 7),
            metadata[self.row_value(first_row, 8)]: self.row_value(first_row, 9)
        })
        metadata.update(self.pairs_to_dict(chunk[1:]))

        return pd.DataFrame([metadata])
    