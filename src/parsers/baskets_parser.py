import pandas as pd
from src.parsers.base_parser import BaseParser
from src.utils.logger import get_logger

logger = get_logger(__name__)


class BasketsParser(BaseParser):
    """Parser for INAV and BSKT non-tabular files. Returns two DataFrames: metrics and holdings."""

    def parse(self, **kwargs) -> dict[str, pd.DataFrame]:
        """Parse irregular BSKT format and return metrics and holdings DataFrames.

        Args:
            **kwargs: Optional arguments (e.g., delimiter override for _parse_irregular).

        Returns:
            Dict with "metrics" and "holdings" keys.
        """
        try:
            df0 = self._parse_irregular(delimiter=",", skip_empty=True)
            fund_blocks = self._split_into_blocks(df0)

            metrics_list = []
            holdings_list = []
            for block in fund_blocks:
                # Extract metrics data from this block
                metrics_data = self._extract_metrics(block[:8])

                # Now extract holdings data. Create holdings DataFrame
                holdings_header = block.iloc[8].tolist()
                holdings_df = pd.DataFrame(
                    block[9:].values, columns=holdings_header)

                # Add fund identifiers to holdings (at positions 0 and 1)
                holdings_df.insert(0, "TRADE_DATE", metrics_data["TRADE_DATE"])
                holdings_df.insert(1, "SS_LONG_CODE",
                                   metrics_data["SS_LONG_CODE"])

                metrics_list.append(metrics_data)
                holdings_list.append(holdings_df)

            metrics = pd.DataFrame(
                metrics_list) if metrics_list else pd.DataFrame()
            holdings = pd.concat(
                holdings_list, ignore_index=True) if holdings_list else pd.DataFrame()

            return {"metrics": metrics, "holdings": holdings}

        except Exception as e:
            logger.error(f"Failed to parse {self.path.name}: {e}")
            return {"metrics": pd.DataFrame(), "holdings": pd.DataFrame()}

    def _split_into_blocks(self, df: pd.DataFrame, markers: str = "TRADE_DATE") -> list[pd.DataFrame]:
        """Split DataFrame into blocks based on TRADE_DATE marker."""
        # Find all row indices where TRADE_DATE appears
        block_starts = df[df[0].str.upper().str.startswith(
            markers.upper(), na=False)].index.tolist()
        block_starts.append(len(df))  # Add end of file as last block end

        # Create blocks by slicing between consecutive TRADE_DATE markers
        blocks = []
        for i in range(len(block_starts) - 1):
            start_idx = block_starts[i]
            end_idx = block_starts[i + 1]
            # Its critical that indices are reset here.
            block = df.iloc[start_idx:end_idx].reset_index(drop=True)
            blocks.append(block)

        return blocks

    def _extract_metrics(self, df: pd.DataFrame) -> dict:
        """Extract metrics from the first 8 lines of each block."""
        metrics_dict = {}
        # First line key-value pair (e.g., "TRADE_DATE", "20260224")
        metrics_dict[df.iloc[0, 0]] = df.iloc[0, 1]
        metrics_dict["SS_LONG_CODE"] = df.iloc[0, 2]
        metrics_dict["FULL_NAME"] = df.iloc[0, 4]
        metrics_dict["TICKER_1"] = df.iloc[0, 5]
        metrics_dict["TICKER_2"] = df.iloc[0, 6]
        metrics_dict["BASE_CURRENCY"] = df.iloc[0, 7]
        metrics_dict[df.iloc[0, 8]] = df.iloc[0, 9]

        for i in range(1, len(df)):
            row = df.iloc[i]
            row_size = len(row)
            for j in range(0, row_size - 1, 2):
                metrics_dict[row[j]] = row[j + 1]

        return metrics_dict


if __name__ == "__main__":
    baskets_parser = BasketsParser(
        r"C:\Users\mmoin\PYTHON PROJECTS\data-pipeline\data\0_raw data\Harvest_INAVBSKT_ALL.20260227.CSV")
    ans = baskets_parser.parse()

    print("Metrics:")
    print(ans["metrics"].head(5))

    print("\nHoldings:")
    print(ans["holdings"].head(5))
