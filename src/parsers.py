from unittest import result
import polars as pl
import pandas as pd
import csv
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def parse_inkind(file_path: Path) -> pd.DataFrame:
    """Custom parser for the In-Kind files: Harvest_INKIND.YYYYMMDD.TXT."""
    return pd.read_csv(file_path, skiprows=1)

def parse_pcf(file_path: Path) -> pd.DataFrame:
    """Custom parser for the basket files: Harvest_INAVBSKT_ALL.YYYYMMDD.CSV,
    and Harvest_BSKT_ALL.YYYYMMDD.CSV.

    Returns:
        Single unified DataFrame with holdings + metrics columns
    """
    df = _extract_complex(file_path)
    blocks = _split_into_blocks(df, markers="TRADE_DATE")

    enriched_holdings_list = []

    for block in blocks:
        # Extract metrics from first 8 rows
        metrics_dict = _extract_pcf_metrics(block.iloc[:8])

        # Extract holdings data starting from row 9 (row 8 contains headers)
        holdings_header = block.iloc[8].tolist()
        holdings_df = pd.DataFrame(
            block.iloc[9:].values, columns=holdings_header)

        # Add all metrics columns to holdings dataframe (broadcasts metrics to each row)
        for key, value in metrics_dict.items():
            holdings_df[key] = value
        enriched_holdings_list.append(holdings_df)

    # Concatenate all blocks into one big table
    return pd.concat(enriched_holdings_list, ignore_index=True).dropna(axis=1, how="all") if enriched_holdings_list else pd.DataFrame()


def _split_into_blocks(df: pd.DataFrame, markers: str = "TRADE_DATE") -> list[pd.DataFrame]:
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


def _extract_pcf_metrics(df: pd.DataFrame) -> dict:
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
        for j in range(0, len(row)-1, 2):
            metrics_dict[row[j]] = row[j + 1]

    return metrics_dict

def extract(file_path: Path, ext_override: str = None, **kwargs) -> pd.DataFrame:
    """
    Simple extracter function that can handle 80% of files by just reading them into a dataframe.

    Args:
        file_path: Path to the input file.
        ext_override: Optional extension to override the file's extension.
        **kwargs: Additional arguments for specific parsing functions.
    """
    if ext_override:
        ext = ext_override.lower().lstrip(".")
    else:
        ext = file_path.suffix.lower().lstrip(".")

    try:
        if ext in {"csv", "ndm01"}:
            return pd.read_csv(file_path, **kwargs)
        elif ext in {"xls", "xlsx", "xlsm", "xlsb"}:
            return pd.read_excel(
                file_path,
                sheet_name=kwargs.pop("sheet_name", 0),
                engine=kwargs.pop("engine", "openpyxl"),
                **kwargs,
            )
    except Exception as e:
        try:
            return _extract_complex(file_path, **kwargs)
        except Exception as e2:
            logger.error(f"Failed to extract {file_path.name}\n"
                         f"with both simple and complex methods: {e}, {e2}")
            raise e2
    raise ValueError(
        f"Unsupported file type '{ext}' for file {file_path.name}")


def _extract_complex(file_path: Path, **csv_kwargs) -> pd.DataFrame:
    """Extracter function for irregular files that can't be read by pandas.
    Reads the file line by line and splits by a delimiter.

    Args:
        file_path: Path to the input file.
        delimiter: Delimiter to use for splitting the lines.
        skip_empty: Whether to skip empty lines.
        **csv_kwargs: Additional arguments for the CSV reader.
    """
    records = []
    with open(file_path, "r", newline="", encoding="utf-8") as file:

        reader = csv.reader(file, delimiter=csv_kwargs.pop(
            "delimiter", ","), **csv_kwargs)
        for row in reader:
            if csv_kwargs.pop("skip_empty", False) and not any(row):
                continue
            records.append(row)
    return pd.DataFrame(records)

if __name__ == "__main__":
    # Example usage
    inkind = Path(
        r"/Users/muneebmoin/Desktop/ETL-pipeline/data/ misc/Harvest_mft_data/Harvest_Preburst_INKIND_ALL.20260325.TXT")
    pcf_file = Path(
        r"/Users/muneebmoin/Desktop/ETL-pipeline/data/ misc/Harvest_mft_data/Harvest_INAVBSKT_ALL.20260325.CSV")
    df = parse_pcf(pcf_file)
    print(df.head(15))
    print(df.columns.tolist())
    print(df.columns[pd.notna(df.columns)].shape)
    print(df.shape)
