import pandas as pd
import csv
from pathlib import Path


def extract_cil(file_path: Path) -> pd.DataFrame:
    """Custom parser for the CIL files: Harvest_CIL_ALL.YYYYMMDD.CSV.

    Returns:
        Single unified DataFrame with all CIL records
    """
    df = extract_complex(file_path)
    df_new = pd.DataFrame(
        data=df.values[2:], columns=df.iloc[1]).reset_index(drop=True)
    df_new[df.iloc[0, 0]] = df.iloc[0, 1]
    return df_new


def extract_ucf(file_path: Path) -> pd.DataFrame:
    """Custom parser for the UCF files: Harvest_UCF_ALL.YYYYMMDD.CSV.

    Returns:
        Single unified DataFrame with all UCF records
    """
    df = pd.read_csv(file_path, header=None, names=range(34))
    blocks = _split_into_blocks(df, markers="FUND_NAME")
    holdings_list = []
    for block in blocks:
        metrics_df = pd.concat(
            [block.iloc[:7], block.iloc[-16:]], ignore_index=True).reset_index(drop=True)
        metrics_dict = _extract_ucf_metrics(metrics_df)

        holdings_df = pd.DataFrame(
            data=block.iloc[8:-16].values, columns=block.iloc[7].tolist()).reset_index(drop=True)

        for key, value in metrics_dict.items():
            holdings_df[key] = value
        holdings_list.append(holdings_df)

    return pd.concat(holdings_list, ignore_index=True) if holdings_list else pd.DataFrame()


def extract_accounting_navs(file_path: Path) -> pd.DataFrame:
    """Custom parser for the accounting nav files: Harvest Price File - YYYYMMDD.CSV.

    Returns:
        Single unified DataFrame with all accounting nav records
    """
    df = pd.read_excel(file_path, engine="xlrd", header=None)
    df_new = pd.DataFrame(
        data=df.values[4:], columns=df.iloc[3]).reset_index(drop=True)
    df_new[df.iloc[1, 6]] = df.iloc[1, 7]
    return df_new


def extract_pcf(file_path: Path) -> pd.DataFrame:
    """Custom parser for the basket files: Harvest_INAVBSKT_ALL.YYYYMMDD.CSV,
    and Harvest_BSKT_ALL.YYYYMMDD.CSV.

    Returns:
        Single unified DataFrame with holdings + metrics columns
    """
    df = pd.read_csv(file_path, header=None, names=range(25))
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
    return pd.concat(enriched_holdings_list, ignore_index=True) if enriched_holdings_list else pd.DataFrame()


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
            key = row[j]
            value = row[j + 1]
            if pd.notna(key):  # Skip NaN keys to avoid 'nan' column headers
                metrics_dict[key] = value
    return metrics_dict


def _extract_ucf_metrics(df: pd.DataFrame) -> dict:
    """Extract metrics from the first 8 lines of each block."""
    metrics_dict = {}
    metrics_dict["ORDER_TYPE"] = df.iloc[6, 0]
    metrics_dict['BROKER_NAME'] = df.iloc[3, 2]

    # Clear this cell to avoid it being treated as a column header
    df.iloc[3, 2] = None
    df.drop(index=6, inplace=True)  # Drop the 6th row
    for i in range(len(df)):
        row = df.iloc[i]
        for j in range(0, len(row)-1, 2):
            key = row[j]
            value = row[j + 1]
            if pd.notna(key):  # Skip NaN keys to avoid 'nan' column headers
                metrics_dict[key] = value
    return metrics_dict


# def extract(file_path: Path, ext_override: str = None, **kwargs) -> pd.DataFrame:
#     """
#     Simple extracter function that can handle 80% of files by just reading them into a dataframe.

#     Args:
#         file_path: Path to the input file.
#         ext_override: Optional extension to override the file's extension.
#         **kwargs: Additional arguments for specific parsing functions.
#     """
#     if ext_override:
#         ext = ext_override.lower().lstrip(".")
#     else:
#         ext = file_path.suffix.lower().lstrip(".")

#     try:
#         if ext in {"csv", "ndm01"}:
#             return pd.read_csv(file_path, **kwargs)
#         elif ext in {"xls", "xlsx", "xlsm", "xlsb"}:
#             return pd.read_excel(
#                 file_path,
#                 sheet_name=kwargs.pop("sheet_name", 0),
#                 engine=kwargs.pop("engine", "openpyxl"),
#                 **kwargs,
#             )
#     except Exception as e:
#         try:
#             return _extract_complex(file_path, **kwargs)
#         except Exception as e2:
#             logger.error(f"Failed to extract {file_path.name}\n"
#                          f"with both simple and complex methods: {e}, {e2}")
#             raise e2
#     raise ValueError(
#         f"Unsupported file type '{ext}' for file {file_path.name}")

def extract_nbf_sales_qq(file_path: Path) -> pd.DataFrame:
    """Custom parser for the NBF sales qq files: NBF_SALES_QQ.YYYYMMDD.xlsx.
    just reads the file and does some basic cleaning and wrangling with bulk transforms like type casting in silver
    Returns:
        Single unified DataFrame with all sales qq records
    """
    df = pd.read_excel(file_path, header=None)
    df.columns = ["Region", "Branch"] + df.iloc[0,
                                                2:].astype(str).str.replace(".", "/").tolist()
    df_new = pd.DataFrame(
        data=df.iloc[2:, :].values, columns=df.columns).reset_index(drop=True)
    df_new = df_new.replace(r"^\s*$", pd.NA, regex=True)
    df_new = df_new.dropna(how='all').reset_index(drop=True)
    df_new = df_new[df_new.iloc[:, 0].astype(
        str).str.upper().isin(["GRAND TOTAL", "TOTAL"]) == False]
    df_new = df_new.loc[:, ~df_new.columns.str.upper().isin(
        ["GRAND TOTAL", "TOTAL"])]
    # convert all columns except Region and Branch to float, coercing errors to NaN
    df_new[df_new.columns[2:]] = df_new[df_new.columns[2:]].apply(
        pd.to_numeric, errors='coerce', downcast='float')
    return df_new


def extract_bmo_sales_qq(file_path: Path) -> pd.DataFrame:
    df = pd.read_excel(file_path, header=None)
    search_grid = df.iloc[0:6, 0:6].astype(str).apply(
        lambda x: x.str.strip().str.upper())
    found = (search_grid == "BRANCH").stack()
    if not found.any():
        raise ValueError(
            "Could not find 'Branch' in the first 6 rows and columns.")
    anchor_row, anchor_col = found.idxmax()
    df_new = pd.DataFrame(
        data=df.iloc[anchor_row+1:, anchor_col:].values, columns=df.iloc[anchor_row, anchor_col:])
    df_new = df_new.replace(r"^\s*$", pd.NA, regex=True)
    df_new = df_new.dropna(how='all').reset_index(drop=True)
    totals_mask = df_new["Branch"].astype(str).str.upper().isin(
        ["TOTAL", "GRAND TOTAL", "TOTALS"])
    if totals_mask.any():
        totals_idx = df_new[totals_mask].index[0]
        df_new = df_new.iloc[:totals_idx]
    df_new = df_new.dropna(how='all').reset_index(drop=True)
    # drop the column with "TOTAL" header if it exists
    df_new = df_new.loc[:, ~df_new.columns.str.upper().isin(
        ["TOTAL", "GRAND TOTAL", "TOTALS"])]
    df_new = df_new.dropna(axis=1, how='all')
    return df_new


def extract_complex(file_path: Path, **csv_kwargs) -> pd.DataFrame:
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
    nbf_file = Path(
        r"C:\Users\mmoin\PYTHON PROJECTS\Commissions Report\data\raw\Broker Data\NBF\NBF_Q1_2026.xlsx")
    df = extract_nbf_sales_qq(nbf_file)
    df.info()
    print(df.head())
