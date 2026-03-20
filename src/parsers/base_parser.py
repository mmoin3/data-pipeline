import csv, io, logging, sqlalchemy
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

class BaseParser:
    """Simple file reader that detects type by extension and reads into a DataFrame."""
    
    def __init__(self, file_path: str):
        self.path = Path(file_path)

    def read_into_dataframe(self, ext_override:str=None,**kwargs) -> pd.DataFrame:
        """Read file and return DataFrame. Detect type by extension and dispatch.
        Data must already be in tabular for this method to work in isolation

        ext_override: param can be used to override file extension detection
        **kwargs: passed directly to pandas read functions, e.g. read_csv or read_excel
        """
        ext = ext_override.lstrip(".").lower() if ext_override else self.path.suffix.lstrip(".").lower()
            
        if ext in {"csv","ndm01"}:
            return pd.read_csv(self.path, **kwargs)
        elif ext in {"xls", "xlsx", "xlsm", "xlsb"}:
            return pd.read_excel(self.path, **kwargs)
        elif ext == "json":
            lines = kwargs.pop("lines", True)
            return pd.read_json(self.path, lines=lines, **kwargs)
        elif ext == "txt":
            return pd.read_table(self.path, **kwargs)
        elif ext == "parquet":
            return pd.read_parquet(self.path, **kwargs)
        else:
            logger.error(f"Unsupported file type: {ext}")
            return pd.DataFrame()
  
    def list_to_dataframe(self, lines: list[str], **kwargs) -> pd.DataFrame:
        """Convert a list of lines to a pandas DataFrame. Log errors if logger is provided."""
        try:
            if not lines:
                return pd.DataFrame()
            csv_text = "\n".join(line.rstrip("\r\n") for line in lines)
            df = pd.read_csv(io.StringIO(csv_text), **kwargs)
            return df
        except Exception as e:
            logger.error(f"Failed to parse lines to DataFrame: {e}")
            return pd.DataFrame()
        
    def get_blocks(self, table:pd.DataFrame, start_marker:str) -> list[pd.DataFrame]:
        """Split DataFrame into blocks based on rows that start with a marker."""
        try:
            starts = [i for i,v in enumerate(table.iloc[:,0]) if v.startswith(start_marker)]
            blocks = []
            for idx, start in enumerate(starts):
                if (idx + 1) < len(starts):
                    end = starts[idx + 1]
                else:
                    end = len(table)
                blocks.append(table[start:end].reset_index(drop=True))
            return blocks
        except Exception as e:
            logger.error(f"Error splitting table into blocks: {e}")
            return []
        
    def get_header_idx(self, data:pd.DataFrame, header_marker) -> int:
        """Find the first line that contains the header marker and return it as a list of column names.line could also be a pandas dataaframe header row"""
        for idx, line in enumerate(data.iloc[:,0]):
            if header_marker in line:
                return idx
        return -1

    def read_rows(self) -> list[list[str]]:
        """Read file line-by-line and parse each line with CSV-first logic."""
        rows = []
        with open(self.path, "r", encoding="utf-8", errors="replace") as file:
            for raw_line in file:
                line = raw_line.rstrip("\r\n")
                if not line:
                    continue
                rows.append(self.parse_line(line))
        return rows

    def parse_line(self, line: str) -> list[str]:
        """Parse a line using csv.reader when possible, fallback to tab split."""
        try:
            fields = next(csv.reader([line], skipinitialspace=True))
        except Exception:
            fields = [line]
        if len(fields) <= 1 and "\t" in line:
            fields = line.split("\t")

        return [field.strip() for field in fields]

    def split_row_blocks(self, rows: list[list[str]], start_marker: str) -> list[list[list[str]]]:
        """Split parsed rows into blocks where first column starts with marker."""
        blocks = []
        current_block = []
        normalized_marker = start_marker.upper()

        for row in rows:
            first_cell = row[0].upper() if row and row[0] else ""
            if first_cell.startswith(normalized_marker) and current_block:
                blocks.append(current_block)
                current_block = []
            current_block.append(row)

        if current_block:
            blocks.append(current_block)

        return blocks

    def find_header_idx(self, rows: list[list[str]], markers: set[str]) -> int:
        """Find first row where any marker is present as a field."""
        normalized_markers = {marker.upper() for marker in markers}
        for idx, row in enumerate(rows):
            row_values = {value.upper() for value in row if value}
            if normalized_markers.intersection(row_values):
                return idx
        return -1

    def rows_to_dataframe(self, rows: list[list[str]]) -> pd.DataFrame:
        """Convert parsed rows to DataFrame, padding uneven rows safely."""
        if not rows:
            return pd.DataFrame()
        header = rows[0]
        data_rows = rows[1:]

        max_len = max([len(header)] + [len(row) for row in data_rows]) if data_rows else len(header)
        normalized_header = header + [f"unnamed_{idx}" for idx in range(len(header), max_len)]

        normalized_data = []
        for row in data_rows:
            normalized_data.append(row + [""] * (max_len - len(row)))

        dataframe = pd.DataFrame(normalized_data, columns=normalized_header)
        return dataframe.dropna(how="all")

    def row_value(self, row: list[str], index: int, default: str = "") -> str:
        """Safely get a row value by index."""
        return row[index] if index < len(row) else default
    
    def insert_timestamp(self, df: pd.DataFrame, column_name: str = "loaded_at") -> pd.DataFrame:
        """Insert a timestamp column with current time."""
        df[column_name] = pd.Timestamp.now()
        return df
    def insert_filename(self, df: pd.DataFrame, file_name: str, column_name: str = "file_name") -> pd.DataFrame:
        """Insert a column with the source file name."""
        df[column_name] = file_name
        return df

    