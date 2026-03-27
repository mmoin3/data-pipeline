"""
BaseParser: Simple CSV/tabular file reader.

Reads tabular files (CSV, Excel, JSON, Parquet, etc.) and returns them as a dict
containing a DataFrame with the raw data.
"""

import csv
import pandas as pd
from pathlib import Path


class BaseParser:
    """Read tabular files and return as a dict of DataFrames."""

    def __init__(self, file_path: str | Path):
        self.path = Path(file_path)

    def read(self, ext_override: str = None, **kwargs) -> pd.DataFrame:
        """Read file and return a DataFrame.

        Args:
            ext_override: Override file extension detection.
            **kwargs: Passed to pandas read function.
        """
        if ext_override:
            ext = ext_override.lstrip(".").lower()
        else:
            ext = self.path.suffix.lstrip(".").lower()

        if ext in {"csv", "ndm01"}:
            return pd.read_csv(self.path, **kwargs)
        elif ext == "txt":
            return pd.read_table(self.path, **kwargs)
        elif ext in {"xls", "xlsx", "xlsm", "xlsb"}:
            return pd.read_excel(
                self.path,
                sheet_name=kwargs.pop("sheet_name", None),
                engine=kwargs.pop("engine", "openpyxl"),
                **kwargs)
        elif ext == "json":
            return pd.read_json(self.path, lines=kwargs.pop("lines", True), **kwargs)
        elif ext == "parquet":
            return pd.read_parquet(self.path, **kwargs)
        else:
            raise ValueError(
                f"Unsupported file type '{ext}' for file {self.path.name}")

    def parse(self, **kwargs) -> dict[str, pd.DataFrame]:
        """Parse the file and return a dict of DataFrames.

        Args:
            **kwargs: Optional arguments passed to read() (e.g., ext_override, pandas read kwargs).

        Returns:
            Dict with key "data" containing the parsed DataFrame.
        """
        return {"data": self.read(**kwargs)}

    def _parse_irregular(self, delimiter: str, skip_empty: bool, **csv_kwargs) -> pd.DataFrame:
        """Custom parser for files with irregular format (e.g. key-value pairs).

        Args:
            delimiter: CSV delimiter character.
            skip_empty: Skip empty lines.
            **csv_kwargs: Additional arguments passed to csv.reader (e.g., quotechar, escapechar).
        """
        records = []
        with open(self.path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter=delimiter, **csv_kwargs)
            for row in reader:
                if skip_empty and not row:
                    continue
                if skip_empty and all(cell.strip() == '' for cell in row):
                    continue
                records.append(row)
        return pd.DataFrame(records)
