import pandas as pd
from config import *

class DataFrameCleaner:
    """Clean and typecast DataFrames with optional schema."""

    def clean(self, df, schema=None) -> pd.DataFrame:
        """Clean a DataFrame. Schema columns get cast, string/object columns get cleaned, rest left as-is."""
        if not isinstance(df, pd.DataFrame):
            if isinstance(df, list):
                # If it's a list of DataFrames, concatenate them. Otherwise, try to make a DataFrame from it.
                df = pd.concat(df, ignore_index=True) if df and all(isinstance(x, pd.DataFrame) for x in df) else pd.DataFrame(df)
            else:
                df = pd.DataFrame(df)
        # Ensure columns are lowercase, underscored, stripped strings for consistent processing.
        # This also ensures column names are strings, which is important for the rest of the cleaning logic.
        df.columns = df.columns.astype(str).str.strip().str.lower().str.replace(r"[\-]","_",regex=True)
        for col in df.columns:
            if col in schema:
                df[col] = self.cast(df[col], schema[col])
            elif pd.api.types.is_string_dtype(df[col]) or df[col].dtype == object:
                df[col] = self._clean_str(df[col])
        return df

    def _clean_str(self, s: pd.Series, exceptions = NULL_LIKE_VALUES) -> pd.Series:
        """Clean string values — strip whitespace, remove leading apostrophes, replace null-like values."""
        return s.astype(str).str.strip().str.lstrip("'").replace(exceptions, pd.NA).astype("string")  # fix: added .astype("string")

    def _strip_numeric_chars(self, series: pd.Series) -> pd.Series:
        """Strip all non-numeric characters. Keep only digits, decimal point, comma, and minus sign."""
        if pd.api.types.is_numeric_dtype(series):
            return series

        s = series.astype(str).str.strip()
        is_percent = s.str.endswith("%")

        # Keep only: digits, decimal point, comma, minus sign
        cleaned = s.str.extract(r"(-?)[\d.,]+", expand=False).str.strip()

        numeric = pd.to_numeric(cleaned, errors="coerce")
        return numeric.where(~is_percent, numeric/100.0)

    def cast(self, s: pd.Series, target_type, exceptions = NULL_LIKE_VALUES) -> pd.Series:
        """Cast series to target type."""

        if target_type == float:
            return self._strip_numeric_chars(s)

        if target_type == int:
            return self._strip_numeric_chars(s).round().astype("Int64")

        if target_type == bool:
            if pd.api.types.is_numeric_dtype(s):  # fix: guard numeric dtype before .str accessor
                return s.map({1: True, 0: False, 1.0: True, 0.0: False}).astype("boolean")
            bool_map = {"TRUE": True, "FALSE": False, "Y": True, "N": False, "YES": True, "NO": False}
            return self._clean_str(s).str.upper().map(bool_map).astype("boolean")

        if target_type == str:
            return self._clean_str(s, exceptions)

        if str(target_type).lower() in ["datetime64[ns]", "datetime64", "datetime", "timestamp", "date"]:
            if pd.api.types.is_datetime64_any_dtype(s):
                return s
            return self.parse_dates(self._clean_str(s))  # fix: clean string before parsing

        return s  # unknown type: leave as-is

    def parse_dates(self, s: pd.Series) -> pd.Series:
        """Parse dates with multiple formats."""
        result = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
        mask = s.notna()

        for fmt in ["%Y%m%d", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"]:
            if not mask.any():
                break
            parsed = pd.to_datetime(s.where(mask), format=fmt, errors="coerce")
            success = parsed.notna() & mask
            result.loc[success] = parsed[success]
            mask &= ~success

        if mask.any():
            fallback = s.where(mask).apply(lambda x: pd.to_datetime(x, errors="coerce"))
            success = fallback.notna() & mask
            result.loc[success] = fallback[success]

        return result