import pandas as pd
from config import NULL_LIKE_VALUES, BOOLEAN_MAP


class DataFrameCleaner:
    """Clean and typecast DataFrames with optional schema."""

    def clean(self, df: pd.DataFrame, schema=None) -> pd.DataFrame:
        """Clean and typecast a DataFrame."""
        if not isinstance(df, pd.DataFrame):
            if isinstance(df, list):
                df = pd.concat(df, ignore_index=True) if df and all(isinstance(x, pd.DataFrame) for x in df) else pd.DataFrame(df)
            else:
                df = pd.DataFrame(df)

        schema = schema or {}
        
        for col in df.columns:
            if col in schema:
                df[col] = self._cast(df[col], schema[col])
            else:
                df[col] = self._infer(df[col])

        return df

    def _cast(self, s: pd.Series, target_type):
        """Cast series to target type."""
        s = s.astype(str).str.strip().str.lstrip("'").replace(NULL_LIKE_VALUES, pd.NA)
        
        if target_type == float:
            for char in [",", "$", "%"]:
                s = s.str.replace(char, "", regex=False)
            return pd.to_numeric(s, errors="coerce")
        
        if target_type == int:
            for char in [",", "$", "%"]:
                s = s.str.replace(char, "", regex=False)
            return pd.to_numeric(s, errors="coerce").round().astype("Int64")
        
        if target_type == bool:
            return s.str.upper().map(BOOLEAN_MAP).astype("boolean")
        
        if target_type == str:
            return s.astype("string")
        
        if str(target_type).lower() in ["datetime64[ns]", "datetime64", "datetime", "timestamp", "date"]:
            return self._parse_dates(s)
        
        return s

    def _infer(self, s: pd.Series) -> pd.Series:
        """Infer type and cast."""
        s_clean = s.astype(str).str.strip().str.lstrip("'").replace(NULL_LIKE_VALUES, pd.NA)
        valid = s_clean.dropna()
        
        if valid.empty:
            return s_clean.astype("string")
        
        # Try numeric
        numeric = pd.to_numeric(s.astype(str).str.strip().replace(NULL_LIKE_VALUES, pd.NA).str.replace(",", "", regex=False).str.replace("$", "", regex=False).str.replace("%", "", regex=False), errors="coerce")
        if numeric[valid.index].notna().all():
            return numeric.round().astype("Int64") if ((numeric.dropna() % 1) == 0).all() else numeric
        
        # Try boolean
        bool_result = s_clean.str.upper().map(BOOLEAN_MAP).astype("boolean")
        if bool_result[valid.index].notna().all():
            return bool_result
        
        # Try datetime
        dates = self._parse_dates(s_clean)
        if dates[valid.index].notna().all():
            return dates
        
        return s_clean.astype("string")

    def _parse_dates(self, s: pd.Series) -> pd.Series:
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


_default = DataFrameCleaner()

def clean(df: pd.DataFrame, schema=None) -> pd.DataFrame:
    return _default.clean(df, schema)