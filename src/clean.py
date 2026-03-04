import pandas as pd
from typing import Dict
from config.settings import NULL_LIKE_VALUES

def typecast_by_map(
    df: pd.DataFrame,
    type_map: Dict,
) -> pd.DataFrame:
    """Typecast DataFrame columns using a config map.
    Unknown columns are left untouched. Missing columns are skipped.

    Supported target types in map: str, int, float, "datetime64[ns]"
    """
    out = df.copy()

    for col, target_type in type_map.items():
        if col not in out.columns:
            continue

        series = _normalize_text(out[col])
        if target_type == "datetime64[ns]":
            out[col] = pd.to_datetime(series, errors="coerce")
        elif target_type == float:
            out[col] = pd.to_numeric(_normalize_numeric(out[col]), errors="coerce")
        elif target_type == int:
            cleaned = _normalize_numeric(out[col])
            out[col] = pd.to_numeric(cleaned, errors="coerce")
        elif target_type == str:
            out[col] = series.astype("string")

    return out

def _normalize_text(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().str.lstrip("'").replace(NULL_LIKE_VALUES, pd.NA)

def _normalize_numeric(series: pd.Series) -> pd.Series:
    return (
        _normalize_text(series)
        .str.replace(",", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.replace("%", "", regex=False)
    )