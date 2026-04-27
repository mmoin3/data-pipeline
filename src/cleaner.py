from datetime import datetime, date
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def clean_and_cast(df: pd.DataFrame, silver_mapping) -> pd.DataFrame:
    """Clean, rename, and cast DataFrame per SilverMapping config."""
    df = df.copy()

    # Deduplicate exact row matches first (before any transformations)
    rows_before = len(df)
    df = df.drop_duplicates(keep='first')
    rows_after = len(df)
    duplicates_removed = rows_before - rows_after
    if duplicates_removed > 0:
        logger.info(
            f"Deduplicated {duplicates_removed} duplicate rows (kept {rows_after})")

    columns_map = silver_mapping.columns  # dict[source_col -> ColumnMapping]

    # Validate config
    for source_col, col_map in columns_map.items():
        if col_map.source_dtype not in TYPE_CONVERTERS:
            raise ValueError(
                f"Column '{source_col}': unknown type {col_map.source_dtype}")

    # Rename mapped columns
    rename_map = {source_col: col_map.target_name for source_col,
                  col_map in columns_map.items()}
    unmapped = [c for c in df.columns if c not in rename_map]
    df.rename(columns=rename_map, inplace=True)

    # Rename unmapped columns to snake_case
    unmapped_snake = {c: _to_snake_case(c)
                      for c in unmapped if c != _to_snake_case(c)}
    df.rename(columns=unmapped_snake, inplace=True)

    # Cast mapped columns
    for source_col, col_map in columns_map.items():
        target_col = col_map.target_name
        if target_col not in df.columns:
            logger.warning(f"Column '{source_col}' not found")
            continue

        # Track how many empty strings will be converted to null
        empty_str_count = (df[target_col].astype(
            str).str.strip().str.lstrip("'") == "").sum()
        before = df[target_col].notna().sum()

        # Capture sample of non-empty values that will be lost (for diagnostics)
        lost_before = df[target_col].copy()

        df[target_col] = TYPE_CONVERTERS[col_map.source_dtype](
            df[target_col], col_map)

        after = df[target_col].notna().sum()
        lost_count = before - after

        if lost_count > 0:
            # Log as info since quote-only values are intentionally nulled
            lost_values = lost_before[(lost_before.notna()) & (
                df[target_col].isna())].unique()
            lost_sample = str(list(lost_values[:5]))
            logger.info(
                f"Column '{target_col}': {lost_count} values nulled ({empty_str_count} empty, {lost_count - empty_str_count} quote-only). "
                f"Samples: {lost_sample}")

    # Cast unmapped to string, except ingested_at (preserve as datetime)
    for original_col in unmapped:
        col_snake = _to_snake_case(original_col)
        if col_snake == "ingested_at":
            # Keep ingested_at as datetime, don't stringify
            continue
        # Access by snake_case name since we already renamed
        if col_snake in df.columns:
            df[col_snake] = _normalize_str(df[col_snake])

    return df


def _normalize_str(series):
    """String-first: convert to string, strip, replace empty with NA."""
    return series.astype(str).str.strip().str.lstrip("'").replace("", pd.NA)


def _cast_datetime(series, col_map):
    if pd.api.types.is_datetime64_any_dtype(series):
        return series
    fmt = col_map.datetime_format
    return pd.to_datetime(_normalize_str(series), format=fmt, errors='coerce').dt.tz_localize("America/New_York")


def _cast_str(series, col_map):
    return _normalize_str(series)


def _cast_float(series, col_map):
    if pd.api.types.is_float_dtype(series):
        return series
    return pd.to_numeric(_normalize_str(series).str.replace(",", ""), errors='coerce')


def _cast_int(series, col_map):
    if series.dtype == "Int64":
        return series
    flt = pd.to_numeric(_normalize_str(
        series).str.replace(",", ""), errors='coerce')
    return round(flt).astype("Int64")


def _cast_bool(series, col_map):
    if series.dtype == "bool":
        return series
    bool_map = {"true": True, "false": False,
                "1": True, "0": False, "yes": True, "no": False, "y": True, "n": False}
    return _normalize_str(series).str.lower().map(bool_map)


def _cast_pct(series, col_map):
    return pd.to_numeric(
        _normalize_str(series).str.replace("%", "").str.replace(",", ""),
        errors='coerce'
    ) / 100


TYPE_CONVERTERS = {
    datetime: _cast_datetime,
    str: _cast_str,
    float: _cast_float,
    int: _cast_int,
    bool: _cast_bool,
    "pct": _cast_pct,
}


def _to_snake_case(s: str) -> str:
    s = s.strip().lower().replace(" ", "_").replace(
        "/", "_").replace("-", "_").replace(".", "_")
    s = "".join(c if c.isalnum() or c == "_" else "" for c in s)
    while "__" in s:
        s = s.replace("__", "_")
    return s


if __name__ == "__main__":
    from config import SILVER_MAPPINGS

    # Use actual mapping from config
    silver_mapping = SILVER_MAPPINGS["harvest_fund_identifiers"]

    # Test data with column names from bronze
    test_df = pd.DataFrame({
        "ss_id": ["ABC", "XYZ", "DEF", "ABC", "GHI"],
        "ticker": ["ABC.TO", "XYZ.TO", "DEF.TO", "ABC.TO", "GHI.TO"],
        "fund_incept_dt": ["01/15/2010", "03/22/2012", "", "01/15/2010", "06/30/2008"],
        "id_cusip": ["12345678", "87654321", "bad_cusip", "", "11111111"],
        "figi": ["BBG000ABC123", "BBG000XYZ456", "", "BBG000ABC123", "BBG000GHI789"],
        "id_isin": ["CA1234567890", "CA0987654321", "invalid", "", "CA1111111111"],
        "id_sedol1": ["SEDOL001", "SEDOL002", "", "", "SEDOL003"],
        "dda": ["100", "200", "not_int", "", "150"],
        "ingested_at": pd.date_range("2024-01-01", periods=5),
        "extra_metadata": ["source1", "source2", "source3", "source4", "source5"],
    })

    print("BEFORE CLEANING:")
    print(test_df)
    print("\nDtypes before:", test_df.dtypes.to_dict())

    result = clean_and_cast(test_df, silver_mapping)

    print("\n" + "=" * 80)
    print("AFTER CLEANING:")
    print(result)
    print("\nDtypes after:", result.dtypes.to_dict())
