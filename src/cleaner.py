#========================================================
# Data cleaning and type casting utilities for financial data pipelines.
# Supports: numeric (US/EU formats), percentages, timezone-aware dates, strings.
# Handles malformed data, missing values, and international number formats.
#========================================================

import pandas as pd

def clean(data: pd.DataFrame, schema: dict = None, **kwargs) -> pd.DataFrame:
    """Clean and cast DataFrame columns based on provided schema.
    
    Args:
        data (pd.DataFrame): The DataFrame to clean.
        schema (dict, optional): A dictionary specifying the target datatypes for columns.
        **kwargs: Additional arguments for specific cleaning functions.
    """
    df = data.copy()
    schema = schema or {}
    for col in df.columns:
        if col in schema:
            df[col] = _cast(df[col], schema[col], **kwargs)
        else:
            df[col] = _clean_str(df[col])
    return df

def _cast(series: pd.Series, target_type, **kwargs) -> pd.Series:
    """Cast a Series to the target type with cleaning.
    
    Args:
        series (pd.Series): The Series to cast.
        target_type: The target datatype or a string representing the target datatype.
        **kwargs: Additional arguments for specific casting functions.
    """
    if target_type == float:
        return _extract_numeric(series)
    elif target_type == int:
        return _extract_numeric(series).round().astype("Int64")
    elif isinstance(target_type, str) and target_type.lower() in ["pct", "percentage", "percent"]:
        return _extract_numeric(series) / 100.0
    elif isinstance(target_type, str) and target_type.lower() in ["datetime64[ns]", "datetime64", "datetime", "timestamp", "date"]:
        return _parse_dates(_clean_str(series), **kwargs)
    return series

def _clean_str(series: pd.Series) -> pd.Series:
    """Clean string values in a Series.
    
    Args:
        series (pd.Series): The Series to clean.
    """
    stripped = series.astype(str).str.strip().str.lstrip("'\"\ ").str.rstrip("'\"\ ")
    return stripped.replace("", pd.NA)

def _extract_numeric(series: pd.Series, number_format: str = "US") -> pd.Series:
    """Extract numeric values from a Series, handling different number formats.
    
    Args:
        series (pd.Series): The Series to extract numeric values from.
        number_format (str, optional): The number format, either "US" or "EU". Defaults to "US".
    """
    if pd.api.types.is_numeric_dtype(series):
        return series
    s = series.astype(str).str.strip()
    cleaned = s.str.extract(r"(-?[\d.,]+)", expand=False)
    if number_format.upper() == "EU":
        cleaned = cleaned.str.replace(".", "", regex=False).str.replace(",", ".")
    else:
        cleaned = cleaned.str.replace(",", "", regex=False)
    return pd.to_numeric(cleaned, errors="coerce")

def _parse_dates(series: pd.Series, date_input_format: str = "%m/%d/%Y", 
                end_of_business_hour: int = 16, timezone: str = "US/Eastern") -> pd.Series:
    """Parse dates from a Series, handling various formats and timezones.
    
    Args:
        series (pd.Series): The Series to parse dates from.
        date_input_format (str, optional): The date format to use for parsing. Defaults to "%m/%d/%Y".
        end_of_business_hour (int, optional): The hour to set for end-of-business day. Defaults to 0.
        timezone (str, optional): The timezone to localize the dates. Defaults to "US/Eastern".
    """
    dt = pd.to_datetime(series, format=date_input_format, errors="coerce") 
    if dt.isna().all() and series.notna().any():
        dt = pd.to_datetime(series, format=None, errors="coerce")
    if dt.dt.tz is not None:
        return dt.dt.tz_convert(timezone)
    midnight_mask = (dt.dt.hour == 0) & (dt.dt.minute == 0) & dt.notna()
    dt.loc[midnight_mask] = dt.loc[midnight_mask] + pd.Timedelta(hours=end_of_business_hour)
    return dt.dt.tz_localize(timezone)

if __name__ == "__main__":
    test_df = pd.DataFrame({
        0: ["  '1,234.56' ", "1,200.00"],
        1: ["7.89 %", "10%"],
        2: ["04/17/25", "03/01/26"],
        3: ["'Some text","Nothigss''"]
    })

    cleaned = clean(test_df, schema={0: float, 1: "pct", 2: "datetime"})
    print(cleaned)