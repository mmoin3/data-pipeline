"""
Silver layer transformer: Modular transformation functions
(similar to parsers.py - provides building blocks, not orchestration)
"""
import polars as pl
from typing import Callable


def normalize_column_names(df: pl.DataFrame) -> pl.DataFrame:
    """
    Rename all columns to lowercase snake_case.
    "Fund Code" → "fund_code", "Position-ID" → "position_id"
    """
    rename_map = {}
    for col in df.columns:
        new_col = col.strip().lower().replace(" ", "_").replace("-", "_")
        new_col = "".join(c if c.isalnum() or c ==
                          "_" else "" for c in new_col)
        while "__" in new_col:
            new_col = new_col.replace("__", "_")
        rename_map[col] = new_col

    return df.rename(rename_map)


def cast_columns(df: pl.DataFrame, cast_map: dict) -> pl.DataFrame:
    """
    Cast specific columns to given types.

    Args:
        df: Input DataFrame
        cast_map: Dict of {col_name: "Float64"|"Int64"|"Date"|"Utf8"|"Boolean"}

    Example:
        df = cast_columns(df, {"qty": "Float64", "date": "Date"})
    """
    for col, dtype_str in cast_map.items():
        if col not in df.columns:
            continue
        dtype = getattr(pl, dtype_str)
        df = df.with_columns(pl.col(col).cast(dtype, strict=False))

    return df


def rename_columns(df: pl.DataFrame, rename_map: dict) -> pl.DataFrame:
    """
    Rename specific columns.

    Example:
        df = rename_columns(df, {"fundcode": "fund_id", "posid": "position_id"})
    """
    return df.rename(rename_map)


def drop_columns(df: pl.DataFrame, columns: list) -> pl.DataFrame:
    """
    Drop specific columns if they exist.

    Example:
        df = drop_columns(df, ["temp_flag", "internal_audit"])
    """
    cols_to_drop = [c for c in columns if c in df.columns]
    return df.drop(cols_to_drop) if cols_to_drop else df


def deduplicate(df: pl.DataFrame, subset: list, keep: str = "last") -> pl.DataFrame:
    """
    Remove duplicate rows based on key columns.

    Args:
        df: Input DataFrame
        subset: List of columns to deduplicate on
        keep: "first", "last", "none"

    Example:
        df = deduplicate(df, ["fund_id", "date"])
    """
    return df.unique(subset=subset, keep=keep)


def validate_columns(df: pl.DataFrame, required_columns: list) -> None:
    """
    Raise error if required columns missing.

    Example:
        validate_columns(df, ["fund_id", "position_id"])
    """
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def filter_rows(df: pl.DataFrame, filter_fn: Callable) -> pl.DataFrame:
    """
    Apply custom filter function to rows.

    Args:
        df: Input DataFrame
        filter_fn: Function that takes df and returns filtered df

    Example:
        df = filter_rows(df, lambda d: d.filter(pl.col("qty") > 0))
    """
    return filter_fn(df)


def split_table(df: pl.DataFrame, split_fn: Callable) -> dict:
    """
    Split one table into multiple tables using custom logic.

    Args:
        df: Input DataFrame
        split_fn: Function that takes df, returns dict of {table_name: df}

    Returns:
        Dict of {new_table_name: DataFrame}

    Example:
        def normalize_positions(df):
            # Extract US positions to separate table
            us = df.filter(pl.col("country") == "US")
            intl = df.filter(pl.col("country") != "US")
            return {"positions_us": us, "positions_intl": intl}

        result = split_table(df, normalize_positions)
        # result = {"positions_us": df1, "positions_intl": df2}
    """
    return split_fn(df)


def apply_transformations(df: pl.DataFrame, transformations: list) -> pl.DataFrame:
    """
    Chain transformation functions together.

    Args:
        df: Input DataFrame
        transformations: List of (function, kwargs) tuples

    NOTE: If using split_table(), it must be the LAST transformation in the list,
          as it returns dict[str, DataFrame] instead of a single DataFrame.

    Example:
        transformations = [
            (normalize_column_names, {}),
            (cast_columns, {"cast_map": {"qty": "Float64"}}),
            (deduplicate, {"subset": ["fund_id"]}),
        ]
        df = apply_transformations(df, transformations)

    Example with split:
        def split_by_type(df):
            return {
                "type_a": df.filter(pl.col("type") == "A"),
                "type_b": df.filter(pl.col("type") == "B"),
            }

        transformations = [
            (normalize_column_names, {}),
            (split_table, {"split_fn": split_by_type}),  # MUST be last
        ]
        result = apply_transformations(df, transformations)
        # result = {"type_a": df1, "type_b": df2}
    """
    for func, kwargs in transformations:
        df = func(df, **kwargs)
    return df
