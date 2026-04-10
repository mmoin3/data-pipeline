# ========================================================
# Data cleaning and type casting utilities for financial data pipelines.
# Supports: numeric (US/EU formats), percentages, timezone-aware dates, strings.
# Handles malformed data, missing values, and international number formats.
# ========================================================

import polars as pl


def clean(
    data: pl.DataFrame,
    schema: dict = None,
    date_format: str = "%m/%d/%Y",
    number_format: str = "US",
) -> pl.DataFrame:
    """Clean and cast DataFrame columns in three passes:
      1. Strip whitespace/junk chars and null-empty ALL string columns.
      2. Cast columns named in schema to their explicit types.
      3. Infer types for remaining string columns (Int64 → Float64 → leave as String).

    Supported schema types:
        float, int           — numeric extraction (handles commas, currency symbols)
        "pct"/"percent"      — numeric extraction divided by 100
        "datetime"/"date"    — parse to Polars Datetime (UTC)

    Args:
        data:          Input Polars DataFrame.
        schema:        Dict mapping column names to target types.
        date_format:   strptime format string for date columns (default: "%m/%d/%Y").
        number_format: "US" (1,234.56) or "EU" (1.234,56) (default: "US").
    """
    schema = schema or {}

    # ── Step 1: clean all string columns first ────────────────────────────
    string_exprs = [
        pl.col(col)
        .str.strip_chars()
        .str.strip_chars_start("'\"\\ ")
        .str.strip_chars_end("'\"\\ ")
        .replace("", None)
        .alias(col)
        for col in data.columns
        if data[col].dtype == pl.String
    ]
    df = data.with_columns(string_exprs) if string_exprs else data

    # ── Step 2: explicit casts from schema ────────────────────────────────
    cast_exprs = []
    for col, target in schema.items():
        if col not in df.columns:
            continue

        if target in (float, int):
            numeric = pl.col(col).str.extract(r"(-?[\d.,]+)")
            if number_format.upper() == "EU":
                numeric = numeric.str.replace_all(
                    ".", "", literal=True).str.replace_all(",", ".", literal=True)
            else:
                numeric = numeric.str.replace_all(",", "", literal=True)
            numeric = numeric.cast(pl.Float64, strict=False)
            if target == int:
                numeric = numeric.round(0).cast(pl.Int64, strict=False)
            cast_exprs.append(numeric.alias(col))

        elif isinstance(target, str) and target.lower() in ("pct", "percent", "percentage"):
            numeric = (
                pl.col(col).str.extract(r"(-?[\d.,]+)")
                .str.replace_all(",", "", literal=True)
                .cast(pl.Float64, strict=False)
            )
            cast_exprs.append((numeric / 100.0).alias(col))

        elif isinstance(target, str) and target.lower() in ("datetime", "date", "timestamp"):
            cast_exprs.append(
                pl.col(col)
                .str.strptime(pl.Datetime("us", "UTC"), format=date_format, strict=False)
                .alias(col)
            )

    df = df.with_columns(cast_exprs) if cast_exprs else df

    # ── Step 3: infer types for columns not named in schema ───────────────
    infer_exprs = []
    for col in df.columns:
        if col in schema or df[col].dtype != pl.String:
            continue
        null_count = df[col].null_count()
        if df[col].cast(pl.Int64, strict=False).null_count() == null_count:
            infer_exprs.append(pl.col(col).cast(pl.Int64, strict=False))
        elif df[col].cast(pl.Float64, strict=False).null_count() == null_count:
            infer_exprs.append(pl.col(col).cast(pl.Float64, strict=False))
        # else: leave as cleaned string

    return df.with_columns(infer_exprs) if infer_exprs else df


if __name__ == "__main__":
    test_df = pl.DataFrame({
        "price":  ["  '1,234.56' ", "1,200.00"],
        "weight": ["7.89 %", "10%"],
        "date":   ["04/17/25", "03/01/26"],
        "name":   ["'Some text", "Nothing''"],
    })
    cleaned = clean(test_df, schema={
                    "price": float, "weight": "pct", "date": "datetime"})
    print(cleaned)
