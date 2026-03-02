import pandas as pd
from collections.abc import Mapping
from typing import Dict, Optional
from config.settings import NULL_LIKE_VALUES, BOOLEAN_MAP

class DataFrameCleaner:
    def clean(self, df: pd.DataFrame, schema: Optional[Dict] = None) -> pd.DataFrame:
        """Clean and typecast a DataFrame using an optional schema.

        If a column is not mapped in schema, infer and apply a best-effort dtype.
        """
        if schema is not None and not isinstance(schema, Mapping):
            raise TypeError("schema must be a mapping of column name -> target type")

        out = self._coerce_to_dataframe(df)
        effective_schema = schema or {}

        for col in out.columns:
            target_type = effective_schema.get(col)
            if target_type is not None:
                out[col] = self._cast_with_schema(out[col], target_type)
                continue
            out[col] = self._infer_and_clean_column(out[col])

        return out

    def _cast_with_schema(self, series: pd.Series, target_type: object) -> pd.Series:
        normalized_text = self._normalize_text(series)

        if self._is_datetime_target(target_type):
            return self._normalize_datetime(normalized_text)
        if target_type == float:
            return pd.to_numeric(self._normalize_numeric(series), errors="coerce")
        if target_type == int:
            numeric = pd.to_numeric(self._normalize_numeric(series), errors="coerce")
            return numeric.round().astype("Int64")
        if target_type == bool:
            return self._normalize_boolean(normalized_text)
        if target_type == str:
            return normalized_text.astype("string")

        return series

    def _infer_and_clean_column(self, series: pd.Series) -> pd.Series:
        normalized_text = self._normalize_text(series)
        valid_values = normalized_text.dropna()
        if valid_values.empty:
            return normalized_text.astype("string")

        boolean_series = self._normalize_boolean(normalized_text)
        if boolean_series[valid_values.index].notna().all():
            return boolean_series

        datetime_series = self._normalize_datetime(normalized_text)
        if datetime_series[valid_values.index].notna().all():
            return datetime_series

        numeric_series = pd.to_numeric(self._normalize_numeric(series), errors="coerce")
        if numeric_series[valid_values.index].notna().all():
            return self._to_best_numeric_dtype(numeric_series)

        return normalized_text.astype("string")

    def _to_best_numeric_dtype(self, numeric_series: pd.Series) -> pd.Series:
        non_null = numeric_series.dropna()
        if non_null.empty:
            return numeric_series

        is_integer_like = ((non_null % 1) == 0).all()
        if is_integer_like:
            return numeric_series.round().astype("Int64")
        return numeric_series

    def _coerce_to_dataframe(self, data) -> pd.DataFrame:
        if isinstance(data, pd.DataFrame):
            return data.copy()

        if isinstance(data, list):
            if not data:
                return pd.DataFrame()

            if all(isinstance(item, pd.DataFrame) for item in data):
                return pd.concat(data, ignore_index=True, sort=False).copy()

        try:
            return pd.DataFrame(data).copy()
        except Exception as error:
            raise TypeError("clean expects a pandas DataFrame or DataFrame-like input") from error

    def _normalize_text(self, series: pd.Series) -> pd.Series:
        return series.astype("string").str.strip().str.lstrip("'").replace(NULL_LIKE_VALUES, pd.NA)

    def _normalize_numeric(self, series: pd.Series) -> pd.Series:
        return (
            self._normalize_text(series)
            .str.replace(",", "", regex=False)
            .str.replace("$", "", regex=False)
            .str.replace("%", "", regex=False)
        )

    def _normalize_boolean(self, series: pd.Series) -> pd.Series:
        return series.str.upper().map(self.BOOLEAN_MAP).astype("boolean")

    def _is_datetime_target(self, target_type: object) -> bool:
        normalized = str(target_type).strip().lower()
        return normalized in {"datetime64[ns]", "datetime64", "datetime", "timestamp", "date"}

    def _normalize_datetime(self, series: pd.Series) -> pd.Series:
        text = self._normalize_text(series)
        parsed = pd.Series(pd.NaT, index=text.index, dtype="datetime64[ns]")
        remaining_mask = text.notna()

        preferred_formats = ("%Y%m%d", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y")
        for fmt in preferred_formats:
            if not remaining_mask.any():
                break

            candidates = pd.to_datetime(text.where(remaining_mask), errors="coerce", format=fmt)
            success_mask = candidates.notna() & remaining_mask
            if success_mask.any():
                parsed.loc[success_mask] = candidates.loc[success_mask]
                remaining_mask = remaining_mask & ~success_mask

        if remaining_mask.any():
            fallback = text.where(remaining_mask).apply(
                lambda value: pd.to_datetime(value, errors="coerce")
            )
            fallback = pd.to_datetime(fallback, errors="coerce")
            success_mask = fallback.notna() & remaining_mask
            if success_mask.any():
                parsed.loc[success_mask] = fallback.loc[success_mask]

        return parsed


_DEFAULT_CLEANER = DataFrameCleaner()


def clean(df: pd.DataFrame, schema: Optional[Dict] = None) -> pd.DataFrame:
    return _DEFAULT_CLEANER.clean(df=df, schema=schema)