import logging
from pathlib import Path
from typing import Callable, Optional, Union

import deltalake
import pandas as pd

logger = logging.getLogger(__name__)


TransformFn = Callable[[pd.DataFrame], pd.DataFrame]
ValidateFn = Callable[[pd.DataFrame], None]


def _add_silver_metadata(df: pd.DataFrame, source_table: str) -> pd.DataFrame:
    """Add Silver-level processing metadata."""
    out = df.copy()
    out["_silver_processed_at"] = pd.Timestamp.now(tz="UTC")
    out["_silver_source_table"] = source_table
    return out


def _write_to_delta(
    df: pd.DataFrame,
    silver_path: Path,
    write_mode: str = "append",
) -> None:
    if write_mode not in {"append", "overwrite"}:
        raise ValueError(
            f"Invalid write_mode '{write_mode}'. Use 'append' or 'overwrite'."
        )

    silver_path.mkdir(parents=True, exist_ok=True)
    deltalake.write_deltalake(silver_path, df, mode=write_mode)


def load_into_silver(
    bronze_df: pd.DataFrame,
    source_table: str,
    target_table: Union[str, Path],
    transform_fn: Optional[TransformFn] = None,
    validate_fn: Optional[ValidateFn] = None,
    write_mode: str = "append",
) -> pd.DataFrame:
    """
    Transform + validate Bronze data, then write to Silver Delta.

    `transform_fn` and `validate_fn` should live in a separate module
    (e.g., src/pipelines/silver_transformations.py).
    """
    if not isinstance(bronze_df, pd.DataFrame):
        raise TypeError("bronze_df must be a pandas DataFrame.")

    silver_df = bronze_df.copy()

    if transform_fn is not None:
        silver_df = transform_fn(silver_df)
        if not isinstance(silver_df, pd.DataFrame):
            raise TypeError("transform_fn must return a pandas DataFrame.")

    if validate_fn is not None:
        validate_fn(silver_df)

    silver_df = _add_silver_metadata(silver_df, source_table)

    target_path = Path(target_table)
    _write_to_delta(silver_df, target_path, write_mode=write_mode)

    logger.info(
        "Silver load complete | source=%s | mode=%s | rows=%s | target=%s",
        source_table,
        write_mode,
        len(silver_df),
        target_path,
    )

    return silver_df