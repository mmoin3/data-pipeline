import logging
from pathlib import Path
from typing import Callable, Literal, Optional, Union

import deltalake
import pandas as pd

logger = logging.getLogger(__name__)

Layer = Literal["bronze", "silver", "gold"]
TransformFn = Callable[[pd.DataFrame], pd.DataFrame]
ValidateFn = Callable[[pd.DataFrame], None]


def _add_metadata(
    df: pd.DataFrame,
    layer: Layer,
    source: str,
) -> pd.DataFrame:
    out = df.copy()
    ts = pd.Timestamp.now(tz="UTC")

    if layer == "bronze":
        out["_ingested_at"] = ts
        out["_source_file"] = source
    elif layer == "silver":
        out["_processed_at"] = ts
        out["_source_table"] = source
    elif layer == "gold":
        out["_aggregated_at"] = ts
        out["_source_table"] = source

    return out


def _write_to_delta(
    df: pd.DataFrame,
    target_path: Path,
    write_mode: str,
) -> None:
    if write_mode not in {"append", "overwrite"}:
        raise ValueError(
            f"Invalid write_mode '{write_mode}'. Use 'append' or 'overwrite'."
        )

    target_path.mkdir(parents=True, exist_ok=True)
    deltalake.write_deltalake(target_path, df, mode=write_mode)


def load_to_layer(
    df: pd.DataFrame,
    layer: Layer,
    source: str,
    target_table: Union[str, Path],
    transform_fn: Optional[TransformFn] = None,
    validate_fn: Optional[ValidateFn] = None,
    write_mode: str = "append",
) -> pd.DataFrame:
    """
    Enrich, transform, validate, and write a DataFrame to a medallion layer.

    Args:
        df:           Input DataFrame.
        layer:        Target medallion layer ("bronze", "silver", "gold").
        source:       Source file name (bronze) or source table name (silver/gold).
        target_table: Destination Delta table path.
        transform_fn: Optional transform, applied before validation (silver/gold).
        validate_fn:  Optional validation, raises on failure.
        write_mode:   "append" or "overwrite".
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame.")

    out = df.copy()

    if transform_fn is not None:
        out = transform_fn(out)
        if not isinstance(out, pd.DataFrame):
            raise TypeError("transform_fn must return a pandas DataFrame.")

    if validate_fn is not None:
        validate_fn(out)

    out = _add_metadata(out, layer, source)

    target_path = Path(target_table)
    _write_to_delta(out, target_path, write_mode)

    logger.info(
        "Load complete | layer=%s | source=%s | mode=%s | rows=%s | target=%s",
        layer,
        source,
        write_mode,
        len(out),
        target_path,
    )

    return out