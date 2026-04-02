from src.utils.logger import get_logger
from pathlib import Path
from typing import Union

import deltalake
import pandas as pd

logger = get_logger(__name__)


def ingest(df: pd.DataFrame, source_name: str, current_time: pd.Timestamp,
           target_path: Union[str, Path], batch_id: str, write_mode: str = "append") -> pd.DataFrame:
    """
    Load a DataFrame to the bronze Delta table. Adds metadata columns
    and writes data in append or overwrite mode.

    Args:
        df:           Input DataFrame.
        source_name:  Source file name.
        current_time: Timestamp for ingestion.
        target_path:  Destination Delta table path.
        batch_id:     Unique identifier for this ingestion batch.
        write_mode:   "overwrite" or "append" (default: "append").
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame.")

    if write_mode not in ["overwrite", "append"]:
        raise ValueError("write_mode must be 'overwrite' or 'append'.")

    # Add metadata columns
    out = df.copy()
    out["ingested_at"] = current_time
    out["source_file"] = source_name
    out["batch_id"] = batch_id

    target_path = Path(target_path)
    target_path.mkdir(parents=True, exist_ok=True)

    deltalake.write_deltalake(target_path, out, mode=write_mode)
    logger.info(
        "Loaded into bronze | batch=%s | source=%s | mode=%s | rows=%s | target=%s",
        batch_id, source_name, write_mode, len(out), target_path
    )
    return out
