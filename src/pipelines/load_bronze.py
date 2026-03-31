import logging
from pathlib import Path
from typing import Union

import deltalake
import pandas as pd

logger = logging.getLogger(__name__)


def _add_bronze_metadata(df: pd.DataFrame, file_path: Path) -> pd.DataFrame:
    """Add standard Bronze metadata columns."""
    out = df.copy()
    ts = pd.Timestamp.now(tz="US/Eastern")

    out["_source_file"] = file_path.name
    out["_ingested_at"] = ts
    return out


def _write_to_delta(
    df: pd.DataFrame,
    bronze_path: Path,
    write_mode: str = "append",
) -> None:
    """Write DataFrame to Delta Lake."""
    if write_mode not in {"append", "overwrite"}:
        raise ValueError(
            f"Invalid write_mode '{write_mode}'. Use 'append' or 'overwrite'."
        )

    bronze_path.mkdir(parents=True, exist_ok=True)
    deltalake.write_deltalake(bronze_path, df, mode=write_mode)


def ingest_into_bronze(
    file_path: Path,
    parsed_df: pd.DataFrame,
    target_table: Union[str, Path],
    write_mode: str = "append",
) -> None:
    """
    Ingest a single parsed DataFrame into one Bronze Delta table.
    """
    if not isinstance(parsed_df, pd.DataFrame):
        raise TypeError("parsed_df must be a pandas DataFrame.")

    target_path = Path(target_table)
    bronze_df = _add_bronze_metadata(parsed_df, file_path)
    _write_to_delta(bronze_df, target_path, write_mode=write_mode)

    logger.info(
        "Bronze ingest complete | file=%s | mode=%s | rows=%s | target=%s",
        file_path.name,
        write_mode,
        len(bronze_df),
        target_path,
    )
