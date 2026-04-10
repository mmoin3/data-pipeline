import polars

from src.utils.logger import get_logger
from pathlib import Path
from typing import Union, Optional

import deltalake
import duckdb
import pandas as pd
import polars as pl

logger = get_logger(__name__)


def ingest(df: pd.DataFrame,
           source_name: str,
           current_time: pd.Timestamp,
           target_path: Union[str, Path],
           batch_id: str,
           write_mode: str = "append",
           merge_schema: bool = True) -> pd.DataFrame:
    """
    Load a DataFrame to the bronze Delta table. Adds metadata columns
    and writes data in append or overwrite mode.

    Args:
        df:            Input DataFrame.
        source_name:   Source file name.
        current_time:  Timestamp for ingestion.
        target_path:   Destination Delta table path.
        batch_id:      Unique identifier for this ingestion batch.
        write_mode:    "overwrite" or "append" (default: "append").
        merge_schema:  Allow new columns in incoming data (default: True).
    """
    if not isinstance(df, (pd.DataFrame, pl.DataFrame)):
        raise TypeError("df must be a pandas or polars DataFrame.")

    if isinstance(df, pl.DataFrame):
        df = df.to_pandas()

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


def upsert_to_silver(
    silver_table_name: str,
    con: duckdb.DuckDBPyConnection,
    silver_path: Path,
    merge_keys: list = None,
) -> None:
    """
    Write transformed data (in DuckDB connection) to silver delta table.
    Caller must have already created "transformed" view in the connection.

    Adds silver metadata (batch_id, cleaned_at, was_revised), then handles
    create-on-first-run or merge on subsequent runs (SCD2 tracking).

    Args:
        silver_table_name:  Name of the silver table.
        con:                DuckDB connection with "transformed" view ready.
        silver_path:        Path to silver delta table directory.
        merge_keys:         List of columns to upsert on. If None, overwrites.
    """
    # Prepare path
    silver_path = Path(silver_path)
    silver_path.mkdir(parents=True, exist_ok=True)

    # Wrap transformed with silver metadata
    wrapped_sql = """
        SELECT
            t.*,
            meta.batch_id,
            CURRENT_TIMESTAMP AS cleaned_at,
            FALSE AS was_revised
        FROM transformed AS t,
        (SELECT DISTINCT batch_id FROM bronze) AS meta
    """
    con.create_table("with_metadata", con.execute(wrapped_sql))

    # Upsert or overwrite
    if merge_keys:
        # Check if silver table exists (has delta log)
        delta_log_path = silver_path / "_delta_log"

        if not delta_log_path.exists():
            # First run: create table with transformed data
            con.execute(f"""
                CREATE TABLE delta_scan('{silver_path}') AS
                SELECT * FROM with_metadata
            """)
            write_mode = "create"
        else:
            # Subsequent runs: merge with SCD2 tracking
            on_clause = " AND ".join(
                [f"target.{k} = source.{k}" for k in merge_keys])
            con.execute(f"""
                MERGE INTO delta_scan('{silver_path}') AS target
                USING with_metadata AS source
                ON {on_clause}
                WHEN MATCHED THEN UPDATE SET was_revised = TRUE
                WHEN NOT MATCHED THEN INSERT *
            """)
            write_mode = "merge"
    else:
        con.execute(f"""
            CREATE OR REPLACE TABLE delta_scan('{silver_path}') AS
            SELECT * FROM with_metadata
        """)
        write_mode = "overwrite"

    logger.info(
        "Upserted to silver | table=%s | mode=%s | path=%s",
        silver_table_name, write_mode, silver_path
    )
