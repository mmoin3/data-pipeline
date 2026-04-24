import polars

from src.utils.logger import get_logger
from pathlib import Path
from typing import Union, Optional
from datetime import datetime

import deltalake
import duckdb
import pandas as pd
import polars as pl

logger = get_logger(__name__)


def ingest_into_bronze(
    df: pd.DataFrame,
        source_name: str,
        current_time: pd.Timestamp,
        target_path: Union[str, Path],
        batch_id: str,
        write_mode: str = "append",
        merge_schema: bool = True) -> pd.DataFrame:
    """
    Load a DataFrame to the bronze Delta table. Adds metadata columns
    and writes data in append or overwrite mode.

    Deduplicates exact row matches before ingestion.

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

    # Deduplicate exact row matches (all columns)
    rows_before = len(df)
    df = df.drop_duplicates(keep='first')
    rows_after = len(df)
    duplicates_removed = rows_before - rows_after
    if duplicates_removed > 0:
        logger.info(
            f"Removed {duplicates_removed} duplicate rows from {source_name} (kept {rows_after})")

    # Add metadata columns
    out = df.copy()
    out["ingested_at"] = current_time
    out["source_file"] = source_name
    out["batch_id"] = batch_id

    target_path = Path(target_path)
    target_path.mkdir(parents=True, exist_ok=True)

    # Enable schema evolution: "merge" allows new columns, "overwrite" replaces schema
    schema_mode = "merge" if merge_schema else "overwrite"
    deltalake.write_deltalake(
        target_path, out.reset_index(drop=True), mode=write_mode, schema_mode=schema_mode)
    logger.info(
        "Loaded into bronze | batch=%s | source=%s | mode=%s | schema_mode=%s | rows=%s | target=%s",
        batch_id, source_name, write_mode, schema_mode, len(out), target_path
    )
    return out


def upsert_silver(
    cleaned_df: pd.DataFrame,
    silver_mapping,
    silver_path: Union[str, Path],
) -> None:
    """
    Unified SCD2 merge pattern for all tables (fact and reference).

    Uses is_current flag to track active records and prevent duplicates:
    - Deduplicates within incoming batch (on PK + timestamp)
    - Checks against existing table: skips identical records, updates changed ones
    - Handles NULL primary keys adaptively:
      * Full composite key if all PK cols non-NULL
      * Partial key if some PK cols NULL
      * Skip if all PK cols NULL

    Args:
        cleaned_df: Cleaned pandas DataFrame with silver column names.
        silver_mapping: SilverMapping config with table_type, primary_keys, dedup_timestamp.
        silver_path: Path to silver delta table directory.
    """
    silver_path = Path(silver_path)
    silver_path.mkdir(parents=True, exist_ok=True)

    pk_cols = list(silver_mapping.primary_keys)
    ts_col = silver_mapping.dedup_timestamp

    # Separate rows by null patterns in primary keys
    # Group 1: Rows with ALL keys non-NULL (use full composite key)
    full_key_mask = cleaned_df[pk_cols].notna().all(axis=1)
    full_key_df = cleaned_df[full_key_mask].copy()

    # Group 2: Rows with SOME keys NULL (use adaptive partial keys)
    partial_key_mask = ~full_key_mask & cleaned_df[pk_cols].notna().any(axis=1)
    partial_key_df = cleaned_df[partial_key_mask].copy()

    # Group 3: Rows with ALL keys NULL (skip)
    all_null_mask = cleaned_df[pk_cols].isnull().all(axis=1)
    if all_null_mask.any():
        logger.warning(
            f"Skipping {all_null_mask.sum()} rows with NULL in ALL primary keys "
            f"for {silver_mapping.silver_table_name}"
        )

    # Process full-key rows
    processed_df_list = []
    if len(full_key_df) > 0:
        dedup_full = full_key_df.sort_values(
            by=ts_col, ascending=False).drop_duplicates(subset=pk_cols, keep="first")
        processed_df_list.append(dedup_full)
        logger.info(
            f"Processing {len(dedup_full)} rows with full composite key")

    # Process partial-key rows (adaptive dedup per null pattern)
    if len(partial_key_df) > 0:
        # Create null pattern identifier: convert boolean array to tuple of strings for grouping
        partial_key_df = partial_key_df.copy()
        partial_key_df['_null_pattern'] = partial_key_df[pk_cols].isnull().apply(
            lambda row: tuple(row), axis=1)

        partial_groups = []
        for null_pattern, group in partial_key_df.groupby('_null_pattern'):
            active_keys = [pk_cols[i]
                           for i in range(len(pk_cols)) if not null_pattern[i]]
            if active_keys:
                dedup_group = group.drop(columns=['_null_pattern']).sort_values(
                    by=ts_col, ascending=False).drop_duplicates(subset=active_keys, keep="first")
                partial_groups.append(dedup_group)
                logger.info(
                    f"Processing {len(dedup_group)} rows with partial key {active_keys}"
                )
        if partial_groups:
            processed_df_list.append(
                pd.concat(partial_groups, ignore_index=True))

    if not processed_df_list:
        logger.warning(
            f"No valid records to upsert to {silver_mapping.silver_table_name}")
        return

    incoming_df = pd.concat(processed_df_list, ignore_index=True)

    delta_log = silver_path / "_delta_log"
    table_exists = delta_log.exists()

    if not table_exists:
        # First run: initialize table with is_current flag
        incoming_df["is_current"] = True
        incoming_df["is_current"] = incoming_df["is_current"].astype(bool)
        deltalake.write_deltalake(
            silver_path, incoming_df.reset_index(drop=True), mode="overwrite", schema_mode="overwrite")
        logger.info(
            f"Created {silver_mapping.table_type} table {silver_mapping.silver_table_name}: {len(incoming_df)} rows")
    else:
        # Merge upsert: deduplicate against existing table, apply SCD2 logic
        existing_df = deltalake.DeltaTable(silver_path).to_pandas()
        existing_df["is_current"] = existing_df["is_current"].astype(bool)

        # Identify which incoming records are truly new or changed
        # Merge on PK to find matches in existing table
        merge_keys = pk_cols
        incoming_with_flag = incoming_df.merge(
            existing_df[merge_keys + ["is_current"]
                        ].drop_duplicates(subset=merge_keys),
            on=merge_keys,
            how="left",
            indicator=True,
            suffixes=("", "_existing")
        )

        # Records to skip: identical record already exists (same PK + is_current=True)
        if "is_current_existing" in incoming_with_flag.columns:
            incoming_with_flag["_is_duplicate"] = (
                incoming_with_flag["is_current_existing"] == True
            )
            records_to_add = incoming_with_flag[
                incoming_with_flag["_is_duplicate"] == False
            ].drop(columns=["_is_duplicate", "is_current_existing", "_merge"])
            duplicates_skipped = incoming_with_flag["_is_duplicate"].sum()
        else:
            records_to_add = incoming_with_flag.drop(columns=["_merge"])
            duplicates_skipped = 0

        # Mark old versions of records being updated as not current
        pks_being_updated = records_to_add[merge_keys].drop_duplicates()
        existing_df = existing_df.merge(
            pks_being_updated.assign(_is_updated=True),
            on=merge_keys,
            how="left"
        )
        existing_df.loc[
            (existing_df["_is_updated"] == True) & (
                existing_df["is_current"] == True),
            "is_current"
        ] = False
        existing_df = existing_df.drop(columns=["_is_updated"])

        # Add new records with is_current=True
        records_to_add["is_current"] = True
        records_to_add = records_to_add.astype(
            {col: existing_df[col].dtype for col in records_to_add.columns if col in existing_df.columns})

        result_df = pd.concat([existing_df, records_to_add], ignore_index=True)
        result_df["is_current"] = result_df["is_current"].astype(bool)

        deltalake.write_deltalake(
            silver_path, result_df.reset_index(drop=True), mode="overwrite", schema_mode="merge")

        logger.info(
            f"Upserted {silver_mapping.table_type} {silver_mapping.silver_table_name}: "
            f"{len(records_to_add)} new/updated rows, {duplicates_skipped} duplicates skipped"
        )
