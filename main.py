"""
Main entry point: Configure and run the ingestion pipeline.
Orchestrates file discovery, parsing, and loading to Bronze layer.
Silver and Gold transformations handled separately in DuckDB.
"""
from datetime import datetime
from pathlib import Path
from typing import Optional
from uuid import uuid4
import pandas as pd
import duckdb

from src.ingestor import ingest_into_bronze, upsert_into_silver
from src.utils.logger import get_logger
from config import INBOX_DIR, PROCESSED_DIR, FAILED_DIR, BRONZE_DIR, SILVER_DIR, INGESTION_MAPPINGS, SILVER_MAPPINGS

logger = get_logger(__name__)


def main() -> None:
    """Run ETL pipeline: discover files, parse, and ingest to bronze. Then transform to silver."""
    batch_id = f"{uuid4()}"

    # 1) Bronze ingestion
    for file_path in INBOX_DIR.iterdir():
        if not file_path.is_file():
            continue

        mapping = _get_mapping(file_path.name)
        if not mapping:
            logger.warning("No mapping found | batch=%s | file=%s",
                           batch_id, file_path.name)
            _move_to_failed(file_path)
            continue

        if mapping.get("rename", False):
            new_name = f"{file_path.stem}_{datetime.now().strftime('%Y%m%d_%H%M')}{file_path.suffix}"
            new_path = file_path.parent / new_name
            file_path.rename(new_path)
            file_path = new_path

        try:
            parsed_df = _parse_file(file_path, mapping)
            target_path = BRONZE_DIR / mapping.get("bronze_table")
            ingest_into_bronze(
                df=parsed_df,
                source_name=file_path.name,
                target_path=target_path,
                batch_id=batch_id,
                current_time=pd.Timestamp.now(tz="US/Eastern"),
                write_mode=mapping.get("load_type", "append")
            )
            _move_to_processed(file_path)
        except Exception as e:
            logger.error("Bronze ingestion failed | batch_id=%s | file=%s | error=%s",
                         batch_id, file_path.name, str(e))
            _move_to_failed(file_path)

    # 2) Silver transformations
    for table_name, mapping in SILVER_MAPPINGS.items():
        transform_fn = mapping.get("transform_fn")
        if not transform_fn:
            logger.warning(f"No transform_fn for {table_name}")
            continue

        merge_keys = mapping.get("merge_keys")
        try:
            conn = duckdb.connect()
            bronze_path = BRONZE_DIR / table_name
            bronze_relation = conn.delta_scan(bronze_path)
            conn.create_temp_view("bronze", bronze_relation)
            conn.create_table("transformed", conn.execute(transform_fn()))
            row_count = conn.execute(
                "SELECT COUNT(*) FROM transformed").fetchone()[0]

            upsert_into_silver(
                silver_table_name=table_name,
                conn=conn,
                silver_path=SILVER_DIR / table_name,
                merge_keys=merge_keys,
            )

            conn.close()
            logger.info(f"{table_name} → silver ({row_count} rows)")

        except Exception as e:
            logger.error(
                f"Silver transform failed | table={table_name} | error={e}")


def _get_mapping(filename: str) -> Optional[dict]:
    """
    Find mapping for a given filename by pattern matching.

    Args:
        filename: Name of the file to match against patterns.
    """
    for pattern, mapping in INGESTION_MAPPINGS.items():
        if pattern in filename:
            return mapping
    return None


def _parse_file(file_path: Path, mapping: dict):
    """
    Parse file using the specified parser.

    Args:
        file_path: Path to the file to parse.
        mapping: Configuration mapping with parser name.
    """
    parser_name = mapping.get("parser")
    if not parser_name:
        raise ValueError(
            f"Parser not defined in mapping for file {file_path.name}")
    return parser_name(file_path)


def _move_to_processed(file_path: Path) -> None:
    """
    Move file to processed folder after successful bronze ingestion.

    Args:
        file_path: Path to the file to move.
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    # Organize processed files into year/month/day subdirectories
    now = pd.Timestamp.now(tz="US/Eastern")
    subdir = PROCESSED_DIR / f"{now.year}" / \
        f"{now.strftime('%B')}" / f"{now.day:02d}"
    subdir.mkdir(parents=True, exist_ok=True)
    new_path = subdir / file_path.name
    file_path.rename(new_path)


def _move_to_failed(file_path: Path) -> None:
    """
    Move file to failed folder if ingestion fails.

    Args:
        file_path: Path to the file to move.
    """
    FAILED_DIR.mkdir(parents=True, exist_ok=True)
    now = pd.Timestamp.now(tz="US/Eastern")
    # format month as name instead of number
    subdir = FAILED_DIR
    subdir.mkdir(parents=True, exist_ok=True)
    new_path = subdir / file_path.name
    file_path.rename(new_path)


if __name__ == "__main__":
    main()
