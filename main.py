"""
Main entry point: Configure and run the ingestion pipeline.
Orchestrates file discovery, parsing, and loading to Bronze layer.
Silver transformations applied directly to the parsed DataFrame.
"""
from datetime import datetime
from pathlib import Path
from typing import Optional
from uuid import uuid4
import re
import pandas as pd
import duckdb
import deltalake

from src.deltalake_writer import ingest_into_bronze, upsert_silver
from src.cleaner import clean_and_cast
from src.utils.logger import get_logger
from config import INBOX_DIR, PROCESSED_DIR, FAILED_DIR, BRONZE_DIR, SILVER_DIR, INGESTION_MAPPINGS

logger = get_logger(__name__)


def main() -> None:
    """Run ETL pipeline: discover files, parse, ingest to bronze, then transform to silver."""
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

        if mapping.rename:
            new_name = f"{file_path.stem}_{datetime.now().strftime('%Y%m%d_%H%M')}{file_path.suffix}"
            new_path = file_path.parent / new_name
            file_path.rename(new_path)
            file_path = new_path

        try:
            parsed_df = _parse_file(file_path, mapping)
            target_path = BRONZE_DIR / mapping.bronze_table
            ingest_into_bronze(
                df=parsed_df,
                source_name=file_path.name,
                target_path=target_path,
                batch_id=batch_id,
                current_time=pd.Timestamp.now(tz="America/New_York"),
                write_mode=mapping.load_type
            )

            bronze_ingestion_successful = True

            # 2) Silver transformation: only if bronze ingestion succeeded
            if bronze_ingestion_successful and mapping.silver_mapping:
                try:
                    # TEMPORARY: When load_type="append", read full bronze table
                    # When load_type="overwrite", use only parsed_df
                    if mapping.load_type == "append":
                        bronze_path = BRONZE_DIR / mapping.bronze_table
                        delta_log = bronze_path / "_delta_log"
                        if delta_log.exists():
                            logger.info(
                                "Reading full bronze table for append mode | table=%s", mapping.bronze_table)
                            transform_df = deltalake.DeltaTable(
                                str(bronze_path)).to_pandas()
                        else:
                            logger.info(
                                "Bronze table not yet created, using parsed_df | table=%s", mapping.bronze_table)
                            transform_df = parsed_df
                    else:
                        # overwrite mode: use only parsed_df (latest version)
                        logger.info(
                            "Using parsed_df for overwrite mode | table=%s", mapping.bronze_table)
                        transform_df = parsed_df

                    cleaned_df = clean_and_cast(
                        transform_df, mapping.silver_mapping)
                    silver_path = SILVER_DIR / mapping.silver_mapping.silver_table_name
                    upsert_silver(
                        cleaned_df=cleaned_df,
                        silver_mapping=mapping.silver_mapping,
                        silver_path=silver_path
                    )
                    logger.info(
                        "Upserted to silver | table=%s | silver_table=%s | rows=%s",
                        mapping.bronze_table, mapping.silver_mapping.silver_table_name, len(
                            cleaned_df)
                    )
                except Exception as e:
                    logger.error(
                        "Silver upsert failed | table=%s | error=%s",
                        mapping.bronze_table, str(e), exc_info=True
                    )

            # File successfully processed (even if silver upsert failed, bronze succeeded)
            _move_to_processed(file_path)

        except Exception as e:
            logger.error("Bronze ingestion failed | batch_id=%s | file=%s | error=%s",
                         batch_id, file_path.name, str(e))
            _move_to_failed(file_path)


def _get_mapping(filename: str):
    """
    Find mapping for a given filename by pattern matching.

    Supports regex patterns (re.Pattern).

    Args:
        filename: Name of the file to match against patterns.

    Returns:
        IngestionMapping object if found, None otherwise.
    """
    for pattern, mapping in INGESTION_MAPPINGS.items():
        # Handle compiled regex patterns
        if isinstance(pattern, re.Pattern):
            if pattern.search(filename):
                return mapping
    return None


def _parse_file(file_path: Path, mapping):
    """
    Parse file using the specified parser from IngestionMapping.

    Args:
        file_path: Path to the file to parse.
        mapping: IngestionMapping with parser function.
    """
    if not mapping.parser:
        raise ValueError(
            f"Parser not defined in mapping for file {file_path.name}")
    return mapping.parser(file_path)


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


def process_bronze_to_silver() -> None:
    """
    Process existing bronze tables that have silver mappings configured.
    Reads bronze data and applies transformations directly without requiring new inbox files.
    Useful for testing silver logic independently.
    """
    logger.info(
        "Starting bronze-to-silver transformation for all configured tables")

    # Find all mappings that have silver transformations defined
    silver_mappings = [
        (mapping.bronze_table, mapping.silver_mapping)
        for mapping in INGESTION_MAPPINGS.values()
        if mapping.silver_mapping
    ]

    if not silver_mappings:
        logger.warning("No bronze tables with silver mappings configured")
        return

    logger.info("Found %d tables with silver mappings", len(silver_mappings))

    for bronze_table_name, silver_mapping in silver_mappings:
        try:
            bronze_path = BRONZE_DIR / bronze_table_name

            # Check if bronze table exists
            delta_log = bronze_path / "_delta_log"
            if not delta_log.exists():
                logger.warning("Bronze table not found | table=%s | path=%s",
                               bronze_table_name, bronze_path)
                continue

            logger.info("Reading bronze table | table=%s", bronze_table_name)
            transform_df = deltalake.DeltaTable(str(bronze_path)).to_pandas()

            if transform_df.empty:
                logger.warning(
                    "Bronze table is empty | table=%s", bronze_table_name)
                continue

            logger.info("Transforming and upserting to silver | bronze_table=%s | rows=%s",
                        bronze_table_name, len(transform_df))

            cleaned_df = clean_and_cast(transform_df, silver_mapping)
            silver_path = SILVER_DIR / silver_mapping.silver_table_name
            upsert_silver(
                cleaned_df=cleaned_df,
                silver_mapping=silver_mapping,
                silver_path=silver_path
            )

            logger.info(
                "[SUCCESS] Upserted to silver | bronze_table=%s | silver_table=%s | rows=%s",
                bronze_table_name, silver_mapping.silver_table_name, len(
                    cleaned_df)
            )

        except Exception as e:
            logger.error(
                "[FAILED] Silver upsert failed | table=%s | error=%s",
                bronze_table_name, str(e), exc_info=True
            )


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--silver":
        logger.info("Mode: Bronze-to-Silver transformation")
        process_bronze_to_silver()
    else:
        logger.info("Mode: Full ETL pipeline (inbox -> bronze -> silver)")
        main()
