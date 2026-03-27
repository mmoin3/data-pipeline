"""
Main entry point: Configure and run the ingestion pipeline.
Orchestrates file discovery, parsing, and loading.
"""
from pathlib import Path
import pandas as pd

from src.parsers.base_parser import BaseParser
from src.parsers.baskets_parser import BasketsParser
from src.pipelines.ingester import write_to_bronze
from config import INBOX_DIR, INGESTION_MAPPINGS
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Map parser class names to actual classes
PARSERS_MAP = {
    "BaseParser": BaseParser,
    "BasketsParser": BasketsParser
}


def main():
    """Run ingestion pipeline: discover files -> parse -> load to bronze."""
    for file_path in INBOX_DIR.iterdir():
        if not file_path.is_file():
            continue

        logger.info(f"Processing file: {file_path.name}")

        try:
            mapping = _get_file_mapping(file_path)
            parser_class = PARSERS_MAP[mapping["parser"]]
            parser = parser_class(file_path)
            parsed_data = parser.parse()

            # Load each parsed dataframe to its target table
            for df_key, target_table in mapping["outputs"].items():
                if df_key not in parsed_data:
                    logger.warning(
                        f"Expected key '{df_key}' not found in parsed data for {file_path.name}")
                    continue

                df = parsed_data[df_key]
                load_type = mapping["load_type"]
                write_to_bronze(df, target_table, load_type, file_path)

        except Exception as e:
            logger.error(f"Failed to process {file_path.name}: {e}")


def _get_file_mapping(file_path: Path) -> dict:
    """Get ingestion mapping for file based on filename.

    Args:
        file_path: Path to the file.
    """
    filename = file_path.name
    for keyword, mapping in INGESTION_MAPPINGS.items():
        if keyword in filename:
            return mapping
    raise ValueError(f"No ingestion mapping found for file: {filename}")


if __name__ == "__main__":
    main()
