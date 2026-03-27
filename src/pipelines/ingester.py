import pandas as pd
import deltalake
from pathlib import Path
from config import BRONZE_DIR


def write_to_bronze(df: pd.DataFrame, target_table: str, load_type: str, file_path: Path) -> None:
    """Write DataFrame to Bronze layer as Delta Lake table.

    Args:
        df: DataFrame to write.
        target_table: Target table name (creates subdirectory in bronze/).
        load_type: "append" or "replace" mode.
        file_path: Source file path for tracking.
    """
    # Add tracking columns
    df["_source_file"] = file_path.name
    df["_load_type"] = load_type
    df["_ingested_at"] = pd.Timestamp.now(tz="US/Eastern")

    # Determine target path and write mode
    bronze_path = BRONZE_DIR / target_table
    bronze_path.mkdir(parents=True, exist_ok=True)

    # Write using deltalake
    if load_type == "replace":
        deltalake.write_deltalake(str(bronze_path), df, mode="overwrite")
    else:  # append
        deltalake.write_deltalake(str(bronze_path), df, mode="append")
