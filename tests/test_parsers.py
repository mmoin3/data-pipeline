"""Test file for parser development. Run from project root."""

from src.parsers.baskets_parser import BasketsParser
from pathlib import Path

if __name__ == "__main__":
    file_path = Path("data/0_raw data/Harvest_INAVBSKT_ALL.20260227.CSV")
    
    try:
        parser = BasketsParser(file_path)
        metrics, holdings = parser.parse()
        print("Metrics:")
        print(metrics.head())
        print("\nHoldings:")
        print(holdings.head())
    except Exception as e:
        print(f"Error: {e}")
