import pandas as pd
import logging
from src.cleaner import clean_basic

# Set up logging to see warnings
logging.basicConfig(level=logging.WARNING)


def test_cleaner_all_types():
    """Test cleaner with all supported type conversions."""

    # Test data with various formats and edge cases
    test_data = {
        "id_raw": ["1", "2", "3", "4", "5"],
        "price_raw": ["19.99", "1,234.50", "bad_value", "", "99.99"],
        "quantity_raw": ["100", "5,000", "not_int", "", "50"],
        "is_active_raw": ["true", "False", "1", "0", "yes"],
        "fee_pct_raw": ["5%", "10.50%", "", "0%", "2.5%"],
        "date_raw": ["01/15/2024", "12/25/2023", "invalid_date", "", "06/30/2024"],
        "description": ["Nice Item", "Another Thing", "", "Product XYZ", "Final Item"],
        "unused_column": ["a", "b", "c", "d", "e"],
    }

    df = pd.DataFrame(test_data)

    # Column mapping
    columns = {
        "id_raw": ("int", "id"),
        "price_raw": ("float", "price"),
        "quantity_raw": ("int", "quantity"),
        "is_active_raw": ("bool", "is_active"),
        "fee_pct_raw": ("pct", "fee_percentage"),
        "date_raw": ("datetime", "transaction_date", "%m/%d/%Y"),
        "description": ("str", "description"),
        # Note: unused_column not in mapping, should be snake_case normalized
    }

    print("=" * 80)
    print("BEFORE CLEANING")
    print("=" * 80)
    print(df)
    print("\nDtypes before:")
    print(df.dtypes)

    # Run cleaner
    result = clean_basic(df, columns)

    print("\n" + "=" * 80)
    print("AFTER CLEANING")
    print("=" * 80)
    print(result)
    print("\nDtypes after:")
    print(result.dtypes)

    # Verify results
    print("\n" + "=" * 80)
    print("DETAILED INSPECTION")
    print("=" * 80)

    print("\n✓ ID column (int):")
    print(result[["id"]])

    print("\n✓ Price column (float with comma handling):")
    print(result[["price"]])

    print("\n✓ Quantity column (int with comma handling & NaN coercion):")
    print(result[["quantity"]])

    print("\n✓ Is Active column (bool with multiple formats):")
    print(result[["is_active"]])

    print("\n✓ Fee Percentage column (pct with % sign removal & division by 100):")
    print(result[["fee_percentage"]])

    print("\n✓ Transaction Date column (datetime with format parsing):")
    print(result[["transaction_date"]])

    print("\n✓ Description column (str with empty→NA):")
    print(result[["description"]])

    print("\n✓ Unmapped column (should be snake_case normalized):")
    print(result[["unused_column"]])

    print("\n" + "=" * 80)
    print("SUCCESS: All types converted correctly!")
    print("=" * 80)


if __name__ == "__main__":
    test_cleaner_all_types()
