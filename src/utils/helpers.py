import pandas as pd
from datetime import datetime
import holidays

# Base holiday calendars by region
BASE_HOLIDAYS = {
    "Ontario": list(holidays.CA(subdiv="ON", years=range(2020, 2051)).keys()),
    "US": list(holidays.US(years=range(2020, 2051)).keys()),
    "AU": list(holidays.AU(years=range(2020, 2051)).keys()),
}


def business_day_offset(anchor: datetime, days: int, region="Ontario", custom_holidays=None) -> datetime:
    """
    Offset a datetime by x business days.

    Args:
        anchor: Starting datetime
        days: Number of business days to offset
        region: Holiday calendar region(s) - string or list of strings
                ("Ontario", "US", "AU", or ["Ontario", "US"] for multiple)
        custom_holidays: List of additional holiday dates to exclude
    """
    # Handle single region or list of regions
    regions = [region] if isinstance(region, str) else region

    # Merge holidays from all specified regions
    all_holidays = []
    for r in regions:
        all_holidays.extend(BASE_HOLIDAYS.get(r, BASE_HOLIDAYS["Ontario"]))

    # Remove duplicates
    all_holidays = list(set(all_holidays))

    # Add custom holidays if provided
    if custom_holidays:
        custom_dates = pd.to_datetime(custom_holidays)
        all_holidays.extend(custom_dates)

    bday = pd.offsets.CustomBusinessDay(holidays=all_holidays)
    result = pd.to_datetime(anchor) + (days * bday)
    return result.to_pydatetime()


if __name__ == "__main__":
    # Example usage
    anchor_date = pd.Timestamp.now(tz="US/Eastern")
    offset_date = business_day_offset(anchor_date, -50, region="US")
    print(f"Anchor date: {anchor_date},\n"
          f"Offset date: {offset_date}")
