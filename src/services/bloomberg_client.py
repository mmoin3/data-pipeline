import pandas as pd
import numpy as np
import re
from blpapi import Session, SessionOptions, Name, Event
from typing import Sequence
from datetime import datetime


def _parse_element(elem):
    """Extract value from Bloomberg element based on its type."""
    try:
        if elem.isNull():
            return None
        elif elem.isDatetime():
            return elem.getValueAsDatetime()
        elif elem.isDate():
            return elem.getValueAsDate()
        elif elem.isTime():
            return elem.getValueAsTime()
        elif elem.isFloat():
            return elem.getValueAsFloat()
        elif elem.isInt():
            return elem.getValueAsInt()
        elif elem.isBoolean():
            return elem.getValueAsBoolean()
        else:
            # For strings and complex types, extract the value part
            val_str = elem.getValueAsString()
            # Remove trailing newlines and quotes
            val_str = val_str.strip().strip('"')
            return val_str
    except:
        val_str = str(elem).strip()
        # Parse "FieldName = value\n" format
        if '=' in val_str:
            val_str = val_str.split('=', 1)[1].strip().strip('"').strip()
        return val_str


class BloombergClient:
    """Bloomberg API client for retrieving historical and reference data.

    Use as context manager:
        with BloombergClient() as client:
            df = client.BDP(["AAPL US Equity"], ["NAME", "SECTOR"])
    """

    def __init__(self, host: str = "localhost", port: int = 8194):
        """Initialize and connect to Bloomberg API.

        Args:
            host: Bloomberg server hostname (default: localhost)
            port: Bloomberg server port (default: 8194)
        """
        options = SessionOptions()
        options.setServerHost(host)
        options.setServerPort(port)
        self.session = Session(options)
        self.session.start()
        self.session.openService("//blp/refdata")

        # Wait for service to be ready
        while True:
            event = self.session.nextEvent()
            if event.eventType() == Event.SERVICE_STATUS:
                break

    def __enter__(self):
        """Context manager entry. Returns self for use in 'with' statement."""
        return self

    def close(self):
        """Stop Bloomberg session."""
        if self.session:
            self.session.stop()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close session on context manager exit."""
        self.close()

    def BDH(self, tickers: Sequence[str], fields: Sequence[str], start_date: str, end_date: str, frequency: str = "DAILY") -> pd.DataFrame:
        """Fetch historical data for tickers between dates.

        Args:
            tickers: Security identifiers (e.g., ["AAPL US Equity"])
            fields: Data fields (e.g., ["PX_LAST", "VOLUME"])
            start_date: Start date YYYYMMDD format
            end_date: End date YYYYMMDD format
            frequency: DAILY, WEEKLY, MONTHLY, QUARTERLY, YEARLY
        Returns:
            DataFrame with columns [Ticker, Date, *fields]
        """
        service = self.session.getService("//blp/refdata")
        request = service.createRequest("HistoricalDataRequest")

        for ticker in tickers:
            request.getElement("securities").appendValue(ticker)
        for field in fields:
            request.getElement("fields").appendValue(field)

        request.set("startDate", start_date)
        request.set("endDate", end_date)
        request.set("periodicitySelection", frequency)

        self.session.sendRequest(request)

        rows = []
        while True:
            event = self.session.nextEvent()
            for msg in event:
                if msg.messageType() != Name("HistoricalDataResponse"):
                    continue
                security_data = msg.getElement("securityData")
                ticker = security_data.getElementAsString("security")
                field_data = security_data.getElement("fieldData")

                for i in range(field_data.numValues()):
                    entry = field_data.getValueAsElement(i)
                    row = {"Ticker": ticker,
                           "Date": entry.getElementAsDatetime("date")}
                    for field in fields:
                        row[field] = entry.getElementAsFloat(
                            field) if entry.hasElement(field) else np.nan
                    rows.append(row)

            if event.eventType() == Event.RESPONSE:
                break

        df = pd.DataFrame(rows) if rows else pd.DataFrame()
        df.columns = df.columns.astype(str)
        return df

    def BDP(self, tickers: Sequence[str], fields: Sequence[str]) -> pd.DataFrame:
        """Fetch reference data for tickers.

        Args:
            tickers: Security identifiers (e.g., ["AAPL US Equity"])
            fields: Data fields (e.g., ["NAME", "SECTOR", "MARKET_CAP"])

        Returns:
            DataFrame with columns [Ticker, *fields]
        """
        service = self.session.getService("//blp/refdata")
        request = service.createRequest("ReferenceDataRequest")

        for ticker in tickers:
            request.getElement("securities").appendValue(ticker)
        for field in fields:
            request.getElement("fields").appendValue(field)

        self.session.sendRequest(request)

        rows = []
        while True:
            event = self.session.nextEvent()
            for msg in event:
                if msg.messageType() != Name("ReferenceDataResponse"):
                    continue
                security_data_array = msg.getElement("securityData")
                for i in range(security_data_array.numValues()):
                    security_data = security_data_array.getValueAsElement(i)
                    ticker = security_data.getElementAsString("security")
                    field_data = security_data.getElement("fieldData")

                    row = {"Ticker": ticker}
                    for field in fields:
                        if field_data.hasElement(field):
                            row[field] = _parse_element(
                                field_data.getElement(field))
                        else:
                            row[field] = None
                    rows.append(row)

            if event.eventType() == Event.RESPONSE:
                break

        df = pd.DataFrame(rows) if rows else pd.DataFrame()
        df.columns = df.columns.astype(str)
        return df

    def BDS(self, ticker: str, field: str) -> pd.DataFrame:
        """Fetch bulk data for a ticker and field.

        Args:
            ticker: Security identifier (e.g., "HHIS CN Equity")
            field: Bulk data field (e.g., "DVD_HIST_ALL")

        Returns:
            DataFrame with bulk data rows
        """
        service = self.session.getService("//blp/refdata")
        request = service.createRequest("ReferenceDataRequest")
        request.getElement("securities").appendValue(ticker)
        request.getElement("fields").appendValue(field)
        self.session.sendRequest(request)

        rows = []
        while True:
            event = self.session.nextEvent()
            for msg in event:
                if msg.messageType() != Name("ReferenceDataResponse"):
                    continue

                security_data = msg.getElement(
                    "securityData").getValueAsElement(0)
                field_data = security_data.getElement("fieldData")

                if field_data.hasElement(field):
                    bulk_data = field_data.getElement(field)
                    for i in range(bulk_data.numValues()):
                        entry = bulk_data.getValueAsElement(i)
                        row = {}
                        for j in range(entry.numElements()):
                            elem = entry.getElement(j)
                            row[elem.name()] = elem.getValueAsString()
                        rows.append(row)

            if event.eventType() == Event.RESPONSE:
                break

        df = pd.DataFrame(rows) if rows else pd.DataFrame()
        # Convert blpapi.Name objects to strings
        df.columns = df.columns.astype(str)
        return df


if __name__ == "__main__":
    with BloombergClient() as client:
        # Historical Data
        hist = client.BDH(["HTAE CN Equity"], ["PX_LAST", "FUND_NET_ASSET_VAL"],
                          "20260101", "20260430", frequency="MONTHLY")
        print("\nHistorical Data:\n")
        print(hist.head())

        # Reference Data
        ref = client.BDP(["AAPL US Equity"], ["NAME", "GICS_SECTOR_NAME"])
        print("\nReference Data:\n")
        print(ref.head())

        # Bulk Data
        bulk = client.BDS("HPYE CN Equity", "DVD_HIST_ALL")
        bulk['Ticker'] = "HPYE CN Equity"
        print("\nBulk Data:\n")
        print(bulk.head(20))
