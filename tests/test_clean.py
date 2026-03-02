import unittest
import pandas as pd

from src.dataframe_cleaner import DataFrameCleaner

class TestDataFrameCleaner(unittest.TestCase):
	def setUp(self):
		self.cleaner = DataFrameCleaner()

	def test_clean_with_schema_casts_datetime_and_numeric(self):
		schema = {"TRADE_DATE": "datetime64[ns]", "NAV": float, "COUNT": int}
		dataframe = pd.DataFrame(
			[
				{"TRADE_DATE": "20260220", "NAV": "1,234.56", "COUNT": "10"},
				{"TRADE_DATE": "2026-02-21", "NAV": "$789.00", "COUNT": "20"},
			]
		)

		cleaned = self.cleaner.clean(dataframe, schema=schema)

		self.assertTrue(str(cleaned["TRADE_DATE"].dtype).startswith("datetime64"))
		self.assertEqual(cleaned["NAV"].tolist(), [1234.56, 789.0])
		self.assertEqual(str(cleaned["COUNT"].dtype), "Int64")
		self.assertEqual(cleaned["COUNT"].tolist(), [10, 20])

	def test_clean_without_schema_infers_types_and_normalizes_text(self):
		dataframe = pd.DataFrame(
			[
				{"date_col": "20260220", "num_col": "1,000", "name": "  'abc  "},
				{"date_col": "2026-02-21", "num_col": "2000", "name": "def"},
			]
		)

		cleaned = self.cleaner.clean(dataframe)

		self.assertTrue(str(cleaned["date_col"].dtype).startswith("datetime64"))
		self.assertEqual(str(cleaned["num_col"].dtype), "Int64")
		self.assertEqual(cleaned["num_col"].tolist(), [1000, 2000])
		self.assertEqual(str(cleaned["name"].dtype), "string")
		self.assertEqual(cleaned["name"].tolist(), ["abc", "def"])

	def test_clean_with_partial_schema_still_infers_unmapped_columns(self):
		schema = {"TRADE_DATE": "datetime64[ns]"}
		dataframe = pd.DataFrame(
			[
				{"TRADE_DATE": "2026-02-20", "UNMAPPED_NUM": "5", "UNMAPPED_TEXT": " x "},
				{"TRADE_DATE": "2026-02-21", "UNMAPPED_NUM": "10", "UNMAPPED_TEXT": "'y"},
			]
		)

		cleaned = self.cleaner.clean(dataframe, schema=schema)

		self.assertTrue(str(cleaned["TRADE_DATE"].dtype).startswith("datetime64"))
		self.assertEqual(str(cleaned["UNMAPPED_NUM"].dtype), "Int64")
		self.assertEqual(cleaned["UNMAPPED_NUM"].tolist(), [5, 10])
		self.assertEqual(cleaned["UNMAPPED_TEXT"].tolist(), ["x", "y"])


if __name__ == "__main__":
	unittest.main()
