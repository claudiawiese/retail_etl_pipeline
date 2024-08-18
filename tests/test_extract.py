import unittest
import sqlite3
import sys
import os
from common_test_utilities import CommonTestUtilities

# Add the `etl_workflow` directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../etl_workflow')))

from extract import SQLiteExtractor

class TestSQLiteExtractor(CommonTestUtilities):
    def setUp(self):
        self.setUpConfig()
        self.extractor = SQLiteExtractor(self.config)
       
    def test_extract_date_from_filename(self):
        date = self.extractor.extract_date_from_filename('test_data_15_01_2022.csv')
        self.assertEqual(date.year, 2022)
        self.assertEqual(date.month, 1)
        self.assertEqual(date.day, 15)

    def test_open_csv(self):
        rows = self.extractor.open_csv(self.config['CSV_FILE'])
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0][-1].year, 2022)  # Checking the date column

if __name__ == '__main__':
    unittest.main()