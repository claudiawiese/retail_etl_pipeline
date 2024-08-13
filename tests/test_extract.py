import unittest
import sqlite3
import sys
import os

# Add the `etl_workflow` directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../etl_workflow')))

from extract import SQLiteExtractor
import os

class TestSQLiteExtractor(unittest.TestCase):
   
    def setUp(self):
        self.db_path = 'tests/test_database.db'
        self.create_db()
        self.config = {
            'DB_FILE': 'tests/test_database.db',
            'CSV_FILE': os.path.abspath('tests/test_data_15_01_2022.csv')
        }
        
        self.extractor = SQLiteExtractor(self.config)
        self.extractor.connect_to_db()
    
    def create_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS transactions")
        # Create the database schema with the specified columns
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id TEXT PRIMARY KEY,
                transaction_date TEXT,
                category TEXT,
                name TEXT,
                quantity INTEGER,
                amount_excl_tax REAL,
                amount_inc_tax REAL
            )
        ''')

        # Commit changes and close the connection
        conn.commit()
        conn.close()
        
       
    def tearDown(self):
        self.extractor.cursor.execute('DROP TABLE IF EXISTS transactions')
        self.extractor.conn.close()

    def test_connect_to_db(self):
        self.assertIsInstance(self.extractor.conn, sqlite3.Connection)
        self.assertIsInstance(self.extractor.cursor, sqlite3.Cursor)

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