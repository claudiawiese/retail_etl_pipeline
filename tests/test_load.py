"""
import unittest
import sqlite3
import sys
import os

# Add the `etl_workflow` directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../etl_workflow')))

from load import SQLiteLoader


class TestSQLiteLoader(unittest.TestCase):
    def setUp(self):
        # Set up a temporary in-memory database
        self.config = {
            'DB_FILE': ':memory:',
            'CSV_FILE': os.path.abspath('tests/sample_data_15_01_2022.csv'),
            'BATCH_SIZE': 2,
            'INTEGRATION_METHOD': 'ignore'
        }
        self.loader = SQLiteLoader(self.config)
        self.loader.connect_to_db()
        self.create_test_table()
        self.insert_test_data()

    def create_test_table(self):
        # Create a table in the temporary database for testing
        self.loader.cursor.execute('''
            CREATE TABLE transactions (
                id INTEGER PRIMARY KEY,
                transaction_date DATE
                category TEXT,
                name TEXT,
                quantity INTEGER,
                amount_excl_tax REAL,
                amount_inc_tax REAL,
            )
        ''')
        self.loader.conn.commit()

    def insert_test_data(self):
        # Insert test data into the database
        self.loader.rows = [
            ('0284f92e-54f7-4766-880d-2cc5a8993a89', '2022-01-14','SELL', 'Nike Running Shoes', 2, 200, 240),
            ('05d7b23c-3b9a-45c1-a1a8-c947d5418b68', '2022-01-14', 'BUY', 'Patagonia Jacket', 4, 180, 216)
        ]

    def tearDown(self):
        # Close the database connection
        self.loader.conn.close()

    def test_insert_ignore_duplicates(self):
        self.loader.insert_ignore_duplicates(self.loader.rows)
        self.loader.conn.commit()

        self.loader.cursor.execute('SELECT * FROM transactions')
        rows = self.loader.cursor.fetchall()
        self.assertEqual(len(rows), 3)
        
    def test_insert_update(self):
        self.loader.insert_update(self.loader.rows)
        self.loader.conn.commit()

        self.loader.cursor.execute('SELECT * FROM transactions')
        rows = self.loader.cursor.fetchall()
        self.assertEqual(len(rows), 3)

    def test_load_all_rows(self):
        self.loader.load()  # This should load all rows since batch size is 2
        self.loader.cursor.execute('SELECT * FROM transactions')
        rows = self.loader.cursor.fetchall()
        self.assertEqual(len(rows), 3)

if __name__ == '__main__':
    unittest.main()
"""