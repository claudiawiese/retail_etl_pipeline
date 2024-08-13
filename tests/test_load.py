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
            'DB_FILE': 'tests/test_database.db',
            'CSV_FILE': os.path.abspath('tests/test_data_15_01_2022.csv'),
            'BATCH_SIZE': 0,
            'INTEGRATION_METHOD': 'ignore'
        }
        self.loader = SQLiteLoader(self.config)
        self.loader.connect_to_db()

    def tearDown(self):
        # Close the database connection
        self.loader.conn.rollback()
        self.loader.conn.close()

    def test_insert_ignore_duplicates(self):
        self.loader.insert_ignore_duplicates(self.loader.rows)
        self.loader.conn.commit()

        self.loader.cursor.execute('SELECT * FROM transactions')
        rows = self.loader.cursor.fetchall()
        self.assertEqual(len(rows), 735)

    def test_insert_update(self):
        self.loader.insert_update(self.loader.rows)
        self.loader.conn.commit()

        self.loader.cursor.execute('SELECT * FROM transactions')
        rows = self.loader.cursor.fetchall()
        self.assertEqual(len(rows), 735)

if __name__ == '__main__':
    unittest.main()
"""    