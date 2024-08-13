import unittest
import sqlite3
import sys
import os

# Add the `etl_workflow` directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../etl_workflow')))

from load import SQLiteLoader

class TestSQLiteLoader(unittest.TestCase):
    
    def setUp(self):
        self.config = {
            'DB_FILE': 'tests/test_database.db',
            'CSV_FILE': os.path.abspath('tests/test_data_15_01_2022.csv')
        }

        self.conn = sqlite3.connect("tests/test_database.db")
        self.cursor = self.conn.cursor()

        self.conn.execute('BEGIN')

        self.loader = SQLiteLoader(self.config)
        self.loader.conn = self.conn
        self.loader.cursor = self.cursor

        self.rows = [
                ('12bb9b25-2d10-4459-833b-742f5f590dcaccc','SELL','Addidas Running Shoes',5,399.95,479.94,'2022-01-15'),
                ('d0d36ed0-3795-4256-a405-8c91176c3c39','BUY','Fitbit Charge 5',5,449.95,539.94, '2022-01-16'),
                ('05e8b96f-0a96-4676-9a7b-810e9ae09c96','SELL','Salomon Jacket',5,799.95,959.94, '2022-01-16'),
            ]
    
    def tearDown(self):
        # Close the database connection     
        self.loader.conn.rollback()
        self.loader.conn.close()

    def test_insert_ignore_duplicates(self):
        self.loader.insert_ignore_duplicates(self.rows)
        self.loader.cursor.execute('SELECT * FROM transactions')
        rows = self.loader.cursor.fetchall()
        self.assertEqual(len(rows), 732)

    def test_insert_update(self):
        self.loader.insert_update(self.rows)
        self.loader.conn.commit()

        self.loader.cursor.execute('SELECT * FROM transactions')
        rows = self.loader.cursor.fetchall()
        self.assertEqual(len(rows), 731)


if __name__ == '__main__':
    unittest.main()