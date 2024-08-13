import unittest
import sqlite3
import pdb
import sys
import os

# Add the `etl_workflow` directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../etl_workflow')))

from load import SQLiteLoader

class TestSQLiteLoader(unittest.TestCase):

    def setUp(self):
        self.db_path = 'tests/test_database.db'
        self.create_db()
        self.config = {
            'DB_FILE': 'tests/test_database.db',
            'CSV_FILE': os.path.abspath('tests/test_data_15_01_2022.csv')
        }

        self.conn = sqlite3.connect("tests/test_database.db")
        self.cursor = self.conn.cursor()

        self.loader = SQLiteLoader(self.config)
        self.loader.conn = self.conn
        self.loader.cursor = self.cursor

        self.seed_rows()

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
    
    def seed_rows(self):
        self.rows_initial = [
                ('1a-a','BUY','Addidas Running Shoes',5,399.95,479.94,'2022-01-15'),
                ('2b-b','BUY','Fitbit Charge 5',5,449.95,539.94, '2022-01-15'),
                ('3c-c','BUY','Salomon Jacket',5,799.95,959.94, '2022-01-15')
            ]

        self.rows_update = [
                ('1a-a','SELL','Addidas Running Shoes',5,399.95,479.94,'2022-01-16'),
                ('2b-b','SELL','Fitbit Charge 5',5,449.95,539.94, '2022-01-16'),
                ('4d-d','SELL','Salomon Jacket',5,799.95,959.94, '2022-01-16')
            ]

    def tearDown(self):  
        self.loader.cursor.execute('DROP TABLE IF EXISTS transactions') 
        self.loader.conn.close()
    
    def test_insert(self):
        self.loader.insert_ignore_duplicates(self.rows_initial)
        self.loader.cursor.execute('SELECT * FROM transactions')
        rows = self.loader.cursor.fetchall()
        self.assertEqual(len(rows), len(self.rows_initial))
    
    def test_insert_ignore_duplicates(self):
        self.loader.insert_ignore_duplicates(self.rows_initial)
        self.loader.insert_ignore_duplicates(self.rows_update)

        self.loader.cursor.execute('SELECT * FROM transactions')
        rows = self.loader.cursor.fetchall()
    
        self.assertEqual(len(rows), 4)

        first_record =  self.loader.cursor.execute("SELECT * FROM transactions WHERE id = '1a-a'").fetchone()
        self.assertEqual(first_record, ('1a-a','2022-01-15','BUY','Addidas Running Shoes',5,399.95,479.94))

        last_record =  self.loader.cursor.execute("SELECT * FROM transactions WHERE id = '4d-d'").fetchone()
        self.assertEqual(last_record,('4d-d','2022-01-16','SELL','Salomon Jacket',5,799.95,959.94))
    
    def test_insert_update(self):
        self.loader.insert_update(self.rows_initial)
        self.loader.insert_update(self.rows_update)

        self.loader.cursor.execute('SELECT * FROM transactions')
        rows = self.loader.cursor.fetchall()
        self.assertEqual(len(rows), 4)

        first_record =  self.loader.cursor.execute("SELECT * FROM transactions WHERE id = '1a-a'").fetchone()
        self.assertEqual(first_record,('1a-a','2022-01-16','SELL','Addidas Running Shoes',5,399.95,479.94))

        last_record =  self.loader.cursor.execute("SELECT * FROM transactions WHERE id = '4d-d'").fetchone()
        self.assertEqual(last_record,('4d-d','2022-01-16','SELL','Salomon Jacket',5,799.95,959.94))

if __name__ == '__main__':
    unittest.main()