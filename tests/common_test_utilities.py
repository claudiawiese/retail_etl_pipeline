import unittest
import sqlite3

class CommonTestUtilities(unittest.TestCase):
    def setUpConfig(self):
        self.db_path = 'tests/test_database.db'
        self.create_db()

        self.config = {
            'DB_FILE': self.db_path,
            'CSV_FILE': 'tests/test_data_15_01_2022.csv',
            'BATCH_SIZE': 0,
            'INTEGRATION_METHOD': None
        }

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
