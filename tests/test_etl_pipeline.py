"""
import unittest
import sys
import os

# Add the `etl_workflow` directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../etl_workflow')))

from etl_pipeline import ETLPipeline

class TestETLPipeline(unittest.TestCase):
    def setUp(self):
        self.config = {
            'DB_FILE': ':memory:',
            'CSV_FILE': os.path.abspath('tests/sample_data_15_01_2022.csv'),
            'BATCH_SIZE': 0,
            'INTEGRATION_METHOD': None
        }
        self.etl_pipeline = ETLPipeline(self.config)
        self.etl_pipeline.connect_to_db()
        self.create_test_table()

    def create_test_table(self):
       # Create the 'transactions' table
        self.etl_pipeline.cursor.execute('''
            CREATE TABLE transactions (
                id INTEGER PRIMARY KEY,
                transaction_date DATE,
                category TEXT,
                name TEXT,
                quantity INTEGER,
                amount_excl_tax REAL,
                amount_inc_tax REAL
            )
        ''')
        initial_data = [
            (1, '2022-01-14','SELL', 'Nike Running Shoes', 2, 200, 240),
            (2, '2022-01-14', 'BUY', 'Patagonia Jacket', 4, 180, 216)
        ]
        self.etl_pipeline.cursor.executemany('''
            INSERT INTO transactions (id, transaction_date, category, name, quantity, amount_excl_tax, amount_inc_tax)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', initial_data)
        self.etl_pipeline.conn.commit()

    def tearDown(self):
        self.etl_pipeline.conn.close()

    def test_run(self):
        self.etl_pipeline.run()
        self.etl_pipeline.cursor.execute('SELECT * FROM transactions')
        rows = self.etl_pipeline.cursor.fetchall()
        self.assertEqual(len(rows), 3)
    
if __name__ == '__main__':
    unittest.main()
"""