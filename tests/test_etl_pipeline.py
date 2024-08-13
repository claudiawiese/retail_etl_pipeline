import unittest
import sys
import os

# Add the `etl_workflow` directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../etl_workflow')))

from etl_pipeline import ETLPipeline

class TestETLPipeline(unittest.TestCase):
    def setUp(self):
        self.config = {
            'DB_FILE': 'tests/test_database.db',
            'CSV_FILE': os.path.abspath('tests/test_data_15_01_2022.csv'),
            'BATCH_SIZE': 0,
            'INTEGRATION_METHOD': None
        }
        self.etl_pipeline = ETLPipeline(self.config)
        self.etl_pipeline.connect_to_db()

    def tearDown(self):
        self.etl_pipeline.conn.rollback()
        self.etl_pipeline.conn.close()

    def test_run(self):
        self.etl_pipeline.run()
        self.etl_pipeline = ETLPipeline(self.config)
        self.etl_pipeline.connect_to_db()
        self.etl_pipeline.cursor.execute('SELECT * FROM transactions')
        rows = self.etl_pipeline.cursor.fetchall()
        self.assertEqual(len(rows), 735)

if __name__ == '__main__':
    unittest.main()