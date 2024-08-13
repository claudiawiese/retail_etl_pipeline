import unittest
import sys
import os

# Add the `etl_workflow` directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../etl_workflow')))

from base_etl import BaseETL

class TestBaseETL(unittest.TestCase):
    def setUp(self):
        self.config = {
            'DB_FILE': 'tests/test_database.db',
            'CSV_FILE': 'tests/test_data_15_01_2022.csv'
        }
        self.base_etl = BaseETL(self.config)

    def test_initialization(self):
        self.assertEqual(self.base_etl.config['DB_FILE'], 'tests/test_database.db')
        self.assertEqual(self.base_etl.config['CSV_FILE'], 'tests/test_data_15_01_2022.csv')

    def test_run_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self.base_etl.run()

if __name__ == '__main__':
    unittest.main()