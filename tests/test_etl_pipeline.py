import unittest
import sqlite3
import sys
import os
from common_test_utilities import CommonTestUtilities

# Add the `etl_workflow` directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../etl_workflow')))

from etl_pipeline import ETLPipeline

class TestETLPipeline(CommonTestUtilities):
   
    def setUp(self):
        self.setUpConfig()
        self.create_db()
        self.etl_pipeline = ETLPipeline(self.config)
        self.etl_pipeline.connect_to_db()
    
    def tearDown(self):
        self.etl_pipeline.cursor.execute("DROP TABLE IF EXISTS transactions")
        self.etl_pipeline.conn.close()
        
    def test_run(self):
        self.etl_pipeline.run()
        
        self.etl_pipeline = ETLPipeline(self.config)
        self.etl_pipeline.connect_to_db()

        self.etl_pipeline.cursor.execute('SELECT * FROM transactions')
    
        rows = self.etl_pipeline.cursor.fetchall()
        self.assertEqual(len(rows), 3)

if __name__ == '__main__':
    unittest.main()