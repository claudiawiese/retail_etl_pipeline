import os
import csv
from datetime import datetime
import sqlite3
from base_etl import BaseETL

class SQLiteExtractor(BaseETL):
    def extract_date_from_filename(self, filename):
        base_name = os.path.basename(filename)
        date_str = base_name.replace('.csv', '').split('_')[-3:]
        return datetime.strptime('_'.join(date_str), '%d_%m_%Y').date()

    def open_csv(self, csv_file):
        transaction_date = self.extract_date_from_filename(csv_file)
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            return [row + [transaction_date] for row in reader]

    def extract(self):
        self.rows = self.open_csv(self.config['CSV_FILE'])
        print("Data extraction complete.")