import os
import csv
from datetime import datetime
import sqlite3
from base_etl import BaseETL

class SQLiteExtractor(BaseETL):
    def connect_to_db(self):
        if self.conn is None:
            self.conn = sqlite3.connect(self.config['DB_FILE'])
        self.cursor = self.conn.cursor()
        try:
            self.cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_transaction_id ON transactions (id)')
            self.conn.commit()
            print('Success, unique constraint has been created')
        except sqlite3.Error as e:
            print(f"An error occurred: {e}")

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