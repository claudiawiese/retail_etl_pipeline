import sqlite3
from base_etl import BaseETL
from transform import Transformer

class SQLiteLoader(BaseETL):
    def __init__(self, config):
        super().__init__(config)
        self.transformer = Transformer()  # Instantiate Transformer class

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

    def insert_ignore_duplicates(self,rows):
        transformed_rows = self.transformer.transform_rows(rows) 
        self.cursor.executemany('''
            INSERT OR IGNORE INTO transactions (id, category, name, quantity, amount_excl_tax, amount_inc_tax, transaction_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', transformed_rows)

    def insert_update(self, rows):
        transformed_rows = self.transformer.transform_rows(rows) 
        self.cursor.executemany('''
            INSERT OR REPLACE INTO transactions (id, category, name, quantity, amount_excl_tax, amount_inc_tax, transaction_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', transformed_rows)

    def load(self):
        try:
            self.cursor.execute('BEGIN TRANSACTION')
            
            # Check if batching is required
            if self.config.get('BATCH_SIZE', 0) > 0:
                # Process in batches
                for i in range(0, len(self.rows), self.config['BATCH_SIZE']):
                    batch = self.rows[i:i + self.config['BATCH_SIZE']]
                    if self.config['INTEGRATION_METHOD'] == "update":
                        self.insert_update(batch)
                    else:
                        self.insert_ignore_duplicates(batch)
                    self.conn.commit()  # Commit each batch
            else:
                # Process all rows at once
                if self.config['INTEGRATION_METHOD'] == "update":
                    self.insert_update(self.rows)
                else:
                    self.insert_ignore_duplicates(self.rows)
                self.conn.commit()  # Commit the whole dataset

            print("Data loading complete.")
        except sqlite3.Error as e:
            self.conn.rollback()
            print(f"An error occurred: {e}")