import sqlite3
from base_etl import BaseETL

class SQLiteLoader(BaseETL):
    def insert_ignore_duplicates(self, rows):
        self.cursor.executemany('''
            INSERT OR IGNORE INTO transactions (id, category, name, quantity, amount_excl_tax, amount_inc_tax, transaction_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', rows)

    def insert_update(self, rows):
        self.cursor.executemany('''
            INSERT OR REPLACE INTO transactions (id, category, name, quantity, amount_excl_tax, amount_inc_tax, transaction_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', rows)

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