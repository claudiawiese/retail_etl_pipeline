import sqlite3
from base_etl import BaseETL

class SQLiteLoader(BaseETL):
    def insert_ignore_duplicates(self):
        self.cursor.executemany('''
            INSERT OR IGNORE INTO transactions (id, category, name, quantity, amount_excl_tax, amount_inc_tax, transaction_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', self.rows)

    def insert_update(self):
        self.cursor.executemany('''
            INSERT OR REPLACE INTO transactions (id, category, name, quantity, amount_excl_tax, amount_inc_tax, transaction_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', self.rows)

    def load(self):
        try:
            self.cursor.execute('BEGIN TRANSACTION')
            if self.config['INTEGRATION_METHOD'] == "update":
                self.insert_update()
            else:
                self.insert_ignore_duplicates()

            self.conn.commit()
            print("Data loading complete.")
        except sqlite3.Error as e:
            self.conn.rollback()
            print(f"An error occurred: {e}")