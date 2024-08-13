import sqlite3
import os

db_path = '../db/retail.db'
if not os.path.exists(db_path):
    print("Database file not found at:", db_path)
else:
    print("Database file found.")

conn = sqlite3.connect(db_path)

cursor = conn.cursor()

test = cursor.execute('''
            SELECT * FROM transactions WHERE id = '04056f49-705b-491a-95e8-ab66a95d1ac4'
        ''').fetchone()
    
print(test)

