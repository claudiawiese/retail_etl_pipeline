import sqlite3
import os
from tabulate import tabulate

db_path = 'db/retail.db'
if not os.path.exists(db_path):
    print("Database file not found at:", db_path)
else:
    print("Database file found.")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("QUESTIONS")
print("-------------------------------------------------\n")  

print("Question 1: What is the number of transactions on 14/01/2022 ?")
question1 = cursor.execute('''
            SELECT transaction_date, count(id) 
            FROM transactions 
            WHERE transaction_date = '2022-01-14'
        ''').fetchone()

print(f'The total number of transactions on 14/01/2022 is {question1[1]}.\n')

print("-------------------------------------------------\n")  

print("Question 2: What is the total amount, including tax, of all SELL transactions ?\n")
print("Assuming that amount_inc_tax is not the unit price but total price") 
question2 = cursor.execute('''
            SELECT sum(amount_inc_tax)
            FROM transactions 
            WHERE category = 'SELL'
        ''').fetchone()

print(f'The total amount, including tax, of all SELL transactions is {round(question2[0],2)}\n')

print("Assuming that amount_inc_tax is unit price")
question2 = cursor.execute('''
            SELECT sum(quantity * amount_inc_tax)
            FROM transactions 
            WHERE category = 'SELL'
        ''').fetchone()

print(f'The total amount, including tax, of all SELL transactions is {round(question2[0],2)}\n')


print("-------------------------------------------------\n")  

print("Question 3: Consider the product Amazon Echo Dot:")
print("What is the balance (SELL - BUY) by date? (Assuming amount_inc_tax is unit price)")


question3_1 = cursor.execute('''
            SELECT transaction_date,
                ROUND(
                    sum(case when category = 'BUY' then quantity * amount_inc_tax else 0 end) - 
                    sum(case when category = 'SELL' then quantity * amount_inc_tax else 0 end) 
                ) as net_amount
            FROM transactions 
            GROUP BY transaction_date
        ''')

rows = question3_1.fetchall()

headers = ["Transaction Date", "Net Amount"]

print(tabulate(rows, headers=headers, tablefmt="pretty"))
