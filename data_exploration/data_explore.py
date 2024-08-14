import sqlite3
import os
import sys
from tabulate import tabulate

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../etl_workflow')))

from etl_pipeline import ETLPipeline
import config

#Check if retail.db exists 
db_path = 'db/retail.db'
if not os.path.exists(db_path):
    print("Database file not found at:", db_path)
else:
    print("Database file found.")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

#Launch etl_pipeline to integrate csv
etl_pipeline = ETLPipeline(config.CONFIG)
etl_pipeline.run()

#Data exploration questions 
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
print("3.1 What is the balance (SELL - BUY) by date? (Assuming amount_inc_tax is unit price)\n")


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


print("\n3.2 What is the cumulative balance (SELL - BUY) by date? (Assuming amount_inc_tax is unit price)\n")

question3_2 = cursor.execute('''
    WITH NetAmounts AS (
        SELECT 
            transaction_date,
            ROUND(
                SUM(CASE 
                    WHEN category = 'BUY' THEN quantity * amount_inc_tax 
                    ELSE 0 
                END) - 
                SUM(CASE 
                    WHEN category = 'SELL' THEN quantity * amount_inc_tax 
                    ELSE 0 
                END)
            ) AS net_amount
        FROM 
            transactions
        GROUP BY 
            transaction_date
    )
    SELECT 
        transaction_date,
        net_amount,
        SUM(net_amount) OVER (ORDER BY transaction_date) AS cumulative_balance
    FROM 
        NetAmounts
    ORDER BY 
        transaction_date;
''')

rows = question3_2.fetchall()
headers = ["Transaction Date", "Net Amount", "Cumulative Balance"]
print(tabulate(rows, headers=headers, tablefmt="pretty"))