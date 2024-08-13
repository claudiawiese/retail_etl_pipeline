import sqlite3
import os

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

print(f'The total number of transactions on 14/01/2022 is {question1}')

print("-------------------------------------------------\n")  

print("Question 2: What is the total amount, including tax, of all SELL transactions ?")
question2 = cursor.execute('''
            SELECT sum(amount_inc_tax)
            FROM transactions 
            WHERE category = 'SELL'
        ''').fetchone()

print(f'the total amount, including tax, of all SELL transactions is {question2}')


print("-------------------------------------------------\n")  

print("Question 3: Consider the product Amazon Echo Dot:")
print("What is the balance (SELL - BUY) by date?")
print("What is the cumulated balance (SELL - BUY) by date?")
question3_1 = cursor.execute('''
            SELECT sum(amount_inc_tax)
            FROM transactions 
            WHERE category = 'SELL'
        ''').fetchone()
    
question3_2 = cursor.execute('''
            SELECT sum(amount_inc_tax)
            FROM transactions 
            WHERE category = 'SELL'
        ''')

print(f'The balance (SELL - BUY) by date is {question3_1}')
print(f'The cumulated balance (SELL - BUY) by date is {question3_2}')


