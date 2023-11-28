import psycopg2
import os
import sys
import csv
import sys
sys.path.append('../')
from HGC_DB_postgres.src.utils import connect_db

conn = connect_db()
cursor = conn.cursor()

retrieving_col = input('The column name to retrieve: ')
condition_1 = input('conditions for feature_name: ')
condition_2 = input('conditions for value: ')
condition_3 = input('conditions for bp_name: ')

query = f"""
SELECT {retrieving_col} FROM bp_inspect
WHERE feature_name = %s AND value = %s AND bp_name = %s;
"""

search_conditions = (condition_1, condition_2, condition_3)

cursor.execute(query, search_conditions)

result = cursor.fetchall()
print(f'The {retrieving_col} met with your specificied conditions is ----- {result[0]}')


