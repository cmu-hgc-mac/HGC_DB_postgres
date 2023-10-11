import psycopg2
import sshtunnel
import glob, os
import csv
import numpy as np
import pwinput
from psycopg2 import sql

print('Creating tables in the database...')
# Database connection parameters
db_params = {
    'dbname': open('../dbase_info/dbfname.txt','r').read(),
    'user': 'postgres',
    'password': pwinput.pwinput(prompt='Enter superuser password: ', mask='*'),
    'host': 'localhost',  
    'port': '5432'     
}

# Connect to the database
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()
schema_name = 'public'  # Change this if your tables are in a different schema
print('Connection successful. \n')

# Get a list of all table names in the current schema
cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
table_names = [record[0] for record in cursor.fetchall()]

# Delete all tables in the current schema
for table_name in table_names:
    table_name = sql.Identifier(table_name)
    drop_table_query = sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(table_name)
    cursor.execute(drop_table_query)

print("All tables have been deleted.")

conn.commit()
# Close the connection
cursor.close()
conn.close()


