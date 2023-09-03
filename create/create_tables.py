import psycopg2
import sshtunnel
import csv
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

def get_column_names(col1, col2):
    col1_list = [item.strip() for item in col1.split(',')]
    col2_list = [item.strip() for item in col2.split(',')]
    combined_list = []
    for item1, item2 in zip(col1_list, col2_list):
        combined_list.append(f'{item1} {item2}')
    return ', '.join(combined_list)


def create_table(schema_name, table_name, table_columns):
    # Check if the table exists
    table_exists_query = sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s);")
    cursor.execute(table_exists_query, [schema_name, table_name])
    table_exists = cursor.fetchone()[0]

    # If the table doesn't exist, create it
    if not table_exists:
        create_table_query = sql.SQL(f""" CREATE TABLE {table_name} ( {table_columns} ); """)
        cursor.execute(create_table_query)
        print(f"Table '{table_name}' created successfully.")
    else:
        print(f"Table '{table_name}' already exists.")

    cursor.execute(f"GRANT INSERT, SELECT ON {table_name} TO ogp_user;")
    cursor.execute(f"GRANT INSERT, SELECT ON {table_name} TO lv_user;")
    cursor.execute(f"GRANT SELECT ON {table_name} TO viewer;")
    print(f"Table '{table_name}' access granted to user ogp_user, lv_user.\n")

csv_file_path = '../dbase_info/postgresTables.csv'

# Define the table name and schema
with open(csv_file_path, 'r') as file:
    csv_reader = csv.reader(file)
    for row in csv_reader:
        table_name, table_write, table_read, table_header, dat_type = row
        table_columns = get_column_names(table_header, dat_type)
        create_table(schema_name, table_name, table_columns)

# for table_name in table_names_list:
#     give_user_access(table_name)

conn.commit()
# Close the connection
cursor.close()
conn.close()


