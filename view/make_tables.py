import psycopg2
import sshtunnel
# import pwinput
from psycopg2 import sql

print('Creating tables in the database...')
# Database connection parameters
db_params = {
    'dbname': 'testdb',
    'user': 'postgres',
    'password': input('Enter superuser password: /n'),
    # 'password': pwinput.pwinput(prompt='Enter superuser password: ', mask='*'),
    'host': 'localhost',  
    'port': '5432'     
}

# Connect to the database
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()
schema_name = 'public'  # Change this if your tables are in a different schema

def create_table(schema_name, table_name):
    # Check if the table exists
    table_exists_query = sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s);")
    cursor.execute(table_exists_query, [schema_name, table_name])
    table_exists = cursor.fetchone()[0]

    # If the table doesn't exist, create it
    if not table_exists:
        create_table_query = sql.SQL("""
            CREATE TABLE {} (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                age INT
            );
        """).format(sql.Identifier(table_name))
        cursor.execute(create_table_query)
        print(f"Table '{table_name}' created successfully.")
    else:
        print(f"Table '{table_name}' already exists.")

    cursor.execute(f"GRANT INSERT, SELECT ON {table_name} TO ogp_user;")
    cursor.execute(f"GRANT INSERT, SELECT ON {table_name} TO lv_user;")
    cursor.execute(f"GRANT SELECT ON {table_name} TO viewer;")
    print(f"Table '{table_name}' access granted to user lv_user.\n")

# Define the table name and schema
table_names_list = ['frontside_inspection', 'backside_inspection']
for table_name in table_names_list:
    create_table(schema_name, table_name)

# for table_name in table_names_list:
#     give_user_access(table_name)

conn.commit()
# Close the connection
cursor.close()
conn.close()


