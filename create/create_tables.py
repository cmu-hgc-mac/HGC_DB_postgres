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

def get_csv_fname(loc):
    os.chdir(loc)
    fnameLs = glob.glob("*.csv")
    # fnameLs = [loc+i for i in fnameLs]
    return fnameLs

def get_table_info(loc, fname):
    with open(loc+fname, mode ='r') as file:
        csvFile = csv.reader(file)
        rows = []
        for row in csvFile:
            rows.append(row)
        temp = np.array(rows).T
        fk = temp[0][(np.where(temp[-1] != ''))]
        fk_ref = temp[-2][(np.where(temp[-1] != ''))]
        fk_tab = temp[-1][(np.where(temp[-1] != ''))]
        return fname.split('.csv')[0], temp[0], temp[1], fk, fk_ref, fk_tab   ### fk, fk_tab are returned as lists
    
def get_column_names(col1_list, col2_list, fk_name, fk_ref, parent_table):
    combined_list = []
    for item1, item2 in zip(col1_list, col2_list):
        combined_list.append(f'{item1} {item2}')
    table_columns = ', '.join(combined_list)
    if fk_name.size != 0:
        table_columns += f', CONSTRAINT {fk_ref[0]} FOREIGN KEY({fk_name[0]}) REFERENCES {parent_table[0]}({fk_name[0]})'
    return table_columns

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


######################################################################

loc = '../dbase_info/'
# fname_list = get_csv_fname(loc)
fname_list = ['module_assembly.csv', 'proto_assembly.csv','baseplate.csv', 'sensor.csv', 'hexaboard.csv', 'bp_inspect.csv', 'hxb_inspect.csv', 'proto_inspect.csv','module_inspect.csv', 'module_iv_test.csv','module_pedestal_test.csv','module_pedestal_plots.csv']

## Define the table name and schema
for fname in fname_list:
    print(f'Getting info from {fname} ...')
    table_name, table_header, dat_type, fk_name, fk_ref, parent_table = get_table_info(loc, fname)
    table_columns = get_column_names(table_header, dat_type, fk_name, fk_ref, parent_table)
    create_table(schema_name, table_name, table_columns)


# for table_name in table_names_list:
#     give_user_access(table_name)

conn.commit()
# Close the connection
cursor.close()
conn.close()
