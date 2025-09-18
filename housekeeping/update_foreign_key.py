import asyncio, asyncpg
import glob, os, csv, yaml, argparse, base64, traceback
import numpy as np
import pwinput
from cryptography.fernet import Fernet

parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
parser.add_argument('-p', '--password', default=None, required=False, help="Password to access database.")
parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
args = parser.parse_args()

print('Updating foreign keys ...')
# Database connection parameters
loc = 'dbase_info'
tables_subdir = 'postgres_tables'
table_yaml_file = os.path.join(loc, 'tables.yaml')
conn_yaml_file = os.path.join(loc, 'conn.yaml')

db_params = {
    'database': yaml.safe_load(open(conn_yaml_file, 'r')).get('dbname'),
    'user': 'shipper',
    'host': yaml.safe_load(open(conn_yaml_file, 'r')).get('db_hostname'),
    'port': yaml.safe_load(open(conn_yaml_file, 'r')).get('port'),
}

if args.password is None:
        dbpassword = pwinput.pwinput(prompt='Enter superuser password: ', mask='*')
else:
    if args.encrypt_key is None:
        print("Encryption key not provided. Exiting..."); exit()
    cipher_suite = Fernet((args.encrypt_key).encode())
    dbpassword = cipher_suite.decrypt( base64.urlsafe_b64decode(args.password)).decode() ## Decode base64 to get encrypted string and then decrypt
    db_params.update({'password': dbpassword})

async def update_foreign_key():
    conn = await asyncpg.connect(**db_params)
    # print('Connection successful. \n')
        
    def get_table_info(loc, tables_subdir, fname):
        with open(os.path.join(loc, tables_subdir, fname) , mode='r') as file:
            csvFile = csv.reader(file)
            rows = []
            for row in csvFile:
                rows.append(row)
            columns = np.array(rows).T
            if 'fk_identifier' in columns[-2]:
                fk_itentifier = columns[0][(np.where(columns[-2] == 'fk_identifier'))][0]
                fk = columns[0][(np.where(columns[-1] != ''))][0]
                fk_ref = columns[-2][(np.where(columns[-1] != ''))][0]
                fk_tab = columns[-1][(np.where(columns[-1] != ''))][0]
                return (fname.split('.csv')[0]).split('/')[-1], fk_itentifier, fk, fk_tab, fk_ref  
            return (fname.split('.csv')[0]).split('/')[-1], None, None, None, None

    def get_foreign_key_query(table_name, fk_identifier, fk, fk_table):
        query = f"""
        UPDATE {table_name}
        SET {fk} = {fk_table}.{fk}
        FROM {fk_table} 
        WHERE {table_name}.{fk} IS NULL
        AND REPLACE({table_name}.{fk_identifier},'-','') = REPLACE({fk_table}.{fk_identifier},'-','');
        """
        return query

    try:
        with open(table_yaml_file, 'r') as file:
            data = yaml.safe_load(file)

            for i in data.get('tables'):
                fname = f"{(i['fname'])}"
                # print(f'Getting info from {fname} ...')
                table_name, fk_identifier, fk, fk_table, fk_ref = get_table_info(loc, tables_subdir, fname)
                # print(table_name, fk_identifier, fk, fk_table, fk_ref)
                if fk_identifier is not None:
                    # print(f'Updating foreign key "{fk}" in {table_name} ...')
                    try:
                        await conn.execute(get_foreign_key_query(table_name, fk_identifier, fk, fk_table))
                    except Exception as e:
                        print(f'Updating foreign key "{fk}" in {table_name} ...')
                        print(f"An error occurred: {e}")
                        traceback.print_exc()
            
    except asyncpg.PostgresError as e:
        print("Error:", e)
        traceback.print_exc()
    
    await conn.close()
    print('Foreign keys updated.')

asyncio.run(update_foreign_key())


### SELECT setval('your_table_id_seq', (SELECT MAX(id) FROM your_table));
### SELECT setval('module_info_module_no_seq', (SELECT MAX(module_no) FROM module_info));
### SELECT setval('module_assembly_module_ass_seq', (SELECT MAX(module_ass) FROM module_assembly));