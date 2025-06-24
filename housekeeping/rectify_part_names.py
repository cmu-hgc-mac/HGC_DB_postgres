import asyncio, asyncpg
import glob, os, csv, yaml, argparse, base64, traceback
import numpy as np
import pwinput
from cryptography.fernet import Fernet

parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
parser.add_argument('-fp', '--filepath', default=None, required=False, help="File path of the CSV file.")
# parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
args = parser.parse_args()

# Database connection parameters
loc = 'dbase_info'
tables_subdir = 'postgres_tables'
table_yaml_file = os.path.join(loc, 'tables.yaml')
conn_yaml_file = os.path.join(loc, 'conn.yaml')
password = None

db_params = {
    'database': yaml.safe_load(open(conn_yaml_file, 'r')).get('dbname'),
    'user': 'editor',
    'host': yaml.safe_load(open(conn_yaml_file, 'r')).get('db_hostname'),
    'port': yaml.safe_load(open(conn_yaml_file, 'r')).get('port'),
}

if password is None:
    dbpassword = pwinput.pwinput(prompt='Enter editor password: ', mask='*')
# else:
#     if args.encrypt_key is None:
#         print("Encryption key not provided. Exiting..."); exit()
#     cipher_suite = Fernet((args.encrypt_key).encode())
#     dbpassword = cipher_suite.decrypt( base64.urlsafe_b64decode(args.password)).decode() ## Decode base64 to get encrypted string and then decrypt

db_params.update({'password': dbpassword})

async def rectify_module_name_in_all_tables(part_old_name = '', part_new_name = ''):
    conn = await asyncpg.connect(
        user=db_params['user'],
        password=db_params['password'],
        database=db_params['database'],
        host=db_params['host'], port=db_params['port'])

    part_name_col_dict = {"M": "module_name", "P": "proto_name", "X": "hxb_name", "B": "bp_name"}
    part_name_col = part_name_col_dict[part_old_name[3]]
    roc_version_dict = {'X': 'Preseries', '2': 'HGCROCV3b-2', '4': 'HGCROCV3b-4','C': 'HGCROCV3c',}

    try:
        # Find all tables that have the 'part_name_col' column
        query = f"""SELECT table_name FROM information_schema.columns WHERE column_name = '{part_name_col}' AND table_schema = 'public'; """
        tables = await conn.fetch(query)

        # Iterate through tables and update the (proto)module_name column
        for record in tables:
            table = record['table_name']
            update_query = f""" UPDATE {table} SET {part_name_col} = $1 WHERE {part_name_col} = $2; """
            result = await conn.execute(update_query, part_new_name, part_old_name)
            print(f"Updated rows in table '{table}':  {part_old_name} --> {part_new_name}", '-----', result)

        if part_new_name[3] == 'M':
            roc_version = roc_version_dict[part_new_name[8]]
            update_query = f"""UPDATE module_info SET roc_version = $1 WHERE module_name = $2; """
            await conn.execute(update_query, roc_version, part_new_name)

    except Exception as e:
        print(e)
    finally:
        await conn.close()

print('Rectifying part names ...')
with open(args.filepath, 'r', newline='') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        asyncio.run(rectify_module_name_in_all_tables(part_old_name=row[0], part_new_name=row[1]))
        if row[0][3] == 'M':
            asyncio.run(rectify_module_name_in_all_tables(part_old_name=row[0].replace('320M', '320P'), part_new_name=row[1].replace('320M', '320P')))
