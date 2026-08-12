import asyncio, asyncpg
import glob, os, csv, yaml, argparse, base64, traceback
import numpy as np
import pwinput
from cryptography.fernet import Fernet

parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
parser.add_argument('-p', '--password', default=None, required=False, help="Password to access database.")
parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
args = parser.parse_args()

# Database connection parameters
loc = 'dbase_info'
tables_subdir = 'postgres_tables'
table_yaml_file = os.path.join(loc, 'tables.yaml')
conn_yaml_file = os.path.join(loc, 'conn.yaml')
conn_info = yaml.safe_load(open(conn_yaml_file, 'r'))
  
db_params = {
    'database': conn_info.get('dbname'),
    'user': 'editor',
    'host': conn_info.get('db_hostname'),
    'port': conn_info.get('port'),
}
        
## Database connection parameters for new database
if args.password is None:
    dbpassword = pwinput.pwinput(prompt='Enter superuser password: ', mask='*')
else:
    if args.encrypt_key is None:
        print("Encryption key not provided. Exiting.."); exit()
    cipher_suite = Fernet((args.encrypt_key).encode())
    dbpassword = cipher_suite.decrypt( base64.urlsafe_b64decode(args.password)).decode() ## Decode base64 to get encrypted string and then decrypt

db_params.update({'password': dbpassword})  

async def update_module_iv_test():
    conn = await asyncpg.connect(**db_params)
    # print('Connection successful.')
        
    try:    
        update_query_mod = """
            UPDATE module_iv_test miv
            SET batch_name = (
                SELECT DISTINCT ON (mbl.batch_name) mbl.batch_name
                FROM mmts_batch_logging mbl
                WHERE EXISTS (
                    SELECT 1
                    FROM unnest(mbl.module_names) AS elem
                    WHERE elem ILIKE REPLACE(miv.module_name, '-', '')
                )
                AND mbl.module_names IS NOT NULL
                AND mbl.station_names IS NOT NULL
                AND mbl.log_timestamp < (miv.date_test + miv.time_test)
                ORDER BY mbl.batch_name, mbl.log_timestamp DESC
                LIMIT 1
            );
        """

        result = await conn.execute(update_query_mod)
        print(f"Batch names updated in IV test.")

        update_query_station = """
            UPDATE module_iv_test miv
            SET station_name = (
                SELECT DISTINCT ON (mbl.batch_name) mbl.station_names[elem.idx]
                FROM mmts_batch_logging mbl,
                     LATERAL (
                         SELECT ord AS idx
                         FROM unnest(mbl.module_names) WITH ORDINALITY AS u(name, ord)
                         WHERE u.name ILIKE REPLACE(miv.module_name, '-', '')
                         LIMIT 1
                     ) elem
                WHERE mbl.batch_name = miv.batch_name
                AND mbl.module_names IS NOT NULL
                AND mbl.station_names IS NOT NULL
                LIMIT 1
            )
            WHERE miv.batch_name IS NOT NULL;
        """

        result = await conn.execute(update_query_station)
        print(f"Station names updated in IV test.")

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
    
    await conn.close()

async def update_module_pedestal_test():
    conn = await asyncpg.connect(**db_params)
    # print('Connection successful.')

    try:
        update_query_mod = """
            UPDATE module_pedestal_test mpt
            SET batch_name = (
                SELECT DISTINCT ON (mbl.batch_name) mbl.batch_name
                FROM mmts_batch_logging mbl
                WHERE EXISTS (
                    SELECT 1
                    FROM unnest(mbl.module_names) AS elem
                    WHERE elem ILIKE REPLACE(mpt.module_name, '-', '')
                )
                AND mbl.module_names IS NOT NULL
                AND mbl.station_names IS NOT NULL
                AND mbl.log_timestamp < (mpt.date_test + mpt.time_test)
                ORDER BY mbl.batch_name, mbl.log_timestamp DESC
                LIMIT 1
            );
        """

        result = await conn.execute(update_query_mod)
        print(f"Batch names updated in pedestal test.")

        update_query_station = """
            UPDATE module_pedestal_test mpt
            SET station_name = (
                SELECT DISTINCT ON (mbl.batch_name) mbl.station_names[elem.idx]
                FROM mmts_batch_logging mbl,
                     LATERAL (
                         SELECT ord AS idx
                         FROM unnest(mbl.module_names) WITH ORDINALITY AS u(name, ord)
                         WHERE u.name ILIKE REPLACE(mpt.module_name, '-', '')
                         LIMIT 1
                     ) elem
                WHERE mbl.batch_name = mpt.batch_name
                AND mbl.module_names IS NOT NULL
                AND mbl.station_names IS NOT NULL
                LIMIT 1
            )
            WHERE mpt.batch_name IS NOT NULL;
        """

        result = await conn.execute(update_query_station)
        print(f"Station names updated in pedestal test.")

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()

    await conn.close()

asyncio.run(update_module_iv_test())
asyncio.run(update_module_pedestal_test())