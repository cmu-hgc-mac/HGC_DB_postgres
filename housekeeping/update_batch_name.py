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
    print('Connection successful.')
        
    try:    
        update_query_mod = """
            UPDATE module_iv_test miv                                                                                                             
            SET batch_name = (                                                                                                              
                SELECT mbl.batch_name
                FROM mmts_batch_logging mbl                                                                                                          
                WHERE EXISTS (                                                                                                                       
                    SELECT 1                                                                                                                         
                    FROM unnest(mbl.module_names) AS elem                                                                                            
                    WHERE elem ILIKE REPLACE(miv.module_name, '-', '')                                                                               
                )
                AND mbl.log_timestamp < (miv.date_test + miv.time_test)
                ORDER BY mbl.log_timestamp DESC
                LIMIT 1
            );
        """

        result = await conn.execute(update_query_mod)
        print(f"batch_no updated.")    

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
    
    await conn.close()

asyncio.run(update_module_iv_test())