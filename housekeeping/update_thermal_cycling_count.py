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
    'user': 'postgres',
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

async def update_module_qc_summary():
    conn = await asyncpg.connect(**db_params)
    print('Connection successful.')
        
    try:    
        update_query_mod = """
            UPDATE module_qc_summary mqs                                                                                                             
            SET thermal_cycle_count = (                                                                                                              
                SELECT COALESCE(SUM(mbl.cycle_count), 0)                                                                                                                              FROM mmts_batch_logging mbl                                                                                                          
                WHERE EXISTS (                                                                                                                       
                    SELECT 1                                                                                                                         
                    FROM unnest(mbl.module_names) AS elem                                                                                            
                    WHERE elem ILIKE REPLACE(mqs.module_name, '-', '')                                                                               
                )                                                                                                                                    
                AND mbl.log_timestamp < mqs.grade_timestamp                                                                                          
            );
        """

        result = await conn.execute(update_query_mod)
        print(f"thermal_cycle_count updated.")    

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
    
    await conn.close()

async def update_module_info():
    conn = await asyncpg.connect(**db_params)
    print('Connection successful.')

    try:
        update_query_mod = """
            UPDATE module_info mi
            SET thermal_cycle_count = (
                SELECT COALESCE(SUM(mbl.cycle_count), 0) FROM mmts_batch_logging mbl
                WHERE EXISTS (
                    SELECT 1
                    FROM unnest(mbl.module_names) AS elem
                    WHERE elem ILIKE REPLACE(mi.module_name, '-', '')
                )
            );
        """

        result = await conn.execute(update_query_mod)
        print(f"thermal_cycle_count updated.")

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()

    await conn.close()

asyncio.run(update_module_qc_summary())
asyncio.run(update_module_info())

'''
        DO $$
        DECLARE
            rec RECORD;		
        BEGIN
            FOR rec IN (
                SELECT module_name AS selected_name FROM module_info 
            ) LOOP
                SELECT grade_timestamp AS selected_date FROM module_qc_summary WHERE REPLACE(module_name, '-', '') = rec.selected_name;
                SELECT log_timestamp AS comp_date FROM mmts_batch_logging WHERE (
                    SELECT * 
                    FROM mmts_batch_logging t
                    WHERE EXISTS (
                        SELECT 1
                        FROM unnest(t.module_names) AS elem
                        WHERE elem ILIKE rec.selected_name
                        )
                    )
                    IF (rec.comp_date < rec.selected_date) THEN (
                        UPDATE thermal_cycle_count
                    ) END IF;
                END LOOP;
            END $$;
            """
'''