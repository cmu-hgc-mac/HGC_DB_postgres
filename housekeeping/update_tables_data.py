import asyncio, asyncpg
import glob, os, csv, yaml, argparse
import numpy as np
import pwinput

parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
parser.add_argument('-p', '--password', default=None, required=False, help="Password to access database.")
args = parser.parse_args()

# Database connection parameters
loc = 'dbase_info'
tables_subdir = 'postgres_tables'
table_yaml_file = os.path.join(loc, 'tables.yaml')
conn_yaml_file = os.path.join(loc, 'conn.yaml')

dbpassword = str(args.password).replace(" ", "")
if dbpassword is None:
    dbpassword = pwinput.pwinput(prompt='Enter superuser password: ', mask='*')

db_params = {
    'database': yaml.safe_load(open(conn_yaml_file, 'r')).get('dbname'),
    'user': 'shipper',
    'password': dbpassword,
    'host': yaml.safe_load(open(conn_yaml_file, 'r')).get('db_hostname'),
    'port': yaml.safe_load(open(conn_yaml_file, 'r')).get('port'),
}

async def update_module_info():
    conn = await asyncpg.connect(**db_params)
    print('Connection successful.')
        
    try:    
        update_query_mod = """
            UPDATE module_info
            SET 
                proto_name = COALESCE(module_info.proto_name, module_assembly.proto_name),
                hxb_name = COALESCE(module_info.hxb_name, module_assembly.hxb_name)
            FROM module_assembly
            WHERE module_info.module_name = module_assembly.module_name
              AND (module_info.proto_name IS NULL OR module_info.hxb_name IS NULL);
        """
        update_query_proto = """
            UPDATE module_info
            SET 
                bp_name = COALESCE(module_info.bp_name, proto_assembly.bp_name),
                sen_name = COALESCE(module_info.sen_name, proto_assembly.sen_name)
            FROM proto_assembly
            WHERE module_info.proto_name = proto_assembly.proto_name
              AND (module_info.bp_name IS NULL OR module_info.sen_name IS NULL);
        """
        
        result = await conn.execute(update_query_mod)
        print(f"Updated proto_name, hxb_name columns in module_info table")
        result = await conn.execute(update_query_proto)
        print(f"Updated bp_name, sen_name columns in module_info table")

    except Exception as e:
        print(f"An error occurred: {e}")
    
    await conn.close()

asyncio.run(update_module_info())
