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

async def update_module_info():
    conn = await asyncpg.connect(**db_params)
    print('Connection successful.')
        
    try:    
        update_query_mod = """
            UPDATE module_info
            SET 
                proto_name = REPLACE(COALESCE(module_info.proto_name, module_assembly.proto_name),'-',''),
                hxb_name = REPLACE(COALESCE(module_info.hxb_name, module_assembly.hxb_name),'-','')
            FROM module_assembly
            WHERE REPLACE(module_info.module_name,'-','') = REPLACE(module_assembly.module_name,'-','')
              AND (module_info.proto_name IS NULL OR module_info.hxb_name IS NULL);
        """
        update_query_proto = """
            UPDATE module_info
            SET 
                bp_name = REPLACE(COALESCE(module_info.bp_name, proto_assembly.bp_name),'-',''),
                sen_name = REPLACE(COALESCE(module_info.sen_name, proto_assembly.sen_name),'-','')
            FROM proto_assembly
            WHERE REPLACE(module_info.proto_name,'-','') = REPLACE(proto_assembly.proto_name,'-','')
              AND (module_info.bp_name IS NULL OR module_info.sen_name IS NULL);
        """
        update_query_mod_cure = """
            UPDATE module_assembly
            SET 
                cure_date_end = module_inspect.date_inspect,
                cure_time_end = module_inspect.time_inspect
            FROM module_inspect
            WHERE REPLACE(module_inspect.module_name,'-','') = REPLACE(module_assembly.module_name,'-','')
              AND (module_assembly.cure_date_end IS NULL OR module_assembly.cure_time_end IS NULL);
        """
        update_query_proto_cure = """
            UPDATE proto_assembly
            SET 
                cure_date_end = proto_inspect.date_inspect,
                cure_time_end = proto_inspect.time_inspect
            FROM proto_inspect
            WHERE REPLACE(proto_inspect.proto_name,'-','') = REPLACE(proto_assembly.proto_name,'-','')
              AND (proto_assembly.cure_date_end IS NULL OR proto_assembly.cure_time_end IS NULL);
        """
        update_module_inspect_info = """UPDATE module_info SET inspected = (
            SELECT MIN(module_inspect.date_inspect) FROM module_inspect
            WHERE module_inspect.module_name = module_info.module_name) WHERE module_info.inspected IS NULL;"""
        
        update_module_wbfr_info = """UPDATE module_info SET wb_front = (
            SELECT MIN(front_wirebond.date_bond) FROM front_wirebond
            WHERE front_wirebond.module_name = module_info.module_name AND front_wirebond.wb_fr_marked_done	IS TRUE) WHERE module_info.wb_front IS NULL;"""
        
        update_module_wbbk_info = """UPDATE module_info SET wb_back = (
            SELECT MIN(back_wirebond.date_bond) FROM back_wirebond
            WHERE back_wirebond.module_name = module_info.module_name AND back_wirebond.wb_bk_marked_done IS TRUE) WHERE module_info.wb_back IS NULL;"""
        
        update_module_encapfr_info = """UPDATE module_info SET encap_front = (
            SELECT MIN(front_encap.date_encap) FROM front_encap
            WHERE front_encap.module_name = module_info.module_name) WHERE module_info.encap_front IS NULL;"""
        
        update_module_encapbk_info = """UPDATE module_info SET encap_back = (
            SELECT MIN(back_encap.date_encap) FROM back_encap
            WHERE back_encap.module_name = module_info.module_name) WHERE module_info.encap_back IS NULL;"""
        
        update_module_testiv_info = """UPDATE module_info SET test_iv = (
            SELECT MIN(module_iv_test.date_test) FROM module_iv_test
            WHERE module_iv_test.module_name = module_info.module_name) WHERE module_info.test_iv IS NULL;"""
        
        update_module_testped_info = """UPDATE module_info SET test_ped = (
            SELECT MIN(module_pedestal_test.date_test) FROM module_pedestal_test
            WHERE module_pedestal_test.module_name = module_info.module_name) WHERE module_info.test_ped IS NULL;"""
        
        result = await conn.execute(update_query_mod)
        print(f"Updated proto_name, hxb_name columns in module_info table")
        result = await conn.execute(update_query_proto)
        print(f"Updated bp_name, sen_name columns in module_info table")
        result = await conn.execute(update_query_mod_cure)
        print(f"Updated module curing time")
        result = await conn.execute(update_query_proto_cure)
        print(f"Updated protomodule curing time")
        result = await conn.execute(update_module_inspect_info)
        result = await conn.execute(update_module_wbfr_info)
        result = await conn.execute(update_module_wbbk_info)
        result = await conn.execute(update_module_encapfr_info)
        result = await conn.execute(update_module_encapbk_info)
        result = await conn.execute(update_module_testiv_info)
        result = await conn.execute(update_module_testped_info)
        
    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
    
    await conn.close()

asyncio.run(update_module_info())
