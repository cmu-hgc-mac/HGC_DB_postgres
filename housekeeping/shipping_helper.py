import asyncpg, asyncio, os, yaml, base64
from cryptography.fernet import Fernet

loc = 'dbase_info'
conn_yaml_file = os.path.join(loc, 'conn.yaml')
db_params = {
    'database': yaml.safe_load(open(conn_yaml_file, 'r')).get('dbname'),
    'user': 'shipper',
    'host': yaml.safe_load(open(conn_yaml_file, 'r')).get('db_hostname'),
    'port': yaml.safe_load(open(conn_yaml_file, 'r')).get('port'),
}

def update_packed_timestamp_sync(encrypt_key, password, module_names, timestamp):
    query = """UPDATE module_info SET packed_timestamp = $1 WHERE module_name = ANY($2)"""
    asyncio.run(_update_packed_timestamp(encrypt_key, password, query, timestamp, module_names))

async def _update_packed_timestamp(encrypt_key, password, query, module_names, timestamp, db_params = db_params):
    cipher_suite = Fernet((encrypt_key).encode())
    dbpassword = cipher_suite.decrypt( base64.urlsafe_b64decode(password)).decode() ## Decode base64 to get encrypted string and then decrypt
    db_params.update({'password': dbpassword})
    try:
        conn = await asyncpg.connect(**db_params)
        await conn.execute(query, timestamp, module_names)
        print(f"Updated packed_timestamp for {len(module_names)} modules.")
    except Exception as e:
        print(f"Error updating packed_timestamp: {e}")
