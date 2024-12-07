import asyncio, asyncpg
import yaml, traceback

async def fetch_postgres_data(conn_args, query):
    conn = await asyncpg.connect(**conn_args)
    rows = await conn.fetch(query)
    await conn.close()
    return [dict(row) for row in rows]    

def get_macs_data(query, macs_conn_file):
    data_list = []
    with open(macs_conn_file, 'r') as file:
        macs_conn_data = yaml.safe_load(file)
        for mac in macs_conn_data.get('macs'):
            if len(mac['hostname']) != 0: #and mac['password'] != "":
                conn_args = {
                    'user'     : "viewer",
                    'database' : mac['db_name'],
                    'host'     : mac['hostname'],
                    'port'     : mac['port'],    }             
                try:
                    temp = asyncio.run(fetch_postgres_data(conn_args = conn_args, query = query))
                    data_list += temp
                except:
                    print(f"------> Unable to access {mac['mac_id']}'s database")
                    traceback.print_exc()
    return data_list


