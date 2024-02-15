import asyncpg
import asyncio

def get_query(table_name):
    if table_name == 'module_assembly':
        pre_query = f""" 
    INSERT INTO {table_name} 
    (module_name, proto_name, hxb_name, position, region, ass_tray_id, comp_tray_id, put_id, tape_batch, epoxy_batch, operator)
    VALUES   """  ### maintain space
    elif table_name == 'proto_assembly':
        pre_query = f""" 
    INSERT INTO {table_name} 
    (proto_name, bp_name, sen_name, position, region, ass_tray_id, comp_tray_id, put_id, tape_batch, epoxy_batch, operator)
    VALUES   """ ### maintain space
         
    data_placeholder = ', '.join(['${}'.format(i) for i in range(1, len(pre_query.split(','))+1)])
    query = f"""{pre_query} {'({})'.format(data_placeholder)}"""
    return query

def get_query_read(component_type):
    if component_type == 'protomodule':
        query = """SELECT proto_name, thickness, geometry, resolution FROM proto_inspect WHERE geometry = 'full'"""    
    elif component_type == 'hexaboard':
        query = """SELECT hxb_name, thickness, geometry, resolution FROM hxb_inspect WHERE geometry = 'full'"""
    elif component_type == 'baseplate':
        query = """SELECT bp_name, thickness, geometry, resolution FROM bp_inspect WHERE geometry = 'full'"""
    else:
        query = None
        print('Table not found. Check argument.')
    return query

async def upload_PostgreSQL(table_name, db_upload):
    conn = await asyncpg.connect(
        host='localhost',
        database='hgcdb',
        user='postgres',
        password='hgcal'
    )
    
    print('Connection successful.\n')

    schema_name = 'public'
    table_exists_query = """
    SELECT EXISTS (
        SELECT 1 
        FROM information_schema.tables 
        WHERE table_schema = $1 
        AND table_name = $2
    );
    """
    table_exists = await conn.fetchval(table_exists_query, schema_name, table_name)   
    if table_exists:
        query = get_query(table_name)
        await conn.execute(query, *db_upload)
        print(f'Data is successfully uploaded to the {table_name}!')
    else:
        print(f'Table {table_name} does not exist in the database.')
    await conn.close()


async def fetch_PostgreSQL(query):
    conn = await asyncpg.connect(
        host='localhost',
        database='hgcdb',
        user='postgres',
        password='hgcal'
    )
    value = await conn.fetch(query)
    await conn.close()
    return value

async def request_PostgreSQL(component_type):
    result = await fetch_PostgreSQL(get_query_read(component_type))
    return result
  