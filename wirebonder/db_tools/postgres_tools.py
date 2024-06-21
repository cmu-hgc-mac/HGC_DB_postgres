import sys
import asyncio, asyncpg
import numpy as np
from conn import host, database, user, password

########### Write query #############

def get_query_write(table_name, column_names):
    pre_query = f""" INSERT INTO {table_name} ({', '.join(column_names)}) VALUES """
    data_placeholder = ', '.join(['${}'.format(i) for i in range(1, len(column_names)+1)])
    query = f"""{pre_query} {'({})'.format(data_placeholder)}"""
    return query

async def upload_PostgreSQL(table_name, db_upload_data):
    conn = await asyncpg.connect(
        host=host,
        database=database,
        user=user,
        password=password)
    
    schema_name = 'public'
    table_exists_query = """
    SELECT EXISTS (
        SELECT 1 
        FROM information_schema.tables 
        WHERE table_schema = $1 
        AND table_name = $2
    );
    """
    table_exists = await conn.fetchval(table_exists_query, schema_name, table_name)  ### Returns True/False
    if table_exists:
        query = get_query_write(table_name, db_upload_data.keys())
        await conn.execute(query, *db_upload_data.values())
        print(f'Executing query: {query}')
        print(f'Data successfully uploaded to the {table_name}!')
    else:
        print(f'Table {table_name} does not exist in the database.')
    await conn.close()

######## Read query ###########

async def fetch_PostgreSQL(query):
    conn = await asyncpg.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    value = await conn.fetch(query)
    await conn.close()
    return value

# async def request_PostgreSQL(query):
#     result = await fetch_PostgreSQL(query)
#     return result
