import os, csv, sys
sys.path.append('../')
import asyncio, asyncpg
import numpy as np
import json
from conn import host, database, user, password
# from utils import connect_db #, get_table_name

'''
def get_query_read(component_type, part_name = None):
    if component_type == 'protomodule':
        query = """SELECT proto_name, thickness, geometry, resolution FROM proto_inspect WHERE geometry = 'full'"""    
    elif component_type == 'hexaboard':
        query = """SELECT hxb_name, thickness, geometry, resolution FROM hxb_inspect WHERE geometry = 'full'"""
    elif component_type == 'baseplate':
        query = """SELECT bp_name, thickness, geometry, resolution FROM bp_inspect WHERE geometry = 'full'"""
    elif component_type == 'baseplate_name':
        query = """SELECT bp_name FROM {part}_inspect WHERE geometry = 'full' ORDER BY bp_row_no DESC LIMIT 10;"""
    elif component_type == 'baseplate_plot':
        query = f"""SELECT hexplot FROM {}_inspect WHERE bp_name = '{part_name}'"""
    else:
        query = None
        print('Table not found. Check argument.')
    return query
'''
# comptable = {'baseplate':{'prefix': 'bp', 'pkprefix': 'bp'},'hexaboard':{'prefix': 'hxb', 'pkprefix': 'hxb'},'protomodule':{'prefix': 'proto', 'pkprefix': 'proto'},'module':{'prefix': 'module', 'pkprefix': 'module'}}

comptable = {'baseplate':{'prefix': 'bp'},'hexaboard':{'prefix': 'hxb'},'protomodule':{'prefix': 'proto'},'module':{'prefix': 'module'}}

def get_query_read(component_type, part_name = None, comptable=comptable):
    if part_name is None:
        query = f"""SELECT {comptable[component_type]['prefix']}_name FROM {comptable[component_type]['prefix']}_inspect ORDER BY {comptable[component_type]['pkprefix']}_row_no DESC LIMIT 10;"""
    else:
        query = f"""SELECT hexplot FROM {comptable[component_type]['prefix']}_inspect WHERE {comptable[component_type]['prefix']}_name = '{part_name}'"""
    return query


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

async def request_PostgreSQL(component_type, bp_name = None):
    result = await fetch_PostgreSQL(get_query_read(component_type, bp_name ))
    return result

def get_query(table_name):
    if table_name == 'bp_inspect':
        pre_query = f""" 
        INSERT INTO {table_name} 
        (bp_name, bp_material, geometry, resolution, flatness, thickness, x_points, y_points, z_points, date_inspect, time_inspect, hexplot, inspector, comment)
        VALUES """
    elif table_name == 'hxb_inspect':
        pre_query = f""" 
        INSERT INTO {table_name} 
        (hxb_name, geometry, resolution, flatness, thickness, x_points, y_points, z_points, date_inspect, time_inspect, hexplot, inspector, comment)
        VALUES  """
    elif table_name == 'proto_inspect':
        pre_query = f""" 
        INSERT INTO {table_name} 
        (proto_name, geometry, resolution, flatness, thickness, x_points, y_points, z_points, x_offset, y_offset, ang_offset, date_inspect, time_inspect, hexplot, inspector, comment)
        VALUES  """
    elif table_name == 'module_inspect':
        pre_query = f""" 
        INSERT INTO {table_name} 
        (module_name, geometry, resolution, flatness, thickness, x_points, y_points, z_points, x_offset, y_offset, ang_offset, date_inspect, time_inspect, hexplot, inspector, comment)
        VALUES  """
    data_placeholder = ', '.join(['${}'.format(i) for i in range(1, len(pre_query.split(','))+1)])
    query = f"""{pre_query} {'({})'.format(data_placeholder)}"""
    return query


async def upload_PostgreSQL(table_name, db_upload_data):
    conn = await asyncpg.connect(
        host=host,
        database=database,
        user=user,
        password=password)
    
    print('Connection successful. \n')

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
        query = get_query(table_name)
        await conn.execute(query, *db_upload_data)
        print(f'Executing query: {query}')
        print(f'Data successfully uploaded to the {table_name}!')
    else:
        print(f'Table {table_name} does not exist in the database.')
    await conn.close()

# inspector = 'cmu_person'
# resolution = 'LD'
# geometry = 'full'
# material = 'cf'
# proto_name='test2'
# comments = 'none'
# flatness = '0'
# inspectDate, inspectTime = 0,0
# sensor_Heights = np.loadtxt('sensor_heights.txt')
# actual_X, actual_Y, actual_Z = sensor_Heights[0], sensor_Heights[1], sensor_Heights[2]
# print(actual_X.tolist())
# thickness = np.mean(actual_Z)
# table_name = "proto_inspect"
# print('Data Obtained')
# upload_PostgreSQL(table_name,
#                     proto_name,
#                        material,
#                        geometry,
#                        resolution,
#                        thickness,
#                        actual_X,
#                        actual_Y,
#                        actual_Z,
#                        flatness,
#                        inspectDate,
#                        inspectTime,
#                        comments)


