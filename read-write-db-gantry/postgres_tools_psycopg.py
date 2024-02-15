import psycopg2
from psycopg2 import sql

def get_query(table_name):
    if table_name == 'module_assembly':
        query = f""" 
    INSERT INTO {table_name} 
    (module_name, proto_name, hxb_name, position, region, ass_tray_id, comp_tray_id, put_id, tape_batch, epoxy_batch, operator)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    elif table_name == 'proto_assembly':
        query = f""" 
    INSERT INTO {table_name} 
    (proto_name, bp_name, sen_name, position, region, ass_tray_id, comp_tray_id, put_id, tape_batch, epoxy_batch, operator)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    else:
        query = None
        print('Table not found. Check argument.')
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


def upload_PostgreSQL(table_name,
                      db_upload):
    
    # conn = connect_db()

    conn = psycopg2.connect(
        host = 'localhost',
        database = 'hgcdb',
        user = 'postgres',
        password = 'hgcal')
    
    cursor = conn.cursor()
    print('Connection successful. \n')

    schema_name = 'public'
    table_exists_query = sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s);")
    cursor.execute(table_exists_query, [schema_name, table_name])
    table_exists = cursor.fetchone()[0]## this gives true/false
    
    query = get_query(table_name)
    
    data = tuple(db_upload)
    if table_exists:
        cursor.execute(query, data)
        conn.commit()
        print(f'Data is successfully uploaded to the {table_name}!')
    
    else:
        print(f'Table {table_name} does not exist in the database.')
        
    ## close connection
    cursor.close()
    conn.close()
    
    return None

def request_PostgreSQL(component_type):

    conn = psycopg2.connect(
        host = 'localhost',
        database = 'hgcdb',
        user = 'postgres',
        password = 'hgcal')
    
    cursor = conn.cursor();
    query = get_query_read(component_type)
    
    cursor.execute(query)
    value = cursor.fetchall();
    
    cursor.close(); conn.close();
    return value