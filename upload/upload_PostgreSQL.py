import psycopg2
import os
import csv
import sys
sys.path.append('../')
from psycopg2 import sql
import numpy as np
from src.utils import connect_db, get_table_name

def upload_PostgreSQL(table_name,
                       material,
                       geometry,
                       resolution,
                       thickness,
                       actual_X,
                       actual_Y,
                       actual_Z,
                       flatness,
                       inspectDate,
                       inspectTime,
                       comments):
    
    conn = connect_db()
    cursor = conn.cursor()
    print('Connection successful. \n')


    table_exists_query = sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s);")
    cursor.execute(table_exists_query, [schema_name, table_name])
    table_exists = cursor.fetchone()[0]## this gives true/false
    
    query = f""" 
    INSERT INTO {table_name} 
    ({material, geometry, resolution, thickness, actual_X, actual_Y, 
    actual_Z, flatness, inspectDate, inspectTime, comments})
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    data = (material, geometry, resolution, thickness, actual_X, actual_Y, 
            actual_Z, flatness, inspectDate, inspectTime, comments)
    
    if table_exists:
        cursor.execute(query, tuple(data))
        conn.commit()
        print(f'Data is successfully uploaded to the {table_name}!')
    
    else:
        print(f'Table {table_name} does not exist in the database.')
        
    ## close connection
    cursor.close()
    conn.close()
    
    return None
