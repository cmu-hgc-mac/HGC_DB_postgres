import psycopg2
import os
import sys
import csv

'''
When you want to use the functions stored here, please add the followings at the top:

import sys
sys.path.append('../')
from src.utils import connect_db, get_table_name (the name of functions you want to import)

'''
def connect_db():
    '''
    connect to your postsgresql database
    '''
    db_params = {
        'dbname': open('../dbase_info/dbfname.txt','r').read(),
        'user': 'postgres',   # Assuming this is the superuser
        'password': input('Set superuser password: '),
        # 'password': pwinput.pwinput(prompt='Set superuser password: ', mask='*'),
        'host': 'localhost',  # Change this if your PostgreSQL server is on a different host
        'port': '5432'        # Default PostgreSQL port
    }

    # Connect to the database
    conn = psycopg2.connect(**db_params)
#     cursor = conn.cursor()
    
    return conn


def get_table_name():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute(
        """ 
        SELECT tablename FROM pg_tables 
        WHERE schemaname = 'public'
        """
    )
    
    tables = [row[0] for row in cursor.fetchall()]
    return tables

def print_tree(current_dict, indent=""):
    for key, value in current_dict.items():
        if isinstance(value, dict):
            print(f"{indent}{key}:")
            print_tree(value, indent + "  ")
        else:
            print(f"{indent}{key}: {value}")
