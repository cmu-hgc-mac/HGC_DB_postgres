import csv
import os
import asyncio
import asyncpg
import yaml
import sys
sys.path.append('../')
from src.utils import connect_db

'''
logic:
1. extract the existing table schema
2. read the updated schema from csv File
3. Compare 1 and 2
4. Apply the changes
'''

# 1. extract the existing table schema
async def get_existing_table_schema(conn, table_name: str):
    query = f"""
    SELECT column_name, data_type, ordinal_position
    FROM information_schema.columns
    WHERE table_name = $1
    ORDER BY ordinal_position;
    """
    rows = await conn.fetch(query, table_name)
    existing_schema = {row['column_name']: row['data_type'] for row in rows}
    return existing_schema


# 2. read the updated schema from csv File
def get_desired_table_schema_from_csv(csv_file_path: str):
    desired_schema = {}
    with open(csv_file_path, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            column_name = row[0]
            data_type = row[1]
            desired_schema[column_name] = data_type
    return desired_schema

# 3. Compare 1 and 2
def compare_schemas(existing_schema: dict, desired_schema: dict):
    changes = []
    existing_columns = set(existing_schema.keys())
    desired_columns = set(desired_schema.keys())

    renamed_columns = []
    for existing_col in existing_columns:
        existing_type = existing_schema[existing_col]
        for desired_col in desired_columns:
            if existing_type == desired_schema[desired_col] and existing_col != desired_col:
                renamed_columns.append((existing_col, desired_col))
                existing_columns.remove(existing_col)
                desired_columns.remove(desired_col)
                break
    
    print(f'renamed_columns -- {renamed_columns}')
    for old_col, new_col in renamed_columns:
        changes.append(('rename_column', old_col, new_col))

    for column, new_type in desired_schema.items():
        if column in existing_schema:
            old_type = existing_schema[column]
            
            if ((new_type != 'serial PRIMARY KEY') & (old_type != new_type.lower())):## ignore primary key as we assume it will not be modified
                if (old_type, new_type) != ('integer', 'INT'):
                    changes.append(('datatype', column, old_type, new_type))
            
        else:
            changes.append(('new_column', column, None, new_type))
    
    for column in existing_schema:
        if column not in desired_schema:
            changes.append(('remove_column', column, existing_schema[column], None))
    
    return changes

# 4. Apply the changes - datatype
async def change_column_datatype(conn, table_name: str, column_name: str, old_datatype: str, new_datatype: str):
    alter_query = f"""
    ALTER TABLE {table_name}
    ALTER COLUMN {column_name} TYPE {new_datatype}
    USING {column_name}::{new_datatype};
    """
    await conn.execute(alter_query)
    print(f"Column {column_name} in table {table_name} changed from {old_datatype} to {new_datatype}.")

# 4. Apply the changes - rename column
async def change_column_name(conn, table_name: str, old_col_name: str, new_col_name: str):
    alter_query = f"""
    ALTER TABLE {table_name}
    RENAME COLUMN {old_col_name} TO {new_col_name};
    """
    await conn.execute(alter_query)
    print(f"Column {old_col_name} in table {table_name} renamed to {new_col_name}.")

# 4. Apply the changes
async def apply_changes(conn, table_name: str, changes):
    for change in changes:
        if change[0] == 'datatype':
            _, column, old_type, new_type = change
            await change_column_datatype(conn, table_name, column, old_type, new_type)
        elif change[0] == 'new_column':
            _, column, _, new_type = change
            alter_query = f"ALTER TABLE {table_name} ADD COLUMN {column} {new_type};"
            await conn.execute(alter_query)
            print(f"Column {column} added to table {table_name}.")
        elif change[0] == 'remove_column':
            _, column, _, _ = change
            alter_query = f"ALTER TABLE {table_name} DROP COLUMN {column};"
            await conn.execute(alter_query)
            print(f"Column {column} removed from table {table_name}.")
        elif change[0] == 'rename_column':
            _, old_col_name, new_col_name = change
            await change_column_name(conn, table_name, old_col_name, new_col_name)

async def main():
    ## Database connection parameters for new database
    loc = '../dbase_info/'
    yaml_file = f'{loc}tables.yaml'
    db_params = {
        'database': yaml.safe_load(open(yaml_file, 'r'))['dbname'],
        'user': 'postgres',   
        # 'password': input('Set superuser password: '),
        'password': 'hgcal',
        'host': yaml.safe_load(open(yaml_file, 'r'))['db_hostname'],  
        'port': yaml.safe_load(open(yaml_file, 'r'))['port']        
    }

    # establish a connection with database
    conn = await asyncpg.connect(user=db_params['user'], 
                                password=db_params['password'], 
                                host=db_params['host'], 
                                database=db_params['database'],
                                port=db_params['port'])

    # retrieve all table names from csv files
    all_table_names = []
    for filename in os.listdir(loc):
        if filename.endswith('.csv'):
            csv_file_path = os.path.join(loc, filename)
            all_table_names.append(os.path.splitext(filename)[0])  # Assuming table name is the same as CSV file name
    
    table_name = input('Enter the table name you want to apply a change(s). -- ')
    
    assert table_name in all_table_names, "Table was not found in the database."

    
    existing_schema = await get_existing_table_schema(conn, table_name)
    csv_file_path = os.path.join(loc, table_name) + '.csv'
    desired_schema = get_desired_table_schema_from_csv(csv_file_path)
    changes = compare_schemas(existing_schema, desired_schema)

    await apply_changes(conn, table_name, changes)
    await conn.close()
asyncio.run(main())

    