import os, sys, argparse, base64
import asyncio, asyncpg
import yaml, csv
from cryptography.fernet import Fernet
sys.path.append('../')
import pwinput

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
    SELECT column_name, data_type, ordinal_position, column_default
    FROM information_schema.columns
    WHERE table_name = $1
    ORDER BY ordinal_position;
    """
    rows = await conn.fetch(query, table_name)
    existing_schema = {row['column_name']: {'data_type': row['data_type'], 'default': row['column_default']} for row in rows}
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
async def change_column_datatype(conn, table_name: str, column_name: str, old_datatype: str, new_datatype: str, default_value: str):
    # Step 1: Change the data type (without DEFAULT)
    if (old_datatype != new_datatype) and (len(new_datatype.split()) == 1):
        alter_query = f"ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {new_datatype};"
        print(f"Executing: {alter_query}")
        try:
            await conn.execute(alter_query)
        except Exception as e:
            print(f"Failed to change data type for {column_name}: {e}")
            return  # Stop further processing for this column if type change fails
    
    # Step 2: Set the DEFAULT value separately
    if len(new_datatype.split()) > 1:
        set_default_query = f"ALTER TABLE {table_name} ALTER COLUMN {column_name} SET DEFAULT {default_value};"
        print(f"Executing: {set_default_query}")
        try:
            await conn.execute(set_default_query)
        except Exception as e:
            print(f"Failed to set DEFAULT for {column_name}: {e}")
    
    # print(f"Column {column_name} in table {table_name} processed.")

# 4. Apply the changes - drop a column if no data exists
async def remove_empty_column(conn, table_name, column_name):
    try:
        # Step 1: Count the non-NULL entries in the column
        query = f"""
            SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NOT NULL;
        """
        non_null_count = await conn.fetchval(query)

        # Step 2: If the count is 0, drop the column
        if non_null_count == 0:
            drop_query = f"""
                ALTER TABLE {table_name} DROP COLUMN {column_name};
            """
            await conn.execute(drop_query)
            print(f"Column '{column_name}' was successfully removed from table '{table_name}'.")
        else:
            print(f"Column '{column_name}' in table '{table_name}' contains data and was not removed.")

    except Exception as e:
        print(f"An error occurred: {e}")

# 4. Apply the changes - rename column
async def change_column_name(conn, table_name: str, old_col_name: str, new_col_name: str):
    alter_query = f"""
    ALTER TABLE {table_name}
    RENAME COLUMN {old_col_name} TO {new_col_name};
    """
    await conn.execute(alter_query)
    print(f"Column {old_col_name} in table {table_name} renamed to {new_col_name}.")

# 4. Apply the changes
async def apply_changes(conn, table_name: str, changes, existing_schema):
    for change in changes:
        # if change[0] == 'datatype':
        #     _, column, old_type, new_type = change
        #     await change_column_datatype(conn, table_name, column, old_type, new_type)
        if change[0] == 'datatype':
            _, column, old_type, new_type = change
            default_value = existing_schema[column].get('default')
            await change_column_datatype(conn, table_name, column, old_type, new_type, default_value)
        elif change[0] == 'new_column':
            _, column, _, new_type = change
            alter_query = f"ALTER TABLE {table_name} ADD COLUMN {column} {new_type};"
            await conn.execute(alter_query)
            print(f"Column {column} added to table {table_name}.")
        elif change[0] == 'remove_column':
            _, column, _, _ = change
            await remove_empty_column(conn, table_name, column)
        elif change[0] == 'rename_column':
            _, old_col_name, new_col_name = change
            await change_column_name(conn, table_name, old_col_name, new_col_name)

async def table_modify_seq(conn, table_name, loc, tables_subdir):
    existing_schema = await get_existing_table_schema(conn, table_name)
    csv_file_path = os.path.join(loc, tables_subdir, table_name) + '.csv'
    desired_schema = get_desired_table_schema_from_csv(csv_file_path)
    changes = compare_schemas(existing_schema, desired_schema)
    await apply_changes(conn, table_name, changes, existing_schema)

async def main():
    parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
    parser.add_argument('-t', '--tablename', default='all', required=False, help="Name of table to modify.")
    parser.add_argument('-p', '--password', default=None, required=False, help="Password to access database.")
    parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
    args = parser.parse_args()

    loc = 'dbase_info'
    tables_subdir = 'postgres_tables'
    table_yaml_file = os.path.join(loc, 'tables.yaml')
    conn_yaml_file = os.path.join(loc, 'conn.yaml')
    conn_info = yaml.safe_load(open(conn_yaml_file, 'r'))
    db_params = {
        'database': conn_info.get('dbname'),
        'user': 'postgres',
        'host': conn_info.get('db_hostname'),
        'port': conn_info.get('port'),}
    
    ## Database connection parameters for new database
    if args.password is None:
        dbpassword = pwinput.pwinput(prompt='Enter superuser password: ', mask='*')
    else:
        if args.encrypt_key is None:
            print("Encryption key not provided. Exiting.."); exit()
        cipher_suite = Fernet((args.encrypt_key).encode())
        dbpassword = cipher_suite.decrypt( base64.urlsafe_b64decode(args.password)).decode() ## Decode base64 to get encrypted string and then decrypt
        db_params.update({'password': dbpassword})

    # Establish a connection with database
    conn = await asyncpg.connect(**db_params)

    # retrieve all table names from csv files
    all_table_names = []
    for filename in os.listdir(os.path.join(loc, tables_subdir)):
        if filename.endswith('.csv'):
            csv_file_path = os.path.join(loc, tables_subdir, filename)
            all_table_names.append(os.path.splitext(filename)[0])  # Assuming table name is the same as CSV file name
    
    ## table_name = input('Enter the table name you want to apply a change(s). -- ')
    tablename_arg = ((args.tablename).split('.csv')[0]).lower()

    table_name_list = all_table_names if tablename_arg == 'all' else [tablename_arg]

    for table_name in table_name_list:
        try:
            assert table_name in all_table_names, "Table was not found in the csv list."
            await table_modify_seq(conn, table_name, loc, tables_subdir)
        except Exception as e:
            print('\n')
            print('##############################')
            print('########### ERROR! ###########')
            print(f'For table {table_name}:')
            print(e)
            print('##############################')
            print('\n')
        
    await conn.close()

asyncio.run(main())
