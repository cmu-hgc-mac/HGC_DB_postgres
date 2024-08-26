import asyncio
import asyncpg
import yaml
import sys
sys.path.append('../')
import os
from table_hierarchy import local_db_hrchy

def find_key_by_value(dictionary, value_to_find):
    for key, value in dictionary.items():
        # Check if the current key is associated with the value_to_find
        if key == value_to_find:
            return None  # To prevent going back to an earlier level

        if isinstance(value, dict):
            # If value_to_find is a key in this dictionary, return the current key
            if value_to_find in value:
                return key
            # Recursively search within the nested dictionary
            result = find_key_by_value(value, value_to_find)
            if result is not None:
                # If a key is found at a lower level, return the key at this level
                return key if result is None else result

        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    # Recursively search within the dictionary inside the list
                    result = find_key_by_value(item, value_to_find)
                    if result is not None:
                        return key
                elif item == value_to_find:
                    return key
    return None


async def fill_foreign_keys():

    your_table = input("Enter the name of table you want to add -- ")
    parent_table = find_key_by_value(local_db_hrchy, your_table)
    
    if your_table == 'proto_assembly':
        common_col, fkey_col = 'proto_name', 'module_no'
    else:
        if parent_table == 'module_info':
            common_col, fkey_col = 'module_name', 'module_no'
        elif parent_table == 'module_assembly':
            common_col, fkey_col = 'hxb_name', 'module_no'
        elif parent_table == 'hexaboard':
            common_col, fkey_col = 'hxb_name', 'hxb_no'
        else:
            if your_table == 'proto_inspect':
                common_col, fkey_col = 'proto_name', 'proto_no'
            elif your_table == 'sensor':
                common_col, fkey_col = 'sen_name', 'proto_no'
            elif your_table == 'baseplate':
                common_col, fkey_col = 'bp_name', 'proto_no'
            elif your_table == 'bp_inspect':
                common_col, fkey_col = 'bp_name', 'bp_no'

    print(f'Your origin table: {parent_table}, common col: {common_col}, fkey_col: {fkey_col}')
    print("Establishing database connection...")

    # Connect to PostgreSQL
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
    print("Database connection established.")
    
    try:
        fetch_query = f"""
            SELECT {common_col}
            FROM {your_table}
            WHERE {fkey_col} IS NULL
        """
        print(f"Fetching rows where {fkey_col} is NULL...")
        rows = await conn.fetch(fetch_query)
        print(f"Fetched {len(rows)} rows.")
        
        # Iterate over each row and update the foreign key
        for row in rows:
            if your_table == 'proto_assembly':
                update_query = f"""
                    UPDATE {your_table}
                    SET {fkey_col} = {parent_table}.{fkey_col}
                    FROM {parent_table}
                    WHERE {parent_table}.module_name = regexp_replace({your_table}.{common_col}, 'P', 'M');
                """
                print(update_query)
                # Execute the update query for each row
                result = await conn.execute(update_query)
                print(f"Updated rows for {common_col}: {result}")
            else:
                module_name = row[common_col]
                update_query = f"""
                    UPDATE {your_table}
                    SET {fkey_col} = (
                        SELECT {parent_table}.{fkey_col}
                        FROM {parent_table}
                        WHERE {parent_table}.{common_col} = $1
                    )
                    WHERE {common_col} = $1
                """

                print(update_query)
                # Execute the update query for each row
                result = await conn.execute(update_query, module_name)
                print(f"Updated rows for {common_col} '{module_name}': {result}")

    finally:
        # Close the connection
        await conn.close()

# Running the async function
asyncio.run(fill_foreign_keys())
