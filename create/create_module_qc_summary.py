import asyncio
import asyncpg
import numpy as np
import pwinput
import yaml
import csv

print('Creating module_qc_summary tables in the database...')
# Database connection parameters
loc = '../dbase_info/'
yaml_file = f'{loc}tables.yaml'
db_params = {
    'database': yaml.safe_load(open(yaml_file, 'r'))['dbname'],
    'user': 'postgres',
    'password': pwinput.pwinput(prompt='Enter superuser password: ', mask='*'),
    'host': 'localhost',
    'port': '5432'
}

def parse_csv_schema(csv_file):
    columns = []
    with open(csv_file, newline='') as file:
        reader = csv.reader(file)
        for row in reader:
            column_name, data_type, key, source_table = row[:4]
            if column_name == 'mod_qc_no':
                # Handling the primary key which doesn't import from any table
                columns.append((column_name, data_type, key, None, None))
            elif column_name == 'module_no':
                # Handling a foreign key that references another table
                original_column = 'module_no'
                columns.append((column_name, data_type, key, source_table, original_column))
            else:
                # Regular column processing based on prefix
                if 'proto_' in column_name:
                    source_table = 'proto_inspect'
                elif 'module_' in column_name:
                    source_table = 'module_inspect'
                elif 'hxb_' in column_name:
                    source_table = 'hxb_pedestal_test'
                elif 'iv_' in column_name:
                    source_table = 'module_iv_test'

                original_column = column_name.split('_', 1)[1]  # Remove prefix
                columns.append((column_name, data_type, key, source_table, original_column))
    return columns

columns = parse_csv_schema(loc + 'module_qc_summary.csv')

async def create_module_qc_summary_table(conn, columns):
    # Start with the primary key defined explicitly using SERIAL
    primary_key_sql = "mod_qc_no SERIAL PRIMARY KEY"
    sql_column_definitions = [primary_key_sql]  # Start with primary key in definition

    # Generate column definitions for other columns from CSV (skipping the primary key column)
    for col in columns:
        column_name, data_type, key, source_table, original_column = col
        if source_table:  # Only handling columns that are actually derived from other tables
            sql_part = f"{column_name} {data_type}"
            sql_column_definitions.append(sql_part)

    sql_create = f"""
    CREATE TABLE module_qc_summary (
        {', '.join(sql_column_definitions)}
    );
    """

    # Prepare the SELECT clause for the INSERT statement
    sql_select_parts = []
    for col in columns:
        if col[3]:  # Check if there's a source table
            column_name, data_type, key, source_table, original_column = col
            sql_part = f"{source_table}.{original_column} AS {column_name}"
            sql_select_parts.append(sql_part)

    select_clause = ', '.join(sql_select_parts)

    # Insert data into the table
    sql_insert = f"""
    INSERT INTO {table_name} ({', '.join([col[0] for col in columns if col[3]])})
    SELECT {select_clause}
    FROM proto_inspect, module_inspect, hxb_pedestal_test, module_iv_test;
    """

    await conn.execute(sql_create)
    await conn.execute(sql_insert)
    print("Table created and data inserted successfully.")

async def main():
    conn = await asyncpg.connect(**db_params)
    try:
        schema_name = 'public'
        table_name = 'module_qc_summary'
        
        ## check if the module_qc_summary exists
        table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2);"
        table_exists = await conn.fetchval(table_exists_query, schema_name, table_name)
        
        if table_exists:
            print(f"Table {table_name} already exists.")
            # Here you can decide to return, or maybe alter the table, or perform other actions
            return
        ## if the table has not existed, then create it. 
        await create_module_qc_summary_table(conn, columns)
    finally:
        await conn.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
