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
    sql_parts = []
    for col in columns:
        column_name, data_type, key, source_table, original_column = col
        if source_table is None:
            sql_part = f"{column_name} {data_type}"  # Handle special cases like primary key
        else:
            sql_part = f"{source_table}.{original_column} AS {column_name}"

        if key == 'PRIMARY KEY':
            sql_part += f" {key}"

        sql_parts.append(sql_part)

    select_clause = ', '.join(sql_parts)
    sql = f"""
    CREATE TABLE module_qc_summary AS
    SELECT {select_clause}
    FROM proto_inspect, module_inspect, hxb_pedestal_test, module_iv_test;  -- Adjust JOINs as needed
    """
    await conn.execute(sql)
    
async def main():
    conn = await asyncpg.connect(**db_params)
    try:
        await create_module_qc_summary_table(conn, columns)
    finally:
        await conn.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
