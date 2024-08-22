import asyncio
import asyncpg
import yaml
import sys
sys.path.append('../')
import os

async def fill_foreign_keys():
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
        # Fetch all rows from module_iv_test where module_no is NULL
        fetch_query = """
            SELECT proto_name 
            FROM proto_inspect
            WHERE proto_no IS NULL
        """
        
        print("Fetching rows where proto_no is NULL...")
        rows = await conn.fetch(fetch_query)
        print(f"Fetched {len(rows)} rows.")
        
        # Iterate over each row and update the foreign key
        for row in rows:
            module_name = row['proto_name']

            update_query = """
                UPDATE proto_inspect
                SET proto_no = (
                    SELECT mi.proto_no
                    FROM proto_assembly mi
                    WHERE mi.proto_name = $1
                )
                WHERE proto_name = $1
            """
            print(update_query)
            # Execute the update query for each row
            result = await conn.execute(update_query, module_name)
            print(f"Updated rows for proto_name '{module_name}': {result}")

    finally:
        # Close the connection
        await conn.close()

# Running the async function
asyncio.run(fill_foreign_keys())
