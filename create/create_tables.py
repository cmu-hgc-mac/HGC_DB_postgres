import asyncio, asyncpg
import glob, os, csv, yaml
import numpy as np
import pwinput

print('Creating tables in the database...')
# Database connection parameters
db_params = {
    'database': (open('../dbase_info/dbfname.txt', 'r').read()).split('\n')[0].split(' ')[0],
    'user': 'postgres',
    'password': pwinput.pwinput(prompt='Enter superuser password: ', mask='*'),
    'host': 'localhost',
    'port': '5432'
}

async def create_tables():
    # Connect to the database
    conn = await asyncpg.connect(**db_params)
    schema_name = 'public'  # Change this if your tables are in a different schema
    print('Connection successful. \n')

    def get_csv_fname(loc):
        os.chdir(loc)
        fnameLs = glob.glob("*.csv")
        return fnameLs

    def get_table_info(loc, fname):
        with open(loc + fname, mode='r') as file:
            csvFile = csv.reader(file)
            rows = []
            for row in csvFile:
                rows.append(row)
            temp = np.array(rows).T
            fk = temp[0][(np.where(temp[-1] != ''))]
            fk_ref = temp[-2][(np.where(temp[-1] != ''))]
            fk_tab = temp[-1][(np.where(temp[-1] != ''))]
            return fname.split('.csv')[0], temp[0], temp[1], fk, fk_ref, fk_tab  ### fk, fk_tab are returned as lists

    def get_column_names(col1_list, col2_list, fk_name, fk_ref, parent_table):
        combined_list = []
        for item1, item2 in zip(col1_list, col2_list):
            combined_list.append(f'{item1} {item2}')
        table_columns = ', '.join(combined_list)
        if fk_name.size != 0:
            table_columns += f', CONSTRAINT {fk_ref[0]} FOREIGN KEY({fk_name[0]}) REFERENCES {parent_table[0]}({fk_name[0]})'
        return table_columns

    async def create_table(table_name, table_columns):
        # Check if the table exists
        table_exists_query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2);"
        table_exists = await conn.fetchval(table_exists_query, schema_name, table_name)
        if not table_exists:
            create_table_query = f""" CREATE TABLE {table_name} ( {table_columns} ); """
            await conn.execute(create_table_query)
            print(f"Table '{table_name}' created successfully.")
        else:
            print(f"Table '{table_name}' already exists.")

    async def allow_perm(table_name, permission, user):
        await conn.execute(f"GRANT {permission} ON {table_name} TO {user};")
        print(f"Table '{table_name}' has {permission} access granted to {user}.")

    async def allow_schema_perm(user):
        await conn.execute(f"GRANT USAGE ON SCHEMA public TO {user};")
        await conn.execute(f"GRANT SELECT ON information_schema.tables TO {user};")
        print(f"Schema permission access granted to {user}.")

    # fname_list = ['module_info.csv',
    #               'module_assembly.csv', 
    #               'proto_assembly.csv', 
    #               'baseplate.csv', 
    #               'sensor.csv', 
    #               'hexaboard.csv', 
    #               'bp_inspect.csv', 
    #               'hxb_inspect.csv', 
    #               'hxb_pedestal_test.csv', 
    #               'proto_inspect.csv', 
    #               'module_inspect.csv', 
    #               'module_iv_test.csv', 
    #               'module_pedestal_test.csv', 
    #               'module_pedestal_plots.csv']   

    # Function creation SQL
    create_function_sql = """
        CREATE OR REPLACE FUNCTION notify_insert()
        RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify('incoming_data_notification', '');
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    
    create_trigger_sql_template = """
        CREATE TRIGGER {table_name}_insert_trigger
        AFTER INSERT ON {table_name}
        FOR EACH ROW
        EXECUTE FUNCTION notify_insert();
        """

    try:
        # Create a cursor and execute the function creation SQL
        async with conn.transaction():
            await conn.execute(create_function_sql)

        ## Define the table name and schema
        loc = '../dbase_info/'
        yaml_file = f'{loc}tables.yaml'

        with open(yaml_file, 'r') as file:
            data = yaml.safe_load(file)

            for i in data['users']:
                username = f"{i['username']}"
                await allow_schema_perm(username)

            print('\n')

            for i in data['tables']:
                fname = f"{(i['fname'])}"
                print(f'Getting info from {fname}...')
                table_name, table_header, dat_type, fk_name, fk_ref, parent_table = get_table_info(loc, fname)
                table_columns = get_column_names(table_header, dat_type, fk_name, fk_ref, parent_table)
                await create_table(table_name, table_columns)
                try:
                    create_trigger_sql = create_trigger_sql_template.format(table_name=table_name)
                    await conn.execute(create_trigger_sql)
                    for k in i['permission'].keys():
                        await allow_perm(table_name, i['permission'][k], k)
                except:
                    print('Either trigger or permissions already exist.')
                print('\n')
    
    except asyncpg.PostgresError as e:
        print("Error:", e)
    finally:
        await conn.close()

asyncio.run(create_tables())
