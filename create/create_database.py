'''
SQL Tables should only have to be made once. After that any modifications to the table should be locked with a password.
Tables that we need based on the OGP measurements so far:
'''

import asyncio, asyncpg, yaml, os, argparse

parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
parser.add_argument('-p', '--password', default=None, required=False, help="Password to access database.")
parser.add_argument('-up', '--userpass', default=None, required=False, help="Password to write to database.")
parser.add_argument('-vp', '--viewerpass', default=None, required=False, help="Password to view database.")
args = parser.parse_args()

dbpassword = str(args.password).replace(" ", "")
if dbpassword is None:
    dbpassword = input('Set superuser password: ')

user_password = str(args.userpass).replace(" ", "")
if user_password is None:
    user_password = input('Set user password: ')

viewer_password = str(args.viewerpass).replace(" ", "")
if viewer_password is None:
    viewer_password = input('Set viewer password: ')

async def create_db():
    print("Creating a new database...")
    ## Database connection parameters for new database
    loc = 'dbase_info'
    table_yaml_file = os.path.join(loc, 'tables.yaml')
    conn_yaml_file = os.path.join(loc, 'conn.yaml')
    db_params = {
        'database': yaml.safe_load(open(conn_yaml_file, 'r')).get('dbname'),
        'user': 'postgres',   
        'password': dbpassword,
        'host': yaml.safe_load(open(conn_yaml_file, 'r')).get('db_hostname'),  
        'port': yaml.safe_load(open(conn_yaml_file, 'r')).get('port'),        
    }

    # Connect to the default PostgreSQL database
    default_conn = await asyncpg.connect(user='postgres', password=dbpassword, host=yaml.safe_load(open(conn_yaml_file, 'r')).get('db_hostname'), port=yaml.safe_load(open(conn_yaml_file, 'r')).get('port'))

    # Create a new database
    db_name = db_params['database']
    print(f'Database name: {db_name}')
    create_db_query = f"CREATE DATABASE {db_name};"
    try:
        await default_conn.execute(create_db_query)
        print(f"Database '{db_name}' successfully created.")
    except asyncpg.exceptions.DuplicateDatabaseError:
        print(f"Database '{db_name}' already exists. New database has NOT been created.")

    await default_conn.close()

    # Connect to the newly created database
    conn = await asyncpg.connect(**db_params)
    print(f"Connected to database '{db_name}' successfully.\n")

    # Create user roles and assign privileges
    async def create_role(role_name, user_type):
        # Create the new role (user) if it doesn't exist
        try:
            if role_name.lower() == 'viewer':
                create_role_query = f"CREATE ROLE {role_name} LOGIN PASSWORD '{viewer_password}';"
            else:
                create_role_query = f"CREATE ROLE {role_name} LOGIN PASSWORD '{user_password}';"
            await conn.execute(create_role_query)
            print(f"Role '{role_name}' for '{user_type}' created.")
        except asyncpg.exceptions.DuplicateObjectError:
            print(f"Role '{role_name}' already exists. Continuing...")
        
        try:
            await conn.execute(f"GRANT CONNECT ON DATABASE {db_name} TO {role_name};")
            await conn.execute(f"GRANT USAGE ON SCHEMA public TO {role_name};")
        except asyncpg.exceptions.InsufficientPrivilegeError:
            print(f"Permissions for '{role_name}' already exist.\n")

    # Define user types
    with open(table_yaml_file, 'r') as file:
        data = yaml.safe_load(file)
        for u in data.get('users'):
            await create_role(u['username'], u['description'])

    await conn.close()
    print("Database creation successful!!!\n")

# if __name__ == "__main__":
asyncio.run(create_db())
