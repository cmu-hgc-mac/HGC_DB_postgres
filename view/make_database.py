'''
SQL Tables should only have to be made once. After that any modifications to the table should be locked with a password.
Tables that we need based on the OGP measurements so far:
'''

import psycopg2
import sshtunnel
# import pwinput


print("Creating a new database...")
# Database connection parameters for new database
db_params = {
    'dbname': 'testdb',
    'user': 'postgres',   # Assuming this is the superuser
    'password': input('Set superuser password: /n'),
    # 'password': pwinput.pwinput(prompt='Set superuser password: ', mask='*'),
    'host': 'localhost',  # Change this if your PostgreSQL server is on a different host
    'port': '5432'        # Default PostgreSQL port
}

# Connect to the default PostgreSQL database
default_conn = psycopg2.connect(dbname='postgres', user='postgres', password='hgcal', host='localhost', port='5432')
default_conn.autocommit = True
default_cursor = default_conn.cursor()

# Create a new database
db_name = db_params['dbname']
create_db_query = f"CREATE DATABASE {db_name};"
try:
    default_cursor.execute(create_db_query)
    print(f"Database '{db_name}' successfully created.")
except:
    print(f"Database '{db_name}' already exists. New database has NOT been created.")

default_cursor.close()
default_conn.close()

# Connect to the newly created database
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()
print(f"Connected to database '{db_name}' successfully.\n")

# Create user roles and assign privileges
def create_role(role_name, user_type):
    # Create the new role (user) if it doesn't exist
    try:
        # user_password = pwinput.pwinput(prompt='Set {} password: '.format(user_type), mask='*')
        user_password = input('Set {} password: '.format(user_type))
        create_role_query = "CREATE ROLE {} LOGIN PASSWORD '{}';".format(role_name, user_password)
        cursor.execute(create_role_query)
        print("Role '{}' for '{}' created.".format(role_name, user_type))
    # except psycopg2.errors.DuplicateObject as e:
    except:
        print(f"Role '{role_name}' already exists. Continuing...")
    
    try:
        cursor.execute("GRANT CONNECT ON DATABASE {} TO {};".format(db_name, role_name))
        cursor.execute(f"GRANT USAGE ON SCHEMA public TO {role_name};")
    except:
        print(f"Permissions for '{role_name}' already exist.\n")


role_names_list = ['ogp_user', 'lv_user', 'viewer']
role_types_list = ['OGP computer user', 'Gantry computer user', 'table viewer']
for i in range(len(role_names_list)):
    create_role(role_names_list[i], role_types_list[i])

# cursor.execute("CREATE ROLE test_user LOGIN PASSWORD '"+pwinput.pwinput(prompt='Set test user password: ', mask='*')+"';")
# cursor.execute("GRANT CONNECT ON DATABASE {} TO test_user;".format(db_name))
# cursor.execute("GRANT USAGE ON SCHEMA public TO test_user;")

### cursor.execute("CREATE ROLE lv_user LOGIN PASSWORD '"+pwinput.pwinput(prompt='Set Gantry computer user password: ', mask='*')+"';")
### cursor.execute("CREATE ROLE viewer LOGIN PASSWORD '"+pwinput.pwinput(prompt='Set table viewer password: ', mask='*')+"';")

### cursor.execute("GRANT CONNECT ON DATABASE {} TO lv_user;".format(db_name))
### cursor.execute("GRANT USAGE ON SCHEMA public TO lv_user;")
### cursor.execute("GRANT CONNECT ON DATABASE {} TO viewer;".format(db_name))
### cursor.execute("GRANT USAGE ON SCHEMA public TO viewer;")


# Create tables in the new database
# create_table_query = """
#     CREATE TABLE tab1 (
#         id SERIAL PRIMARY KEY,
#         name VARCHAR(100)
#     );
    
#     CREATE TABLE tab2 (
#         id SERIAL PRIMARY KEY,
#         info TEXT
#     );
# """
# cursor.execute(create_table_query)
# cursor.execute("GRANT INSERT, SELECT ON tab1 TO ogp_user;")
# cursor.execute("GRANT INSERT, SELECT ON tab2 TO lv_user;")
# cursor.execute("GRANT SELECT ON tab1, tab2 TO lv_user;")

conn.commit()

# Close the connection
cursor.close()
conn.close()

print("Full success!!!\n")


# REASSIGN OWNED BY ryan TO <newuser>;
# DROP OWNED BY ryan;
# REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM ryan;
# REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM ryan;
# REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public FROM ryan;
# DROP USER ryan;