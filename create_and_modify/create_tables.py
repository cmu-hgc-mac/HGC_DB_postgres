import asyncio, asyncpg
import glob, os, csv, yaml, argparse, base64, traceback
import numpy as np
import pwinput
from cryptography.fernet import Fernet

parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
parser.add_argument('-p', '--password', default=None, required=False, help="Password to access database.")
parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
args = parser.parse_args()

print('Creating tables in the database...')
# Database connection parameters
loc = 'dbase_info'
tables_subdir = 'postgres_tables'
table_yaml_file = os.path.join(loc, 'tables.yaml')
conn_yaml_file = os.path.join(loc, 'conn.yaml')
conn_info = yaml.safe_load(open(conn_yaml_file, 'r'))
db_params = {
    'database': conn_info.get('dbname'),
    'user': 'postgres',
    'host': conn_info.get('db_hostname'),
    'port': conn_info.get('port'),
    'password': 'hgcal'}

if args.password is None:
    dbpassword = pwinput.pwinput(prompt='Enter superuser password: ', mask='*')
else:
    if args.encrypt_key is None:
        print("Encryption key not provided. Exiting..."); exit()
    cipher_suite = Fernet((args.encrypt_key).encode())
    db_params.update({'password': cipher_suite.decrypt( base64.urlsafe_b64decode(args.password)).decode()}) ## Decode base64 to get encrypted string and then decrypt

def get_csv_fname(loc):
    os.chdir(loc)
    fnameLs = glob.glob("*.csv")
    return fnameLs

def get_column_names(col1_list, col2_list, fk_name, fk_ref, parent_table):
    combined_list = []
    for item1, item2 in zip(col1_list, col2_list):
        combined_list.append(f'{item1} {item2}')
    table_columns_with_type = ', '.join(combined_list)
    if fk_name.size != 0:
        table_columns_with_type += f', CONSTRAINT {fk_ref[0]} FOREIGN KEY({fk_name[0]}) REFERENCES {parent_table[0]}({fk_name[0]})'
    return table_columns_with_type

def get_table_info(loc, tables_subdir, fname):
    with open(os.path.join(loc, tables_subdir, fname) , mode='r') as file:
        csvFile = csv.reader(file, quotechar='"')
        rows = []
        for row in csvFile:
            rows.append(row)
        columns = np.array(rows).T
        comment_columns = columns[2] 
        fk = columns[0][(np.where(columns[-1] != ''))]
        fk_ref = columns[-2][(np.where(columns[-1] != ''))]
        fk_tab = columns[-1][(np.where(columns[-1] != ''))]
        ### returning table_name, table_header, dat_type, fk_name, fk_ref, parent_table, comment_columns
        return fname.split('.csv')[0], columns[0], columns[1], fk, fk_ref, fk_tab, comment_columns  ### fk, fk_tab are returned as lists

async def create_tables_sequence():
    # Connect to the database
    conn = await asyncpg.connect(**db_params)
    schema_name = 'public'  # Change this if your tables are in a different schema
    print('Connection successful. \n')

    async def create_table(table_name, table_columns_with_type, comment_columns = None, table_headers = None):
        # Check if the table exists
        table_exists_query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2);"
        table_exists = await conn.fetchval(table_exists_query, schema_name, table_name)
        if not table_exists:
            create_table_query = f""" CREATE TABLE {table_name} ( {table_columns_with_type} ); """
            await conn.execute(create_table_query)
            for t in range(len(table_headers)):
                set_comment_query = f"COMMENT ON COLUMN {table_name}.{table_headers[t]} IS '{comment_columns[t]}';"
                await conn.execute(set_comment_query) 
            print(f"Table '{table_name}' created successfully.")
        else:
            print(f"Table '{table_name}' already exists.")

    async def allow_perm(table_name, permission, user):
        await conn.execute(f"GRANT {permission} ON {table_name} TO {user};")
        print(f"Table '{table_name}' has {permission} access granted to {user}.")

    async def allow_seq_perm(seq_name, user):
        await conn.execute(f"GRANT USAGE ON {seq_name} TO {user};")
        print(f"Sequence '{seq_name}' has USAGE granted to {user}.")

    async def allow_schema_perm(user):
        #await conn.execute(f"GRANT USAGE ON SCHEMA public TO {user};")
        #await conn.execute(f"GRANT SELECT ON information_schema.tables TO {user};")
        print(f"Schema permission access granted to {user}.")

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

    # SQL query for updating the foreign key:
    def update_foreign_key_trigger(table_name, fk_identifier, fk, fk_table):
        trigger_sql = f"""
        CREATE OR REPLACE FUNCTION {table_name}_update_foreign_key()
        RETURNS TRIGGER AS $$
        BEGIN
            UPDATE {table_name}
            SET {fk} = {fk_table}.{fk}
            FROM {fk_table} 
            WHERE ({table_name}.{fk} IS NULL OR {table_name}.{fk} IS DISTINCT FROM {fk_table}.{fk})
                AND REPLACE({table_name}.{fk_identifier},'-','') = REPLACE({fk_table}.{fk_identifier},'-','');
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER {table_name}_update_foreign_key_trigger
        AFTER INSERT OR UPDATE ON {table_name}
        FOR EACH ROW
        EXECUTE FUNCTION {table_name}_update_foreign_key();
        """
        return trigger_sql

    def get_table_info_fk(loc, tables_subdir, fname):
        with open(os.path.join(loc, tables_subdir, fname) , mode='r') as file:
            csvFile = csv.reader(file)
            rows = []
            for row in csvFile:
                rows.append(row)
            columns = np.array(rows).T
            if 'fk_identifier' in columns[-2]:
                fk_itentifier = columns[0][(np.where(columns[-2] == 'fk_identifier'))][0]
                fk = columns[0][(np.where(columns[-1] != ''))][0]
                fk_ref = columns[-2][(np.where(columns[-1] != ''))][0]
                fk_tab = columns[-1][(np.where(columns[-1] != ''))][0]
                return (fname.split('.csv')[0]).split('/')[-1], fk_itentifier, fk, fk_tab, fk_ref  
            return (fname.split('.csv')[0]).split('/')[-1], None, None, None, None

    # SQL quiery for updating tables data:
    def update_table_datas_trigger(target_table, target_col, source_table, source_col, replace_col, function, i):

        if function == 'name': 
            trigger_sql = f"""
            CREATE OR REPLACE FUNCTION {target_table}_{target_col}_update_data()
            RETURNS TRIGGER AS $$
            BEGIN
                UPDATE {target_table}
                SET {target_col} = REPLACE(COALESCE({target_table}.{target_col},NEW.{source_col}),'-','')
                WHERE REPLACE({target_table}.{replace_col},'-','') = REPLACE(NEW.{replace_col},'-','')
                    AND {target_table}.{target_col} IS NULL;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            CREATE TRIGGER {target_table}_{target_col}_update_data_trigger
            AFTER INSERT OR UPDATE ON {source_table}
            FOR EACH ROW
                EXECUTE FUNCTION {target_table}_{target_col}_update_data();
            """

        elif function == 'time':
            trigger_sql = f"""
            CREATE OR REPLACE FUNCTION {target_table}_{target_col}_update_data()
            RETURNS TRIGGER AS $$
            BEGIN
                UPDATE {target_table}
                SET {target_col} = NEW.{source_col}
                WHERE REPLACE({target_table}.{replace_col},'-','') = REPLACE(NEW.{replace_col},'-','')
                    AND {target_table}.{target_col} IS NULL;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            CREATE TRIGGER {target_table}_{target_col}_update_data_trigger
            AFTER INSERT OR UPDATE ON {source_table}
            FOR EACH ROW
                EXECUTE FUNCTION {target_table}_{target_col}_update_data();
            """
        
        elif function == 'update':
            trigger_sql = f"""
            CREATE OR REPLACE FUNCTION {target_table}_{target_col}_update_data()
            RETURNS TRIGGER AS $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM {target_table}
                    WHERE REPLACE({replace_col}, '-', '') = REPLACE(NEW.{replace_col}, '-', '')
                        AND {target_table}.{target_col} IS NULL
                ) THEN
                    UPDATE {target_table}
                    SET {target_col} = NEW.{source_col}
                    WHERE REPLACE({replace_col}, '-', '') = REPLACE(NEW.{replace_col}, '-', '');
                ELSE
                    INSERT INTO {target_table} ({replace_col}, {target_col})
                    VALUES (NEW.{replace_col}, NEW.{source_col});
                END IF;

                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            CREATE TRIGGER {target_table}_{target_col}_update_data_trigger
            AFTER INSERT OR UPDATE ON {source_table}
            FOR EACH ROW 
            EXECUTE FUNCTION {target_table}_{i}_update_data();
            """

        return trigger_sql
    
    def get_table_info_data(loc, fname):
        results = []
        with open(os.path.join(loc, fname) , mode='r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                target_table = row['target_table'].strip()
                target_col = row['target_col'].strip()
                source_table = row['source_table'].strip()
                source_col  = row['source_col'].strip()
                replace_col = row['replace_col'].strip()
                function = row['function'].strip()
                results.append([target_table, target_col, source_table, source_col, replace_col,function])
        return results


    try:
        ## Define the table name and schema
        with open(table_yaml_file, 'r') as file:
            data = yaml.safe_load(file)
            print('\n')

            for i in data.get('tables'):
                fname = f"{(i['fname'])}"
                print(f'Getting info from {fname}...')

                table_name, table_header, dat_type, fk_name, fk_ref, parent_table, comment_columns = get_table_info(loc, tables_subdir, fname)
                table_columns_with_type = get_column_names(table_header, dat_type, fk_name, fk_ref, parent_table)
                await create_table(table_name=table_name, table_columns_with_type=table_columns_with_type, comment_columns=comment_columns, table_headers=table_header)
                pk_seq = f'{table_name}_{table_header[0]}_seq'

                try:
                    create_trigger_sql = create_trigger_sql_template.format(table_name=table_name)
                    await conn.execute(create_trigger_sql)
                except:
                    print('Trigger already exists..')

                # Create the trigger for the foreign key:
                target_table, fk_identifier, fk, fk_table, fk_reference = get_table_info_fk(loc, tables_subdir, fname)
                if fk_identifier is not None:
                    try:
                        await conn.execute(update_foreign_key_trigger(target_table, fk_identifier, fk, fk_table))
                        print(f' >> Foreign key trigger for {target_table} created...')
                    except:
                        drop_fk_trigger_sql = f"DROP TRIGGER IF EXISTS {target_table}_update_foreign_key_trigger ON {target_table};"
                        await conn.execute(drop_fk_trigger_sql.format(table_name=target_table))
                        await conn.execute(update_foreign_key_trigger(target_table, fk_identifier, fk, fk_table))
                        print(f' >> Foreign key trigger for {target_table} updated.')

                # Create the trigger for updating data:
                duplicate_datas = get_table_info_data('create_and_modify', 'duplicate_data.csv')
                for j in range(len(duplicate_datas)):
                    if duplicate_datas[j][0] == table_name:
                        try: 
                            await conn.execute(update_table_datas_trigger(*duplicate_datas[j],j))
                            print(f' >> Data update trigger for {duplicate_datas[j][0]}_{j} created for column {duplicate_datas[j][1]}...')
                        except:
                            drop_data_trigger_sql = f"DROP TRIGGER IF EXISTS {duplicate_datas[j][0]}_{j}_update_data_trigger ON {duplicate_datas[j][2]};"
                            await conn.execute(drop_data_trigger_sql.format(table_name=duplicate_datas[j][0]))
                            await conn.execute(update_table_datas_trigger(*duplicate_datas[j],j))
                            print(f' >> Data update trigger for {duplicate_datas[j][0]}_{j} updated for column {duplicate_datas[j][1]}...')

                # Allow permissions:
                for k in i['permission'].keys():
                    try:
                        await allow_perm(table_name, i['permission'][k], k)
                        if 'INSERT' in i['permission'][k]:
                            await allow_seq_perm(pk_seq, k)
                    except:
                        print(f'Permission {k} already exist.')
                
                print('\n')

        print("Granting UPDATE permission to teststand_user for front_wirebond.wb_fr_marked_done.")
        front_wirebond_done_query = "GRANT UPDATE (wb_fr_marked_done) ON front_wirebond TO teststand_user;"
        await conn.execute(front_wirebond_done_query)

    except asyncpg.PostgresError as e:
        print("Error:", e)
        traceback.print_exc()
    finally:
        await conn.close()

asyncio.run(create_tables_sequence())
