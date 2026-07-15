import os, sys, argparse, base64
import asyncio, asyncpg
import yaml, csv
from cryptography.fernet import Fernet
sys.path.append('../')
import pwinput
import numpy as np
from create_tables import get_table_info


# SQL query for updating the foreign key:
def update_foreign_key_trigger(table_name, fk_identifier, fk, fk_table):
    assemble_identifier = ['proto_assembly', 'module_info']
    components = ['baseplate', 'proto_assembly', 'sensor', 'hexaboard']

    # Check if the table is a component and the fk_table is proto_assembly or module_info
        # If so, update the corresponding component with the fk in the component table
    if fk_table in assemble_identifier and table_name in components:
        trigger_sql = f"""
        CREATE OR REPLACE FUNCTION {table_name}_update_{fk_table}_foreign_key()
        RETURNS TRIGGER AS $$
        BEGIN
            UPDATE {table_name}
            SET {fk} = NEW.{fk}
            WHERE REPLACE({fk_identifier},'-','') = REPLACE(new.{fk_identifier},'-','');
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS {table_name}_update_{fk_table}_foreign_key_trigger ON {fk_table};

        CREATE TRIGGER {table_name}_update_{fk_table}_foreign_key_trigger
        AFTER INSERT OR UPDATE OF {fk_identifier} ON {fk_table}
        FOR EACH ROW
        EXECUTE FUNCTION {table_name}_update_{fk_table}_foreign_key();
        """

    # In the other case, update the fk while inserting or updating the table
    else:
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

        DROP TRIGGER IF EXISTS {table_name}_update_foreign_key_trigger ON {table_name};

        CREATE TRIGGER {table_name}_update_foreign_key_trigger
        AFTER INSERT OR UPDATE OF {fk_identifier} ON {table_name}
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
    # Update name: proto_name/hxb_name from module_assembly -> module_info
    if function == 'name': 
        trigger_sql = f"""
        CREATE OR REPLACE FUNCTION {target_table}_{target_col}_update_name()
        RETURNS TRIGGER AS $$
        BEGIN
            UPDATE {target_table}
            SET {target_col} = REPLACE(COALESCE({target_table}.{target_col},NEW.{source_col}),'-','')
            WHERE REPLACE({target_table}.{replace_col},'-','') = REPLACE(NEW.{replace_col},'-','')
                AND {target_table}.{target_col} IS NULL;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS {target_table}_{target_col}_update_name_trigger ON {source_table};

        CREATE TRIGGER {target_table}_{target_col}_update_name_trigger
        AFTER INSERT OR UPDATE OF {source_col} ON {source_table}
        FOR EACH ROW
            EXECUTE FUNCTION {target_table}_{target_col}_update_name();
        """

    # Update time: date_inspect/date_bond/date_encap/date_test from corresponding table -> proto_assembly/module_assembly/module_info
    elif function == 'time':
        trigger_sql = f"""
        CREATE OR REPLACE FUNCTION {target_table}_{target_col}_update_time()
        RETURNS TRIGGER AS $$
        BEGIN
            UPDATE {target_table}
            SET {target_col} = NEW.{source_col}
            WHERE REPLACE({target_table}.{replace_col},'-','') = REPLACE(NEW.{replace_col},'-','')
                AND {target_table}.{target_col} IS NULL;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS {target_table}_{target_col}_update_time_trigger ON {source_table};

        CREATE TRIGGER {target_table}_{target_col}_update_time_trigger
        AFTER INSERT OR UPDATE OF {source_col} ON {source_table}
        FOR EACH ROW
            EXECUTE FUNCTION {target_table}_{target_col}_update_time();
        """
    
    # Upsert component recieved vertified time: from the test/assembly table -> component table
    elif function == 'update':
        trigger_sql = f"""
        CREATE OR REPLACE FUNCTION {target_table}_{target_col}_from_{source_table}()
        RETURNS TRIGGER AS $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM {target_table}
                WHERE REPLACE({replace_col}, '-', '') = REPLACE(NEW.{replace_col}, '-', '')
            ) THEN
                UPDATE {target_table}
                SET {target_col} = NEW.{source_col}
                WHERE REPLACE({replace_col}, '-', '') = REPLACE(NEW.{replace_col}, '-', '')
                    AND {target_table}.{target_col} IS NULL;
            ELSE
                INSERT INTO {target_table} ({replace_col}, {target_col})
                VALUES (NEW.{replace_col}, NEW.{source_col});
            END IF;

            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS {target_table}_{target_col}_{source_table}_trigger ON {source_table};

        CREATE TRIGGER {target_table}_{target_col}_{source_table}_trigger
        AFTER INSERT OR UPDATE OF {source_col} ON {source_table}
        FOR EACH ROW 
        EXECUTE FUNCTION {target_table}_{target_col}_from_{source_table}();
        """

    # Update sen_name/bp_name from module_assembly.proto_name -> module_info
    elif function == 'proto_lookup':
        trigger_sql = """
        CREATE OR REPLACE FUNCTION module_info_update_names_from_proto()
        RETURNS TRIGGER AS $$
        DECLARE
            v_bp text;
            v_sen text;
        BEGIN
            SELECT pa.bp_name, pa.sen_name
                INTO v_bp, v_sen
            FROM proto_assembly pa
            WHERE REPLACE(pa.proto_name,'-','') = REPLACE(NEW.proto_name,'-','')
            LIMIT 1;

            IF FOUND THEN
                UPDATE module_info tgt
                SET bp_name  = COALESCE(v_bp, tgt.bp_name),
                    sen_name = COALESCE(v_sen, tgt.sen_name)
                WHERE REPLACE(tgt.proto_name,'-','') = REPLACE(NEW.proto_name,'-','');
            END IF;

            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS module_info_update_names_trigger ON module_assembly;

        CREATE TRIGGER module_info_update_names_trigger
        AFTER INSERT OR UPDATE OF proto_name ON module_assembly
        FOR EACH ROW
        EXECUTE FUNCTION module_info_update_names_from_proto();
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

    # Try to delete all the existing triggers and trigger functions:
    try:
        delete_query = """
        DO $$
        DECLARE
            r RECORD;
        BEGIN
            -- 1) delete all the existing triggers
            FOR r IN
                SELECT t.tgname,
                    n.nspname,
                    c.relname
                FROM pg_trigger t
                JOIN pg_class   c ON c.oid = t.tgrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE NOT t.tgisinternal
                AND n.nspname NOT IN ('pg_catalog','pg_toast','information_schema')
            LOOP
                EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I.%I CASCADE;',
                            r.tgname, r.nspname, r.relname);
            END LOOP;

            -- 2) delete all the existing trigger functions
            FOR r IN
                SELECT n.nspname,
                    p.proname,
                    oidvectortypes(p.proargtypes) AS args
                FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE p.prorettype = 'trigger'::regtype
                AND n.nspname NOT IN ('pg_catalog','pg_toast','information_schema')
            LOOP
                EXECUTE format('DROP FUNCTION IF EXISTS %I.%I(%s) CASCADE;',
                            r.nspname, r.proname, r.args);
            END LOOP;
        END$$;
        """

        await conn.execute(delete_query)
        print(' >> All existing triggers and trigger functions deleted...')

    except:
        print(' >> No existing triggers and trigger functions found...')

    try:
        ## Define the table name and schema
        with open(table_yaml_file, 'r') as file:
            data = yaml.safe_load(file)
            print('\n')

            for i in data.get('tables'):
                fname = f"{(i['fname'])}"
                print(f'Creating triggers and trigger functions for table: {fname}...')

                table_name, table_header, dat_type, fk_name, fk_ref, parent_table, comment_columns = get_table_info(loc, tables_subdir, fname)

                # Create the trigger for the foreign key:
                target_table, fk_identifier, fk, fk_table, fk_reference = get_table_info_fk(loc, tables_subdir, fname)
                if fk_identifier is not None:
                    try:
                        await conn.execute(update_foreign_key_trigger(target_table, fk_identifier, fk, fk_table))
                        print(f' >> Foreign key trigger for {target_table} created.')
                    except:
                        raise

                # Create the trigger for updating data:
                duplicate_datas = get_table_info_data('create_and_modify', 'duplicate_data.csv')
                for j in range(len(duplicate_datas)):
                    if duplicate_datas[j][0] == table_name:
                        try: 
                            await conn.execute(update_table_datas_trigger(*duplicate_datas[j],j))
                            print(f' >> Data update trigger for {duplicate_datas[j][0]} created for column {duplicate_datas[j][1]}.')
                        except:
                            raise
    except: 
        raise
        print(' >> Error in creating triggers and trigger functions...')
    finally:
        await conn.close()

asyncio.run(main())