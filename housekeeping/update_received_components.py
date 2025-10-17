import asyncio, asyncpg
import glob, os, csv, yaml, argparse, base64, traceback
import numpy as np
import pwinput
from cryptography.fernet import Fernet

parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
parser.add_argument('-p', '--password', default=None, required=False, help="Password to access database.")
parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
args = parser.parse_args()

# Database connection parameters
loc = 'dbase_info'
tables_subdir = 'postgres_tables'
table_yaml_file = os.path.join(loc, 'tables.yaml')
conn_yaml_file = os.path.join(loc, 'conn.yaml')

db_params = {
    'database': yaml.safe_load(open(conn_yaml_file, 'r')).get('dbname'),
    'user': 'shipper',
    'host': yaml.safe_load(open(conn_yaml_file, 'r')).get('db_hostname'),
    'port': yaml.safe_load(open(conn_yaml_file, 'r')).get('port'),
}

if args.password is None:
        dbpassword = pwinput.pwinput(prompt='Enter superuser password: ', mask='*')
else:
    if args.encrypt_key is None:
        print("Encryption key not provided. Exiting..."); exit()
    cipher_suite = Fernet((args.encrypt_key).encode())
    dbpassword = cipher_suite.decrypt( base64.urlsafe_b64decode(args.password)).decode() ## Decode base64 to get encrypted string and then decrypt
    db_params.update({'password': dbpassword})

async def update_date_received():
    conn = await asyncpg.connect(**db_params)
    print('Connection successful.')
        
    try:
        #======= Hexaboard =======#
        update_hxb = """
        DO $$
        DECLARE
            rec RECORD;
            min_date DATE;
        BEGIN
            FOR rec IN (
                SELECT DISTINCT hxb_name, REPLACE(hxb_name, '-', '') AS selected_hxb
                FROM (
                    SELECT hxb_name FROM hxb_pedestal_test
                    UNION
                    SELECT hxb_name FROM module_assembly
                    UNION
                    SELECT hxb_name FROM hxb_inspect
                ) AS all_hxbs
            ) LOOP

                SELECT MIN(date) INTO min_date
                FROM (
                    SELECT date_test AS date FROM hxb_pedestal_test WHERE REPLACE(hxb_name, '-', '') = rec.selected_hxb
                    UNION ALL
                    SELECT ass_run_date AS date FROM module_assembly WHERE REPLACE(hxb_name, '-', '') = rec.selected_hxb
                    UNION ALL
                    SELECT date_inspect AS date FROM hxb_inspect WHERE REPLACE(hxb_name, '-', '') = rec.selected_hxb
                ) AS all_dates;
                
                IF min_date IS NULL THEN
                CONTINUE;
                END IF;

                IF EXISTS (
                    SELECT 1 FROM hexaboard
                    WHERE REPLACE(hxb_name, '-', '') = rec.selected_hxb
                ) THEN
                    UPDATE hexaboard
                    SET date_verify_received = min_date
                    WHERE REPLACE(hxb_name, '-', '') = rec.selected_hxb
                        AND date_verify_received IS NULL;
                ELSE
                    INSERT INTO hexaboard (hxb_name, date_verify_received)
                    VALUES (rec.hxb_name, min_date);
                END IF;

            END LOOP;
        END $$;
        """

        #======= Sensor =======#
        update_sensor = """
        DO $$
        DECLARE
            rec RECORD;
            min_date DATE;
        BEGIN
            FOR rec IN (
                SELECT DISTINCT sen_name, REPLACE(sen_name, '-', '') AS selected_sensor
                FROM (SELECT sen_name FROM proto_assembly) AS all_sensors
            ) LOOP

                SELECT MIN(date) INTO min_date
                FROM (
                    SELECT ass_run_date AS date FROM proto_assembly WHERE REPLACE(sen_name, '-', '') = rec.selected_sensor
                ) AS all_dates;
                
                IF min_date IS NULL THEN
                CONTINUE;
                END IF;

                IF EXISTS (
                    SELECT 1 FROM sensor
                    WHERE REPLACE(sen_name, '-', '') = rec.selected_sensor
                ) THEN
                    UPDATE sensor
                    SET date_verify_received = min_date
                    WHERE REPLACE(sen_name, '-', '') = rec.selected_sensor
                        AND date_verify_received IS NULL;
                ELSE
                    INSERT INTO sensor (sen_name, date_verify_received)
                    VALUES (rec.sen_name, min_date);
                END IF;

            END LOOP;
        END $$;
        """

        #======= Baseplate =======#
        update_baseplate = """
        DO $$
        DECLARE
            rec RECORD;
            min_date DATE;
        BEGIN
            FOR rec IN (
                SELECT DISTINCT bp_name, REPLACE(bp_name, '-', '') AS selected_bp
                FROM (
                    SELECT bp_name FROM proto_assembly
                    UNION
                    SELECT bp_name FROM bp_inspect
                ) AS all_bps
            ) LOOP

                SELECT MIN(date) INTO min_date
                FROM (
                    SELECT ass_run_date AS date FROM proto_assembly WHERE REPLACE(bp_name, '-', '') = rec.selected_bp
                    UNION ALL
                    SELECT date_inspect AS date FROM bp_inspect WHERE REPLACE(bp_name, '-', '') = rec.selected_bp
                ) AS all_dates;
                
                IF min_date IS NULL THEN
                CONTINUE;
                END IF;

                IF EXISTS (
                    SELECT 1 FROM baseplate
                    WHERE REPLACE(bp_name, '-', '') = rec.selected_bp
                ) THEN
                    UPDATE baseplate
                    SET date_verify_received = min_date
                    WHERE REPLACE(bp_name, '-', '') = rec.selected_bp
                        AND date_verify_received IS NULL;
                ELSE
                    INSERT INTO baseplate (bp_name, date_verify_received)
                    VALUES (rec.bp_name, min_date);
                END IF;

            END LOOP;    
        END $$;
        """

        result = await conn.execute(update_hxb)
        print(f"Hexaboards date_verify_received updated.")
        result = await conn.execute(update_sensor)
        print(f"Sensors date_verify_received updated.")
        result = await conn.execute(update_baseplate)
        print(f"Baseplates date_verify_received updated.")

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
    
    await conn.close()

asyncio.run(update_date_received())