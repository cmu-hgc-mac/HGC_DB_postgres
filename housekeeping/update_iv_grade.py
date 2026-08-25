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
conn_info = yaml.safe_load(open(conn_yaml_file, 'r'))

db_params = {
    'database': conn_info.get('dbname'),
    'user': 'editor',
    'host': conn_info.get('db_hostname'),
    'port': conn_info.get('port'),
}

## Database connection parameters for new database
if args.password is None:
    dbpassword = pwinput.pwinput(prompt='Enter superuser password: ', mask='*')
else:
    if args.encrypt_key is None:
        print("Encryption key not provided. Exiting.."); exit()
    cipher_suite = Fernet((args.encrypt_key).encode())
    dbpassword = cipher_suite.decrypt( base64.urlsafe_b64decode(args.password)).decode() ## Decode base64 to get encrypted string and then decrypt

db_params.update({'password': dbpassword})

I_500V_LIMIT_A = 100e-6
I_500V_BREAKDOWN_A = 0.995e-3
I_HALF_VDEP_LIMIT_A = 10e-6

# sensor thickness digit in module_name (320XXX(_)XXIINNNN) -> depletion voltage (V)
DEP_V_BY_THICKNESS = {'3': 240, '2': 110, '1': 50}
DEFAULT_DEP_V = 50

def get_dep_v(module_name):
    thickness_digit = module_name[6]
    return DEP_V_BY_THICKNESS.get(thickness_digit, DEFAULT_DEP_V)

def current_at_voltage(meas_v, meas_i, target_v):
    """Current at the measured voltage closest to (but not exceeding) target_v, by |V|."""
    if not meas_v or not meas_i:
        return None
    v_arr = np.abs(np.array(meas_v, dtype=float))
    i_arr = np.abs(np.array(meas_i, dtype=float))
    within = v_arr[v_arr <= target_v]
    if len(within) == 0:
        return None
    closest_v = within.max()
    return float(i_arr[v_arr == closest_v][0])

def compute_grade(module_name, meas_v, meas_i):
    i_500v = current_at_voltage(meas_v, meas_i, 500)
    if i_500v is None:
        return None

    if i_500v < I_500V_LIMIT_A:
        return 'A'
    if i_500v < I_500V_BREAKDOWN_A:
        return 'B'

    dep_v = get_dep_v(module_name)
    i_half_dep_v = current_at_voltage(meas_v, meas_i, dep_v / 2)
    if i_half_dep_v is None:
        return None

    if i_half_dep_v < I_HALF_VDEP_LIMIT_A:
        return 'C'
    return 'F'

def in_grading_conditions(temp_c, rel_hum):
    try:
        temp = float(temp_c)
    except (TypeError, ValueError):
        return False

    if temp < -30:
        return True

    if temp > 0:
        try:
            rh = float(rel_hum)
        except (TypeError, ValueError):
            return False
        return rh < 12

    return False

async def update_module_iv_test_grade():
    conn = await asyncpg.connect(**db_params)

    try:
        rows = await conn.fetch("""
            SELECT mod_ivtest_no, module_name, temp_c, rel_hum, meas_v, meas_i
            FROM module_iv_test;
        """)

        updated, skipped = 0, 0
        for row in rows:
            if not in_grading_conditions(row['temp_c'], row['rel_hum']):
                skipped += 1
                continue

            grade = compute_grade(row['module_name'], row['meas_v'], row['meas_i'])
            if grade is None:
                skipped += 1
                continue

            await conn.execute(
                "UPDATE module_iv_test SET grade = $1 WHERE mod_ivtest_no = $2;",
                grade, row['mod_ivtest_no']
            )
            updated += 1

        print(f"IV grade updated for {updated} rows in module_iv_test ({skipped} skipped).")

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()

    await conn.close()

asyncio.run(update_module_iv_test_grade())
