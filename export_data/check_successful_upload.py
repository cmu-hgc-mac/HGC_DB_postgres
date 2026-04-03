import asyncio
import asyncpg
import csv, pwinput
import os
import zipfile
from pathlib import Path
from datetime import datetime
import re, base64
import yaml
from cryptography.fernet import Fernet
import argparse
import glob
from src import get_conn, str2bool

## get the latest log csv file under mass_upload_logs (dbloader_batch_uploader_YYYYMMDDTTTTTT.csv)
## Open the csv file, find the value for "upload_status"
## if the upload_status is {upload status: boolean}= {"Already Uploaded": True, "Error": False, "Success": True}
## with the dictionary above, update the xml_upload_success column under a certain table in postgresql. 
## Make this table as a variable of a function. 

# --- CONFIG ---
LOG_DIR = Path("export_data/mass_upload_logs")
UPLOAD_STATUS_MAP = {
    "Already Uploaded": True,
    "Success": True,
    "State Timeout": False,
    "Error": False,
    "Copy Failed": False,
    "Processing Exception": False
}
PART_NAME_MAP = {
    'B': 'bp',
    'X': 'hxb',
    'P': 'proto',
    'M': 'module'
}

XML_UPLOAD_DIR = Path("export_data/xmls_for_upload")

YAML_MAP = "export_data/resource.yaml"        # local mapping file
with open(YAML_MAP) as f:
    yaml_data = yaml.safe_load(f)

def _parse_part_from_fname(fname: str):
    """
    Given a filename (no directory), return (prefix, part_name) or None.

    Filename formats:
      module/proto : 320MLF2WDCM0005_{ts}_{stem}.xml   -> part = 320MLF2WDCM0005
      hxb          : 320XLF4DME00891_CMU_{ts}_{stem}.xml -> part = 320XLF4DME00891
      bp           : 320BAFLWIH00996_CMU_{ts}_{stem}.xml  -> part = 320BAFLWIH00996
      sensor       : 200339_0_CMU_{ts}_{stem}.xml         -> part = 200339_0  (digits + _N)
    """
    tokens = os.path.splitext(fname)[0].split('_')
    first = tokens[0]

    if first.isdigit():
        # sensor: part_name = "{digits}_{N}"
        if len(tokens) < 2:
            return None
        part_name = f"{first}_{tokens[1]}"
        return 'sensor', part_name

    # 320-prefixed parts
    m = first.lstrip('320')
    if not m:
        return None
    prefix = PART_NAME_MAP.get(m[0], "")
    if not prefix:
        return None
    return prefix, first


def _find_local_zip(fname: str) -> Path:
    """
    Given a zip filename (no directory), find it under XML_UPLOAD_DIR by
    determining the likely subdirectory from the filename:
      - '_iv' in fname         -> testing/iv
      - '_pedestal' in fname   -> testing/pedestal
      - starts with '320M'     -> module
      - starts with '320P'     -> protomodule
      - starts with '320X'     -> hexaboard
      - starts with '320B'     -> baseplate
      - starts with digits     -> sensor
    Falls back to a recursive glob if not found in the expected directory.
    """
    first = fname.split('_')[0]
    if '_iv' in fname:
        candidate_dir = XML_UPLOAD_DIR / 'testing' / 'iv'
    elif '_pedestal' in fname:
        candidate_dir = XML_UPLOAD_DIR / 'testing' / 'pedestal'
    elif first.startswith('320M'):
        candidate_dir = XML_UPLOAD_DIR / 'module'
    elif first.startswith('320P'):
        candidate_dir = XML_UPLOAD_DIR / 'protomodule'
    elif first.startswith('320X'):
        candidate_dir = XML_UPLOAD_DIR / 'hexaboard'
    elif first.startswith('320B'):
        candidate_dir = XML_UPLOAD_DIR / 'baseplate'
    elif first.isdigit():
        candidate_dir = XML_UPLOAD_DIR / 'sensor'
    else:
        candidate_dir = XML_UPLOAD_DIR

    candidate = candidate_dir / fname
    if candidate.exists():
        return candidate

    # Fallback: search recursively
    matches = list(XML_UPLOAD_DIR.rglob(fname))
    if matches:
        return matches[0]
    return candidate  # return expected path even if missing (will raise on open)


def get_reflected_tables(file_path: str):
    """
    Return (prefix, part_name, tables) for a given xml or zip path.
    For zip files, returns a list of such tuples (one per member).
    For xml files, returns a single tuple (or None).
    """
    table_map = yaml_data["postgres_table_to_xml"]
    fname = os.path.basename(file_path)

    suffix_matching = {
        "wirebond":     "wirebond",
        "_grading":     "{prefix}_grading",
        "_cure_cond":   "{prefix}_cure_cond",
        "_inspection":  "{prefix}_inspection",
        "_build_upload":"{prefix}_info_or_assembly",
        "_assembly":    "{prefix}_assembly",
        "_iv_cond":     "{prefix}_iv_cond",
        "_iv":          "{prefix}_iv",
        "_pedestal":    "{prefix}_pedestal",
    }

    def _resolve_tables(prefix, path_str):
        for suffix, xml_type_tpl in suffix_matching.items():
            if suffix in path_str:
                xml_type = xml_type_tpl.replace("{prefix}", prefix)
                if xml_type == f"{prefix}_info_or_assembly":
                    if prefix == 'proto':
                        return None
                    xml_type = f"{prefix}_info" if prefix == 'module' else f"{prefix}_assembly"
                return table_map.get(xml_type)
        return None

    if fname.endswith('.zip'):
        results = []

        # For IV and pedestal zips, derive part info from the zip filename itself
        # without opening the archive (the path may be remote).
        # Filename pattern: {part_name}_{YYYYMMDDThhmmss}_{type}.zip
        is_testing = '_iv' in fname or '_pedestal' in fname
        if is_testing:
            ts_match = re.search(r'_(\d{8}T\d{6})_', fname)
            timestamp = ts_match.group(1) if ts_match else None
            parsed = _parse_part_from_fname(fname)
            if parsed is not None:
                prefix, part_name = parsed
                tables = _resolve_tables(prefix, fname)
                if tables:
                    results.append((prefix, part_name, tables, timestamp))
            return results

        local_zip = _find_local_zip(fname)
        try:
            with zipfile.ZipFile(local_zip, 'r') as zf:
                member_names = zf.namelist()
        except Exception as e:
            print(f"Could not open local zip {local_zip}:", e)
            member_names = []

        for member in member_names:
            parsed = _parse_part_from_fname(member)
            if parsed is None:
                continue
            prefix, part_name = parsed
            tables = _resolve_tables(prefix, member)
            if tables:
                results.append((prefix, part_name, tables, None))
        return results  # list of tuples

    else:
        parsed = _parse_part_from_fname(fname)
        if parsed is None:
            return None
        prefix, part_name = parsed
        tables = _resolve_tables(prefix, fname)
        if not tables:
            return None
        return prefix, part_name, tables, None  # single tuple


def get_upload_status_csv(csv_path):
    '''
    xml_path,upload_status,upload_state_value,upload_state_path,upload_log_path
    '''
    csv_output = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            xml_path = row["xml_path"]
            upload_status = row["upload_status"].strip()  # Refer to UPLOAD_STATUS_MAP

            if upload_status not in UPLOAD_STATUS_MAP:
                continue

            result = get_reflected_tables(xml_path)
            if result is None:
                continue

            # zip returns a list; xml returns a single tuple
            if isinstance(result, list):
                for prefix, part_name, tables, timestamp in result:
                    csv_output.append((prefix, part_name, upload_status, tables, timestamp))
            else:
                prefix, part_name, tables, timestamp = result
                csv_output.append((prefix, part_name, upload_status, tables, timestamp))

    return csv_output  # [(prefix, part_name, upload_status, db_tables, timestamp), ...]

# def get_api_data(search_id, db_type):
#     if db_type == 'cmsr':
#         url = f"https://hgcapi.web.cern.ch/mac/part/{search_id}/full"
#     elif db_type == 'int2r':
#         url = f"https://hgcapi-intg.web.cern.ch/mac/part/{search_id}/full"

async def _run_update(pool, query, args, sem):
    async with sem:
        async with pool.acquire() as conn:
            return await conn.execute(query, *args)

async def update_upload_status(pool, csv_output, concurrency=10):
    """
    Update 'xml_upload_success' column for relevant tables based on csv_output.

    Args:
        pool: asyncpg connection pool.
        csv_output: List of tuples (prefix, part_name, status, [table_names], timestamp).
                    timestamp is a 'YYYYMMDDThhmmss' string for IV/pedestal rows, else None.
    """
    sem = asyncio.Semaphore(concurrency)
    tasks = []

    for prefix, part_name, status, tables, timestamp in csv_output:
        success_flag = UPLOAD_STATUS_MAP.get(status)
        if success_flag is None:
            print(f"Unknown status '{status}' for part {part_name}, skipping.")
            continue

        # Parse timestamp into date/time for IV/pedestal rows
        test_date = test_time = None
        if timestamp:
            try:
                ts_dt = datetime.strptime(timestamp, "%Y%m%dT%H%M%S")
                test_date = ts_dt.date()
                test_time = ts_dt.time()
            except ValueError:
                print(f"Could not parse timestamp '{timestamp}' for {part_name}, skipping timestamp filter.")

        for table in tables:
            # Sanitize table name for safety: ensure it's alphanumeric + underscore only
            if not table.replace("_", "").isalnum():
                print(f"Skipping suspicious table name: {table}")
                continue
            name_col = 'sen_name' if prefix == 'sensor' else f'{prefix}_name'
            if test_date is not None:
                query = f"""
                    UPDATE {table}
                    SET xml_upload_success = $1
                    WHERE {name_col} = $2
                    AND date_test = $3
                    AND time_test = $4
                    AND xml_gen_datetime IS NOT NULL
                    AND (xml_upload_success IS NULL OR xml_upload_success = FALSE)
                """
                args = (success_flag, part_name, test_date, test_time)
            else:
                query = f"""
                    UPDATE {table}
                    SET xml_upload_success = $1
                    WHERE {name_col} = $2
                    AND xml_gen_datetime IS NOT NULL
                    AND (xml_upload_success IS NULL OR xml_upload_success = FALSE)
                """
                args = (success_flag, part_name)
            # print(f'Updating {part_name} in {table}...')
            tasks.append(_run_update(pool, query, args, sem))
    if not tasks:
        print("No valid update tasks found.")
        return

    # Run updates concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Report results
    total_updates = len(results)
    errors = [r for r in results if isinstance(r, Exception)]
    print(f"Attempted {total_updates} updates; {len(errors)} errors.")

    # Optional: print specific error info
    for e in errors:
        print(f"{type(e).__name__}: {e}")

def get_latest_upload_log():
    pattern = os.path.join(LOG_DIR, "*.csv")
    csv_files = glob.glob(pattern)

    if not csv_files:
        print(f"No CSV files found in {LOG_DIR}")
        return None

    # Sort by modification time (newest last)
    latest_file = max(csv_files, key=os.path.getmtime)
    print(f"Latest upload log: {latest_file}")
    return latest_file

async def main(dbpassword, db_type, encryption_key=None, consolidated_csv=None):
    # Connect to PostgreSQL
    pool = await get_conn(dbpassword, encryption_key, pool=True)
    # print("Connected to database.")

    if db_type == 'int2r':
        print("We do not update the upload status for INT2R")

    elif db_type == 'cmsr':
        try:
            # Use provided consolidated CSV or fall back to latest log
            massloader_log_csv = consolidated_csv if consolidated_csv else get_latest_upload_log()
            if not massloader_log_csv:
                print("No log file to process.")
                return

            # Assuming get_upload_status_csv() returns csv_output as described
            csv_output = get_upload_status_csv(massloader_log_csv)

            # Update DB
            async with pool.acquire() as conn:
                # await update_upload_status(conn, csv_output)
                await update_upload_status(pool, csv_output, concurrency=10)

        finally:
            await pool.close()
            # print("Database connection closed.")
            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script to process files in a directory.")
    parser.add_argument('-dbp', '--dbpassword', default=None, required=False, help="Password to access database.")
    parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
    parser.add_argument('-upld', '--upload_dev_stat', default='False', required=False, help="Upload to dev DBLoader without generate.")
    parser.add_argument('-uplp', '--upload_prod_stat', default='False', required=False, help="Upload to prod DBLoader without generate.")
    parser.add_argument('-conscsv', '--consolidated_csv', default=None, required=True, help="Name of the consolidated log file to check logs")
    
    args = parser.parse_args()
    
    dbpassword = args.dbpassword or pwinput.pwinput(prompt='Enter database shipper password: ', mask='*')
    encryption_key = args.encrypt_key
    upload_dev_stat = args.upload_dev_stat
    upload_prod_stat = args.upload_prod_stat
    consolidated_csv = args.consolidated_csv
    ###### consolidated_csv = f"{LOG_DIR}/{args.consolidated_csv}"

    if str2bool(upload_dev_stat):
        db_type = 'int2r'
        print("We do not update the upload status for INT2R")
    if str2bool(upload_prod_stat):
        db_type = 'cmsr'
        print("Updating Postgres with XML upload status ...")
        asyncio.run(main(dbpassword=dbpassword, db_type=db_type, encryption_key=encryption_key, consolidated_csv=consolidated_csv))