import asyncio
import asyncpg
import csv
import os
from pathlib import Path
from datetime import datetime
import re, base64
import yaml
from cryptography.fernet import Fernet
import argparse
import glob
from src import get_conn

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


YAML_MAP = "export_data/resource.yaml"        # local mapping file
with open(YAML_MAP) as f:
    yaml_data = yaml.safe_load(f)

def get_reflected_tables(xml_path: str) -> str:
    """Return mapping key like 'bp_cond' based on part name and path."""
    """xml_path: /afs/cern.ch/user/u/username/hgc_xml_temp/320MLF2W2CM0102_wirebond_upload.xml"""

    table_map = yaml_data["postgres_table_to_xml"]
    
    ## get prefix
    part_name = xml_path.split('/')[-1].split('_')[0]
    if part_name.isdigit():
        prefix = 'sensor'
        mm = xml_path.split('/')[-1].split('_')[:2]
        part_name = '_'.join(mm)
    else:
        m = part_name.strip('320')[0] ## B, M, P, X
        prefix = PART_NAME_MAP.get(m, "")

    if not prefix:
        raise ValueError(f"Cannot determine part type for part={part_name}, path={xml_path}")
    
    suffix_matching = {
        "wirebond": "wirebond",
        "_cure_cond": f"{prefix}_cure_cond",
        "_inspection": f"{prefix}_inspection",
        "_build_upload": f"{prefix}_build",
        "_assembly": f"{prefix}_assembly",
        "_iv_cond": f"{prefix}_iv_cond",
        "_iv": f"{prefix}_iv",
        "_pedestal": f"{prefix}_pedestal"
    }
    for suffix, xml_type in suffix_matching.items():
        if suffix in xml_path:
            tables = table_map[xml_type]
            if not suffix:
                raise ValueError(f"Cannot determine xml type for part={part_name}, path={xml_path}")
            else:
                return prefix, part_name, tables ##i.e., module, 320MLF2W2CM0102, [module_pedestal, module_cond]


def get_upload_status_csv(csv_path):
    '''
    xml_path,upload_status,upload_state_value,upload_state_path,upload_log_path     
    '''
    csv_output = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            xml_path = row["xml_path"]
            upload_status = row["upload_status"].strip()## Refer to UPLOAD_STATUS_MAP
            prefix, part_name, db_tables_to_be_updated = get_reflected_tables(xml_path)
            csv_output.append((prefix, part_name, upload_status, db_tables_to_be_updated))

            # Skip if unknown status
            if upload_status not in UPLOAD_STATUS_MAP:
                continue
        return csv_output ## [(part_name, upload_status, db_tables_to_be_updated), (...), ...]

def get_api_data(search_id, db_type):
    if db_type == 'cmsr':
        url = f"https://hgcapi.web.cern.ch/mac/part/{search_id}/full"
    elif db_type == 'int2r':
        url = f"https://hgcapi-intg.web.cern.ch/mac/part/{search_id}/full"

async def _run_update(pool, query, success_flag, part_name, sem):
    async with sem:
        async with pool.acquire() as conn:
            return await conn.execute(query, success_flag, part_name)
        
async def update_upload_status(pool, csv_output, concurrency=10):
    """
    Update 'xml_upload_success' column for relevant tables based on csv_output.

    Args:
        conn: An existing asyncpg.Connection object.
        csv_output: List of tuples in the form (part_name, status, [table_names]).
    """
    sem = asyncio.Semaphore(concurrency)
    tasks = []

    for prefix, part_name, status, tables in csv_output:
        success_flag = UPLOAD_STATUS_MAP.get(status)
        if success_flag is None:
            print(f"Unknown status '{status}' for part {part_name}, skipping.")
            continue
        for table in tables:
            # Sanitize table name for safety: ensure it's alphanumeric + underscore only
            if not table.replace("_", "").isalnum():
                print(f"Skipping suspicious table name: {table}")
                continue
            if prefix == 'sensor':
                query = f"""
                    UPDATE {table}
                    SET xml_upload_success = $1
                    WHERE sen_name = $2
                """
            else:
                query = f"""
                    UPDATE {table}
                    SET xml_upload_success = $1
                    WHERE {prefix}_name = $2
                """
            # tasks.append(conn.execute(query, success_flag, part_name))
            tasks.append(_run_update(pool, query, success_flag, part_name, sem))
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

async def main(dbpassword, encryption_key=None):
    # Connect to PostgreSQL
    pool = await get_conn(dbpassword, encryption_key, pool=True)
    print("Connected to database.")

    try:
        # Find and process latest CSV
        massloader_log_csv = get_latest_upload_log()
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
        print("Database connection closed.")
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script to process files in a directory.")
    parser.add_argument('-dbp', '--dbpassword', default=None, required=False, help="Password to access database.")
    parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
    args = parser.parse_args()
    
    dbpassword = args.dbpassword
    encryption_key = args.encrypt_key
    asyncio.run(main(dbpassword=dbpassword, encryption_key=encryption_key))