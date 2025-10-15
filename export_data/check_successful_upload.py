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
    "Error": False
}

YAML_MAP = "export_data/resource.yaml"        # local mapping file
with open(YAML_MAP) as f:
    yaml_data = yaml.safe_load(f)

def get_xml_key(xml_path: str) -> str:
    """Return mapping key like 'bp_cond' based on part name and path."""
    part_name_map = {'BA': 'bp', 'XL': 'hxb', '_': 'sensor', 'PL': 'proto', 'ML': 'module'}
    part_name = xml_path.split('/')[-1].split('_')[0]

    # Detect type code inside long serial number
    m = re.search(r'(BA|XL|PL|ML|_)', part_name)
    typecode = m.group(1) if m else None
    prefix = part_name_map.get(typecode, "")
    suffix = ""

    if "wirebond" in xml_path:
        suffix = "wirebond"
    elif "_cond_upload.xml" in xml_path:
        suffix = "cond"
    elif "_build_upload.xml" in xml_path:
        suffix = "build"
    elif "_assembly.xml" in xml_path:
        suffix = "assembly"
    elif "_iv_cond.xml" in xml_path:
        suffix = "iv_cond"
    elif "_iv.xml" in xml_path:
        suffix = "iv"
    elif "_pedestal" in xml_path:
        suffix = "pedestal"

    xml_type = f"{prefix}_{suffix}"
    table_map = yaml_data["postgres_table_to_xml"]
    tables = table_map[xml_type]

    if not prefix or not suffix:
        raise ValueError(f"Cannot determine key for part={part_name}, path={xml_path}")

    xml_type = f"{prefix}_{suffix}"
    table_map = yaml_data["postgres_table_to_xml"]
    tables = table_map.get(xml_type, [])

    if not tables:
        raise ValueError(f"No tables found in YAML for xml_type={xml_type}")

    return part_name, tables

async def update_upload_status_from_latest_log(pool: asyncpg.Pool):
    """
    Reads the latest dbloader_batch_uploader_*.csv under export_data/mass_upload_logs,
    parses upload_status, and updates xml_upload_success for each part_name in the given table.
    """

    # 1. Find the latest CSV file
    csv_files = list(LOG_DIR.glob("dbloader_batch_uploader_*.csv"))
    if not csv_files:
        print("❌ No log CSV files found.")
        return

    latest_csv = max(csv_files, key=os.path.getmtime)
    print(f"📄 Using latest log file: {latest_csv.name}")

    updates = []

    # 2. Parse the CSV file
    with open(latest_csv, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            xml_path = row["xml_path"]
            upload_status = row["upload_status"].strip()
            
            part_name, db_tables_to_be_updated = get_xml_key(xml_path)

            # Skip if unknown status
            if upload_status not in UPLOAD_STATUS_MAP:
                continue

            # 3. Map status to boolean
            status_bool = UPLOAD_STATUS_MAP[upload_status]
            updates.append((part_name, status_bool, db_tables_to_be_updated))

    if not updates:
        print("⚠️ No valid updates found in log.")
        return

    # 5. Execute updates inside one transaction
    async with pool.acquire() as conn:
        async with conn.transaction():
            for part_name, status_bool, tables in updates:
                for table in tables:
                    query = f"""
                        UPDATE {db_tables_to_be_updated}
                        SET xml_upload_success = $1
                        WHERE module_name = $2
                        OR bp_name = $2
                        OR proto_name = $2
                        OR hxb_name = $2
                        OR sensor_name = $2
                    """
                    print(query)
                    await conn.execute(query, status_bool, part_name)

    print(f"✅ Updated {len(updates)} rows in table '{db_tables_to_be_updated}'.")


async def main(dbpassword, encryption_key=None):

    parser = argparse.ArgumentParser(description="Script to process files in a directory.")
    parser.add_argument('-dbp', '--dbpassword', default=None, required=False, help="Password to access database.")
    parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
    args = parser.parse_args()
    
    dbpassword = args.dbpassword
    encryption_key = args.encrypt_key

    # Example DB connection setup
    loc = 'dbase_info'
    conn_yaml_file = os.path.join(loc, 'conn.yaml')
    conn_info = yaml.safe_load(open(conn_yaml_file, 'r'))
    db_params = {
        'database': conn_info.get('dbname'),
        'user': 'editor',
        'host': conn_info.get('db_hostname'),
        'port': conn_info.get('port'),
    }

    if encryption_key is None:
        db_params.update({'password': dbpassword})
    else:
        cipher_suite = Fernet((encryption_key).encode())
        db_params.update({'password': cipher_suite.decrypt( base64.urlsafe_b64decode(dbpassword)).decode()})

    pool = await asyncpg.create_pool(**db_params)

    try:
        await update_upload_status_from_latest_log(pool)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main(dbpassword))