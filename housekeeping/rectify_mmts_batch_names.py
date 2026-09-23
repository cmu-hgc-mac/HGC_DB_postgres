import asyncio, asyncpg
import os, yaml, argparse, base64, traceback
import pwinput
from cryptography.fernet import Fernet
from datetime import datetime

parser = argparse.ArgumentParser(description="Consolidates mmts_batch_logging to one row per physical batch.")
parser.add_argument('-p', '--password', default=None, required=False, help="Password to access database.")
parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
args = parser.parse_args()

loc = 'dbase_info'
conn_yaml_file = os.path.join(loc, 'conn.yaml')
conn_info = yaml.safe_load(open(conn_yaml_file, 'r'))
backup_dir = os.path.join('housekeeping', 'mmts_batch_logging_backups')

db_params = {
    'database': conn_info.get('dbname'),
    'user': 'editor',
    'host': conn_info.get('db_hostname'),
    'port': conn_info.get('port'),
}

if args.password is None:
    dbpassword = pwinput.pwinput(prompt='Enter superuser password: ', mask='*')
else:
    if args.encrypt_key is None:
        print("Encryption key not provided. Exiting.."); exit()
    cipher_suite = Fernet((args.encrypt_key).encode())
    dbpassword = cipher_suite.decrypt(base64.urlsafe_b64decode(args.password)).decode()

db_params.update({'password': dbpassword})

async def consolidate_batch_logging():
    conn = await asyncpg.connect(**db_params)
    print('Connection successful.')

    try:
        os.makedirs(backup_dir, exist_ok=True)
        backup_file = os.path.join(backup_dir, f"mmts_batch_logging_AS_OF_{datetime.now().strftime('%y%m%d%H%M%S')}.csv")
        await conn.copy_from_query("SELECT * FROM mmts_batch_logging ORDER BY batch_no ASC", output=backup_file, format='csv', header=True)
        print(f"Backup of mmts_batch_logging saved to {backup_file}")
    except Exception as e:
        print(f"Backup of mmts_batch_logging failed: {e}. Skipping consolidation.")
        traceback.print_exc()
        await conn.close()
        return

    try:
        # Only MMTSjobRunning rows are kept; the other logging events carry no extra batch information.
        deleted_events = await conn.execute("""
            DELETE FROM mmts_batch_logging
            WHERE description IN ('MMTSjobFinished', 'IVSCANRun', 'IVSCANEnd')
        """)
        print(f"Removed MMTSjobFinished/IVSCANRun/IVSCANEnd rows: {deleted_events.split()[-1]} row(s) deleted.")

        rows = await conn.fetch("""
            SELECT batch_no, batch_name, cycle_count, description
            FROM mmts_batch_logging
            ORDER BY batch_name ASC
        """)

        # Group rows into physical batches.
        # cycle_count should be the number of thermal cycles, but was incorrectly recorded as the iteration number (1 = first iteration).
        # Seeing cycle_count = 1 after cycle_count > 1 signals a new physical batch. NULL cycle_count rows are skipped.
        groups = []
        current_group = []
        saw_higher = False

        for row in rows:
            if row['description'] != 'MMTSjobRunning':
                if current_group:
                    groups.append(current_group)
                    current_group = []
                    saw_higher = False
                continue
            count = row['cycle_count']
            if count is None:
                print(f"Warning: NULL cycle_count for batch_no={row['batch_no']} "
                      f"(batch_name={row['batch_name']}). Skipping.")
                continue
            if count == 1 and saw_higher:
                groups.append(current_group)
                current_group = [row]
                saw_higher = False
            else:
                if count > 1:
                    saw_higher = True
                current_group.append(row)

        if current_group:
            groups.append(current_group)

        consolidated = 0
        for group in groups:
            total_cycles = max(r['cycle_count'] for r in group)

            # Surviving row: first MMTSjobRunning in iteration 1, fallback to first row in the batch.
            surviving = next(
                (r for r in group
                 if r['description'] == 'MMTSjobRunning' and r['cycle_count'] == 1),
                group[0]
            )
            to_delete = [r['batch_no'] for r in group if r['batch_no'] != surviving['batch_no']]

            if to_delete:
                await conn.execute("""
                    DELETE FROM mmts_batch_logging WHERE batch_no = ANY($1)
                """, to_delete)

            await conn.execute("""
                UPDATE mmts_batch_logging
                SET cycle_count   = -999,
                    description   = 'Batch names resolved using Postgres panel',
                    log_timestamp = TO_TIMESTAMP(batch_name, 'YYYYMMDD-HH24MISS'),
                    timestamp_utc = TO_TIMESTAMP(batch_name, 'YYYYMMDD-HH24MISS')::timestamptz,
                    station_names = (
                        SELECT array_agg(REPLACE(name, 'moduleID', 'MMTS_') ORDER BY ord)
                        FROM unnest(station_names) WITH ORDINALITY AS u(name, ord)
                    )
                WHERE batch_no = $1
            """, surviving['batch_no'])

            print(f"Batch {surviving['batch_name']}: {total_cycles} thermal cycle(s), "
                  f"{len(to_delete)} row(s) deleted.")
            consolidated += 1

        print(f"Done. {consolidated} physical batch(es) consolidated.")

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()

    await conn.close()

asyncio.run(consolidate_batch_logging())
