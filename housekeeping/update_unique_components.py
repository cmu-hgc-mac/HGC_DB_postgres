import asyncio, asyncpg
import glob, os, csv, yaml, argparse, base64, traceback
import numpy as np
import pwinput
from cryptography.fernet import Fernet

parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
parser.add_argument('-p', '--password', default=None, required=False, help="Password to access database.")
parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
args = parser.parse_args()

print('Merging duplicate components ...')
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

async def merge_duplicate_components():
    conn = await asyncpg.connect(**db_params)

    query = """
DO $$
DECLARE
  v_schema text := 'public';

  v_tables text[] := ARRAY['baseplate','hexaboard','module_info','proto_assembly','sensor'];
  v_pks    text[] := ARRAY['bp_no',    'hxb_no',   'module_no',  'proto_no',       'sen_no'];
  v_uks    text[] := ARRAY['bp_name',  'hxb_name', 'module_name','proto_name',     'sen_name'];

  i int;
  v_table text;
  v_pk    text;
  v_uk    text;

  v_sel_list text;
  v_set_list text;
  rec record;
BEGIN
  FOR i IN 1..array_length(v_tables,1) LOOP
    v_table := v_tables[i];
    v_pk    := v_pks[i];
    v_uk    := v_uks[i];

    RAISE NOTICE 'Processing %.% (pk=%, uk=%)', v_schema, v_table, v_pk, v_uk;

    v_sel_list := '';
    v_set_list := '';

    -- Handle mergeable columns (non-array, non-PK, non-UK)
    FOR rec IN
      SELECT
        a.attname AS column_name,
        format_type(a.atttypid, a.atttypmod) AS col_type,
        (format_type(a.atttypid, a.atttypmod) ILIKE 'date%'
         OR format_type(a.atttypid, a.atttypmod) ILIKE 'timestamp%'
         OR format_type(a.atttypid, a.atttypmod) ILIKE 'time%') AS is_datetime
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
      WHERE n.nspname = v_schema
        AND c.relname = v_table
        AND a.attnum > 0
        AND NOT a.attisdropped
        AND a.attname NOT IN (v_pk, v_uk)
        AND a.attndims = 0                   -- ★ Non-array columns only
      ORDER BY a.attnum
    LOOP
      IF rec.is_datetime THEN
        v_sel_list := v_sel_list || CASE WHEN v_sel_list='' THEN '' ELSE ', ' END ||
                      format('MIN(%1$I) AS %1$I', rec.column_name);
        v_set_list := v_set_list || CASE WHEN v_set_list='' THEN '' ELSE ', ' END ||
                      format('%1$I = m.%1$I', rec.column_name);
      ELSE
        v_sel_list := v_sel_list || CASE WHEN v_sel_list='' THEN '' ELSE ', ' END ||
                      format('(ARRAY_AGG(%1$I ORDER BY (%1$I IS NULL), %2$I DESC))[1] AS %1$I',
                             rec.column_name, v_pk);
        v_set_list := v_set_list || CASE WHEN v_set_list='' THEN '' ELSE ', ' END ||
                      format('%1$I = COALESCE(t.%1$I, m.%1$I)', rec.column_name);
      END IF;
    END LOOP;

    IF v_sel_list = '' THEN
      v_sel_list := '/* no mergeable columns */ NULL::int';
    END IF;

    -- write back to keepers
    EXECUTE format($f$
      WITH keepers AS (
        SELECT MIN(%1$I) AS keep_id, %2$I AS k
        FROM %3$I.%4$I
        WHERE %2$I IS NOT NULL
        GROUP BY %2$I
      ),
      merged AS (
        SELECT %2$I, %5$s
        FROM %3$I.%4$I
        WHERE %2$I IS NOT NULL
        GROUP BY %2$I
      )
      UPDATE %3$I.%4$I t
      SET %6$s
      FROM merged m
      JOIN keepers k ON m.%2$I = k.k
      WHERE t.%1$I = k.keep_id
    $f$, v_pk, v_uk, v_schema, v_table, v_sel_list, v_set_list);

    -- delete duplicates
    EXECUTE format($f$
      WITH ranked AS (
        SELECT %1$I AS id,
               ROW_NUMBER() OVER (PARTITION BY %2$I ORDER BY %1$I) AS rn
        FROM %3$I.%4$I
        WHERE %2$I IS NOT NULL
      )
      DELETE FROM %3$I.%4$I t
      USING ranked r
      WHERE t.%1$I = r.id AND r.rn > 1
    $f$, v_pk, v_uk, v_schema, v_table);

    RAISE NOTICE 'Done %.%', v_schema, v_table;
  END LOOP;
END $$;
    """

    try:
        await conn.execute(query)
        print('Duplicate components merged successfully.')

    except asyncpg.PostgresError as e:
        print("Error:", e)
        traceback.print_exc()
    
    await conn.close()

asyncio.run(merge_duplicate_components())