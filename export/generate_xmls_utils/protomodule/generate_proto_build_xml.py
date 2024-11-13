import asyncio, asyncpg, pwinput
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
from lxml import etree
import yaml, os, base64, sys, argparse
from cryptography.fernet import Fernet
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..')))
from HGC_DB_postgres.export.define_global_var import LOCATION
from HGC_DB_postgres.export.src import get_conn, fetch_from_db, update_xml_with_db_values, get_parts_name, get_kind_of_part, update_timestamp_col

async def process_module(conn, yaml_file, xml_file_path, output_dir):
    # Load the YAML file
    with open(yaml_file, 'r') as file:
        yaml_data = yaml.safe_load(file)

    # Retrieve module data from the YAML file
    module_data = yaml_data['proto_build']
    # module_data = [item for item in yaml_data if 'module' in item['dbase_table']]
    
    if not module_data:
        print("No 'module' data found in YAML file")
        return
    db_tables = ['proto_assembly']
    proto_tables = ['proto_assembly', 'proto_inspect']
    _proto_list = []
    for proto_table in proto_tables:
        _proto_list.extend(await get_parts_name('proto_name', proto_table, conn))
    proto_list = list(set(_proto_list))

    # Fetch database values for the XML template variables
    for proto_name in proto_list:
        print(f'getting values for {proto_name}...')
        db_values = {}
        for entry in module_data:
            xml_var = entry['xml_temp_val']
            if xml_var in ['LOCATION', 'INSTITUTION']:
                db_values[xml_var] = LOCATION
            elif xml_var == 'ID':
                db_values[xml_var] = proto_name
            elif xml_var == 'KIND_OF_PART':
                db_values[xml_var] = get_kind_of_part(proto_name)
            elif xml_var == 'KIND_OF_PART_BASEPLATE':
                _query = f"SELECT bp_name FROM proto_assembly WHERE proto_name = '{proto_name}';"
                _bp_name = await conn.fetch(_query)
                if _bp_name:
                    bp_name = _bp_name[0]['bp_name']
                else:
                    bp_name = ''
                db_values[xml_var] = get_kind_of_part(bp_name)
            elif xml_var == 'KIND_OF_PART_SENSOR':
                _query = f"SELECT sen_name FROM proto_assembly WHERE proto_name = '{proto_name}';"
                _sen_name = await conn.fetch(_query)
                if _sen_name:
                    sen_name = _sen_name[0]['sen_name']
                else:
                    sen_name = ''
                db_values[xml_var] = get_kind_of_part(sen_name)
            else:
                dbase_col = entry['dbase_col']
                dbase_table = entry['dbase_table']

                # Skip entries without a database column or table
                if not dbase_col or not dbase_table:
                    continue

                # Ignore nested queries for now
                if entry['nested_query']:
                    query = entry['nested_query'] + f" WHERE proto_assembly.proto_name = '{proto_name}';"
                    
                else:
                    # Modify the query to get the latest entry
                    if dbase_table == 'proto_assembly':
                        query = f"SELECT {dbase_col} FROM {dbase_table} WHERE proto_name = '{proto_name}' ORDER BY ass_run_date DESC, ass_time_begin DESC LIMIT 1"
                    else:
                        query = f"SELECT {dbase_col} FROM {dbase_table} WHERE proto_name = '{proto_name}' ORDER BY date_inspect DESC, time_inspect DESC LIMIT 1"
                
                try:
                    results = await fetch_from_db(query, conn)  # Use conn directly
                except Exception as e:
                    print('QUERY:', query)
                    print('ERROR:', e)
                
                if results:
                    if xml_var == "RUN_BEGIN_TIMESTAMP_":
                        # Fetching both ass_run_date and ass_time_begin
                        run_date = results.get("ass_run_date", "")
                        time_begin = results.get("ass_time_begin", "")
                        db_values[xml_var] = f"{run_date}T{time_begin}"
                    elif xml_var == "RUN_END_TIMESTAMP_":
                        # Fetching both ass_run_date and ass_time_end
                        run_date = results.get("ass_run_date", "")
                        time_end = results.get("ass_time_end", "")
                        db_values[xml_var] = f"{run_date}T{time_end}"
                    else:
                        db_values[xml_var] = results.get(dbase_col, '') if not entry['nested_query'] else list(results.values())[0]

        # Update the XML with the database values
        output_file_name = f'{proto_name}_{os.path.basename(xml_file_path)}'
        output_file_path = os.path.join(output_dir, output_file_name)
        await update_xml_with_db_values(xml_file_path, output_file_path, db_values)
        await update_timestamp_col(conn,
                                   update_flag=True,
                                   table_list=db_tables,
                                   column_name='xml_gen_datetime',
                                   part='protomodule',
                                   part_name=proto_name)
        
async def main(dbpassword, output_dir, encryption_key = None):
    # Configuration
    yaml_file = 'export/table_to_xml_var.yaml'  # Path to YAML file
    xml_file_path = 'export/template_examples/protomodule/build_upload.xml'# XML template file path
    xml_output_dir = output_dir + '/protomodule'  # Directory to save the updated XML


    # Create PostgreSQL connection pool
    conn = await get_conn(dbpassword, encryption_key)

    try:
        await process_module(conn, yaml_file, xml_file_path, xml_output_dir)
    finally:
        await conn.close()

# Run the asyncio program
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
    parser.add_argument('-dbp', '--dbpassword', default=None, required=False, help="Password to access database.")
    parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
    parser.add_argument('-dir','--directory', default=None, help="The directory to process. Default is ../../xmls_for_dbloader_upload.")
    args = parser.parse_args()   

    dbpassword = args.dbpassword
    output_dir = args.directory
    encryption_key = args.encrypt_key

    asyncio.run(main(dbpassword = dbpassword, output_dir = output_dir, encryption_key = encryption_key))

