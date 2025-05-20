import asyncio, asyncpg, pwinput
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
from lxml import etree
import yaml, os, base64, sys, argparse, traceback, datetime
from cryptography.fernet import Fernet
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from export_data.define_global_var import LOCATION, INSTITUTION
from export_data.src import *

async def process_module(conn, yaml_file, xml_file_path, output_dir, date_start, date_end, partsnamelist=None):
    # Load the YAML file
    with open(yaml_file, 'r') as file:
        yaml_data = yaml.safe_load(file)

    # Retrieve data from the YAML file
    xml_data = yaml_data['module_build']
    
    if not xml_data:
        print("No data found in YAML file")
        return
    db_tables = ['module_assembly']
    module_tables = ['module_assembly', 'mod_hxb_other_test', 'module_info', 'module_inspect', 'module_iv_test', 
                     'module_pedestal_test', 'module_pedestal_plots', 'module_qc_summary']
    
    # get applicable modules based on the specified time range
    module_list = set()
    if partsnamelist:
        query = f"""SELECT REPLACE(module_name,'-','') AS module_name FROM module_assembly WHERE module_name = ANY($1)"""
        results = await conn.fetch(query, partsnamelist)
    else:
        module_query = f"""
        SELECT DISTINCT REPLACE(module_name,'-','') AS module_name
        FROM module_assembly
        WHERE ass_run_date BETWEEN '{date_start}' AND '{date_end}' 
        """
        results = await conn.fetch(module_query)
    
    module_list.update(row['module_name'] for row in results if 'module_name' in row)
    
    for module in module_list:
        # Fetch database values for the XML template variables
        print(f'--> {module}...')
        try:
            db_values = {}

            for entry in xml_data:
                xml_var = entry['xml_temp_val']

                if xml_var in ['LOCATION', 'MANUFACTURER']:
                    db_values[xml_var] = LOCATION
                elif xml_var == 'INSTITUTION':
                    db_values[xml_var] = INSTITUTION
                elif xml_var in ['ID', 'BARCODE']:
                    db_values[xml_var] = format_part_name(module)
                elif xml_var == 'RUN_NUMBER':
                    db_values[xml_var] = get_run_num(LOCATION)
                elif xml_var == 'VERSION':
                    db_values[xml_var] = get_roc_version(module)
                elif xml_var in ['KIND_OF_PART', 'KIND_OF_PART_PROTOMODULE', 'KIND_OF_PART_PCB']:
                    if xml_var == 'KIND_OF_PART':
                        db_values[xml_var] = await get_kind_of_part(module)
                    elif xml_var == 'KIND_OF_PART_PROTOMODULE':
                        _query = f"SELECT REPLACE(proto_name,'-','') AS proto_name FROM module_assembly WHERE REPLACE(module_name,'-','') = '{module}';"
                        _proto_name = await conn.fetch(_query)
                        if _proto_name:
                            proto_name = _proto_name[0]['proto_name']
                        else:
                            proto_name = ''
                        db_values[xml_var] = await get_kind_of_part(proto_name)
                    else:
                        _query = f"SELECT REPLACE(hxb_name,'-','') AS hxb_name FROM module_assembly WHERE REPLACE(module_name,'-','') = '{module}';"
                        _hxb_name = await conn.fetch(_query)

                        if _hxb_name:
                            hxb_name = _hxb_name[0]['hxb_name']
                        else:
                            hxb_name = ''
                        db_values[xml_var] = await get_kind_of_part(hxb_name)
                else:
                    dbase_col = entry['dbase_col']
                    dbase_table = entry['dbase_table']

                    # Skip entries without a database column or table
                    if not dbase_col and not dbase_table:
                        continue

                    # Ignore nested queries for now
                    if entry['nested_query']:
                        if dbase_col == 'comment':
                            query = f"""
                            (
                                SELECT comment AS back_wirebond_comment
                                FROM back_wirebond
                                WHERE REPLACE(module_name,'-','') = '{module}'
                                -- AND xml_gen_datetime IS NULL
                                ORDER BY date_bond DESC, time_bond DESC
                                LIMIT 1
                            )
                            UNION ALL
                            (
                                SELECT comment AS front_wirebond_comment
                                FROM front_wirebond
                                WHERE REPLACE(module_name,'-','') = '{module}'
                                -- AND xml_gen_datetime IS NULL
                                ORDER BY date_bond DESC, time_bond DESC
                                LIMIT 1
                            );
                            """
                        else:
                            query = entry['nested_query'] + f" WHERE {dbase_table}.module_name = '{module}' /* AND xml_upload_success IS NULL */;"
                        
                        # print(f'Executing query: {query}')

                    else:
                        # Modify the query to get the latest entry
                        if dbase_table in ['module_info']:
                            query = f"""
                            SELECT {dbase_col} FROM {dbase_table}
                            WHERE REPLACE(module_name,'-','') = '{module}'
                            AND xml_upload_success IS NULL
                            LIMIT 1;
                            """
                        elif dbase_table in ['hexaboard']:
                            query = f"""
                            SELECT {dbase_table}.{dbase_col}
                            FROM {dbase_table}
                            JOIN module_assembly ON {dbase_table}.hxb_name = module_assembly.hxb_name
                            WHERE REPLACE(module_assembly.module_name, '-', '') = '{module}'
                            AND module_assembly.xml_upload_success IS NULL;
                            """
                        else:
                            query = f"""
                            SELECT {dbase_col} FROM {dbase_table} 
                            WHERE REPLACE(module_name,'-','') = '{module}'
                            AND xml_upload_success IS NULL
                            """
                            # ORDER BY ass_run_date DESC, ass_time_begin DESC LIMIT 1;
                    try:
                        results = await fetch_from_db(query, conn)  # Use conn directly
                    except Exception as e:
                        print('QUERY:', query)
                        print('ERROR:', e)

                    if results:
                        if xml_var == 'PCB':
                            db_values[xml_var] = format_part_name(results.get('hxb_name'))
                        elif xml_var == 'PROTOMODULE':
                            db_values[xml_var] = format_part_name(results.get('proto_name'))
                        else:
                            db_values[xml_var] = results.get(dbase_col, '') if not entry['nested_query'] else list(results.values())[0]

            output_file_name = f'{module}_{os.path.basename(xml_file_path)}'
            output_file_path = os.path.join(output_dir, output_file_name)
            await update_xml_with_db_values(xml_file_path, output_file_path, db_values)
            await update_timestamp_col(conn,
                                    update_flag=True,
                                    table_list=db_tables,
                                    column_name='xml_gen_datetime',
                                    part='module',
                                    part_name=module)

        except Exception as e:
            print('#'*15, f'ERROR for {module}','#'*15 ); traceback.print_exc(); print('')
            
        

async def main(dbpassword, output_dir, date_start, date_end, encryption_key = None, partsnamelist=None):
    # Configuration
    yaml_file = 'export_data/table_to_xml_var.yaml'  # Path to YAML file
    xml_file_path = 'export_data/template_examples/module/build_upload.xml'# XML template file path
    xml_output_dir = output_dir + '/module'  # Directory to save the updated XML

    # Create PostgreSQL connection pool
    conn = await get_conn(dbpassword, encryption_key)

    try:
        await process_module(conn, yaml_file, xml_file_path, xml_output_dir, date_start, date_end, partsnamelist)
    finally:
        await conn.close()

# Run the asyncio program
if __name__ == "__main__":
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
    parser.add_argument('-dbp', '--dbpassword', default=None, required=False, help="Password to access database.")
    parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
    parser.add_argument('-dir','--directory', default=None, help="The directory to process. Default is ../../xmls_for_dbloader_upload.")
    parser.add_argument('-datestart', '--date_start', type=lambda s: str(datetime.datetime.strptime(s, '%Y-%m-%d').date()), default=str(today), help=f"Date for XML generated (format: YYYY-MM-DD). Default is today's date: {today}")
    parser.add_argument('-dateend', '--date_end', type=lambda s: str(datetime.datetime.strptime(s, '%Y-%m-%d').date()), default=str(today), help=f"Date for XML generated (format: YYYY-MM-DD). Default is today's date: {today}")
    parser.add_argument("-pn", '--partnameslist', nargs="+", help="Space-separated list", required=False)
    args = parser.parse_args()   

    dbpassword = args.dbpassword
    output_dir = args.directory
    encryption_key = args.encrypt_key
    date_start = args.date_start
    date_end = args.date_end
    partsnamelist = args.partnameslist

    asyncio.run(main(dbpassword = dbpassword, output_dir = output_dir, encryption_key = encryption_key, date_start=date_start, date_end=date_end, partsnamelist=partsnamelist))