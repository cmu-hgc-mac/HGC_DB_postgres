import asyncio, asyncpg, pwinput
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
from lxml import etree
import yaml, os, base64, sys, argparse, traceback, datetime, tzlocal, pytz, re
from cryptography.fernet import Fernet
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from export_data.define_global_var import LOCATION, INSTITUTION
from export_data.src import *

async def process_module(conn, yaml_file, xml_file_path, output_dir, date_start, date_end, lxplus_username, partsnamelist=None):

    # Load the YAML file
    with open(yaml_file, 'r') as file:
        yaml_data = yaml.safe_load(file)

    # Retrieve module data from the YAML file
    module_data = yaml_data['module_assembly']
    if not module_data:
        print("No 'module' data found in YAML file")
        return

    # get the unique database tables that are directly associated with the xml creation
    dbase_tables = ['module_assembly', 'module_inspect']

    # get the unique module_name among all tables that contain module_name by taking the union of the tables
    module_tables = ['module_assembly', 'mod_hxb_other_test', 'module_info', 'module_inspect', 
                     'module_iv_test', 'module_pedestal_test', 'module_pedestal_plots', 'module_qc_summary']
    
    module_list = set()
    # Get the unique module names for the specified date
    for dbase_table in dbase_tables:
        if partsnamelist:
            query = f"""SELECT REPLACE(module_name,'-','') AS module_name FROM {dbase_table} WHERE module_name = ANY($1)"""
            results = await conn.fetch(query, partsnamelist)
        else:
            if dbase_table.endswith('_inspect'):
                module_query = f"""
                SELECT DISTINCT REPLACE(module_name,'-','') AS module_name
                FROM {dbase_table}
                WHERE date_inspect BETWEEN '{date_start}' AND '{date_end}' 
                """
            elif dbase_table.endswith('_test'):
                module_query = f"""
                SELECT DISTINCT REPLACE(module_name,'-','') AS module_name
                FROM {dbase_table}
                WHERE date_test BETWEEN '{date_start}' AND '{date_end}' 
                """
            elif dbase_table.endswith('_assembly'):
                module_query = f"""
                SELECT DISTINCT REPLACE(module_name,'-','') AS module_name
                FROM {dbase_table}
                WHERE ass_run_date BETWEEN '{date_start}' AND '{date_end}' 
                """

            results = await conn.fetch(module_query)
        
        module_list.update(row['module_name'] for row in results if 'module_name' in row)

    # Fetch database values for the XML template variables
    for module in module_list:
        print(f'--> {module}...')
        
        try:
            db_values = {}
            for entry in module_data:
                xml_var = entry['xml_temp_val']

                if xml_var in ['LOCATION']:
                    db_values[xml_var] = LOCATION
                elif xml_var == 'INSTITUTION':
                    db_values[xml_var] = INSTITUTION
                elif xml_var == 'ID':
                    db_values[xml_var] = format_part_name(module)
                elif xml_var == 'KIND_OF_PART':
                    db_values[xml_var] = await get_kind_of_part(format_part_name(module))
                elif xml_var == 'INITIATED_BY_USER':
                    db_values[xml_var] = lxplus_username
                elif entry['default_value']:
                    db_values[xml_var] = entry['default_value']
                else:
                    dbase_col = entry['dbase_col']
                    dbase_table = entry['dbase_table']

                    # Skip entries without a database column or table
                    if not dbase_col or not dbase_table:
                        continue
                        
                    # Ignore nested queries for now
                    if entry['nested_query']:
                        query = f"""SELECT hxb_inspect.avg_thickness FROM module_assembly  JOIN hxb_inspect  ON REPLACE(module_assembly.hxb_name, '-', '') = REPLACE(hxb_inspect.hxb_name, '-', '')  WHERE REPLACE(module_assembly.module_name, '-', '') = '{module}' AND (module_assembly.xml_upload_success IS NULL OR FALSE) ORDER BY hxb_inspect.date_inspect DESC LIMIT 1;"""
                    else:
                        # Modify the query to get the latest entry
                        if dbase_table == 'module_inspect':
                            query = f"""
                            SELECT {dbase_col} FROM {dbase_table} 
                            WHERE REPLACE(module_name,'-','') = '{module}' 
                            AND (xml_upload_success IS NULL OR FALSE);
                            """
                            # ORDER BY date_inspect DESC, time_inspect DESC LIMIT 1
                        else:
                            query = f"""
                            SELECT {dbase_col} FROM {dbase_table} 
                            WHERE REPLACE(module_name,'-','') = '{module}' 
                            AND (xml_upload_success IS NULL OR FALSE);
                            """
                            # ORDER BY ass_run_date DESC, ass_time_begin DESC LIMIT 1

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
                            db_values[xml_var] = format_datetime(run_date, time_begin)
                        elif xml_var == "RUN_END_TIMESTAMP_":
                            # Fetching both ass_run_date and ass_time_end
                            run_date = results.get("ass_run_date", "")
                            time_end = results.get("ass_time_end", "")
                            db_values[xml_var] = format_datetime(run_date, time_end)
                        elif xml_var == 'RUN_NUMBER':
                            run_date = results.get("ass_run_date", "")
                            time_begin = results.get("ass_time_begin", "")
                            combined_str = f"{run_date} {time_begin}"
                
                            try:
                                dt_obj = datetime.datetime.strptime(combined_str, "%Y-%m-%d %H:%M:%S.%f")
                            except ValueError:
                                dt_obj = datetime.datetime.strptime(combined_str, "%Y-%m-%d %H:%M:%S")
                            
                            db_values[xml_var] = get_run_num(LOCATION, dt_obj)
                        elif xml_var == 'PCB':
                            db_values[xml_var] = format_part_name(results.get('hxb_name'))
                        elif xml_var == 'PROTOMODULE':
                            db_values[xml_var] = format_part_name(results.get('proto_name'))
                        else:
                            db_values[xml_var] = results.get(dbase_col, '') if not entry['nested_query'] else list(results.values())[0]
            
            # Update the XML with the database values
            output_file_name = f'{module}_{os.path.basename(xml_file_path)}'
            output_file_path = os.path.join(output_dir, output_file_name)
            await update_xml_with_db_values(xml_file_path, output_file_path, db_values)
            await update_timestamp_col(conn,
                                    update_flag=True,
                                    table_list=dbase_tables,
                                    column_name='xml_gen_datetime',
                                    part='module',
                                    part_name=module)
        except Exception as e:
            print('#'*15, f'ERROR for {module}','#'*15 ); traceback.print_exc(); print('')



async def main(dbpassword, output_dir, date_start, date_end, lxplus_username, encryption_key = None, partsnamelist=None):
    # Configuration
    yaml_file = 'export_data/table_to_xml_var.yaml'  # Path to YAML file
    xml_file_path = 'export_data/template_examples/module/assembly_upload.xml'# XML template file path
    xml_output_dir = output_dir + '/module'  # Directory to save the updated XML

    # Create PostgreSQL connection pool
    conn = await get_conn(dbpassword, encryption_key)

    try:
        await process_module(conn, yaml_file, xml_file_path, xml_output_dir, date_start, date_end, lxplus_username, partsnamelist)
    finally:
        await conn.close()

# Run the asyncio program
if __name__ == "__main__":
    today = datetime.datetime.today().strftime('%Y-%m-%d')

    parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
    parser.add_argument('-dbp', '--dbpassword', default=None, required=False, help="Password to access database.")
    parser.add_argument('-lxu', '--dbl_username', default=None, required=False, help="Username to access lxplus.")
    parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
    parser.add_argument('-dir','--directory', default=None, help="The directory to process. Default is ../../xmls_for_dbloader_upload.")
    parser.add_argument('-datestart', '--date_start', type=lambda s: str(datetime.datetime.strptime(s, '%Y-%m-%d').date()), default=str(today), help=f"Date for XML generated (format: YYYY-MM-DD). Default is today's date: {today}")
    parser.add_argument('-dateend', '--date_end', type=lambda s: str(datetime.datetime.strptime(s, '%Y-%m-%d').date()), default=str(today), help=f"Date for XML generated (format: YYYY-MM-DD). Default is today's date: {today}")
    parser.add_argument("-pn", '--partnameslist', nargs="+", help="Space-separated list", required=False)
    args = parser.parse_args()   

    lxplus_username = args.dbl_username
    dbpassword = args.dbpassword
    output_dir = args.directory
    encryption_key = args.encrypt_key
    date_start = args.date_start
    date_end = args.date_end
    partsnamelist = args.partnameslist

    asyncio.run(main(dbpassword = dbpassword, output_dir = output_dir, encryption_key = encryption_key, date_start=date_start, date_end=date_end, lxplus_username=lxplus_username, partsnamelist=partsnamelist))