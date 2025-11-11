import asyncio, asyncpg, pwinput
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
from lxml import etree
import yaml, os, base64, sys, argparse, traceback, datetime, tzlocal, pytz
from cryptography.fernet import Fernet
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from export_data.define_global_var import LOCATION, INSTITUTION
from export_data.src import *

async def process_module(conn, yaml_file, xml_file_path, xml_file_path_env, output_dir, date_start, date_end, lxplus_username, partsnamelist=None):
    # Load the YAML file
    with open(yaml_file, 'r') as file:
        yaml_data = yaml.safe_load(file)

    # Retrieve module data from the YAML file
    xml_part_data = yaml_data['proto_inspection']
    xml_env_data = yaml_data['proto_inspection_env']
    for val in xml_part_data:
        val['xml_type'] = 'part'
    for val in xml_env_data:
        val['xml_type'] = 'env'
    module_data = xml_part_data + xml_env_data

    if not module_data:
        print("No 'module' data found in YAML file")
        return
    db_tables = ['proto_assembly', 'proto_inspect']
    
    proto_tables = ['proto_assembly', 'proto_inspect']
    
    proto_list = set()
    # Get the unique module names for the specified date
    for dbase_table in db_tables:
        if partsnamelist:
            query = f"""SELECT REPLACE(proto_name,'-','') AS proto_name FROM {dbase_table} WHERE proto_name = ANY($1)"""
            results = await conn.fetch(query, partsnamelist)
        else:
            if dbase_table.endswith('_inspect'):
                module_query = f"""
                SELECT DISTINCT REPLACE(proto_name,'-','') AS proto_name
                FROM {dbase_table}
                WHERE date_inspect BETWEEN '{date_start}' AND '{date_end}' 
                """
            elif dbase_table.endswith('_assembly'):
                module_query = f"""
                SELECT DISTINCT REPLACE(proto_name,'-','') AS proto_name
                FROM {dbase_table}
                WHERE ass_run_date BETWEEN '{date_start}' AND '{date_end}' """

            results = await conn.fetch(module_query)
        
        proto_list.update(row['proto_name'] for row in results if 'proto_name' in row)

    # Fetch database values for the XML template variables
    for proto_name in proto_list:
        print(f'--> {proto_name}...')
        try:
            db_values, db_values_env, db_values_part = {}, {}, {}
            for entry in module_data:
                xml_var, xml_type = entry['xml_temp_val'], entry['xml_type']

                if xml_var in ['LOCATION']:
                    db_values[xml_var] = LOCATION
                elif xml_var == 'INSTITUTION':
                    db_values[xml_var] = INSTITUTION
                elif xml_var == 'ID':
                    db_values[xml_var] = format_part_name(proto_name)
                elif xml_var == 'KIND_OF_PART':
                    db_values[xml_var] = await get_kind_of_part(format_part_name(proto_name))
                elif xml_var == 'INITIATED_BY_USER':
                    db_values[xml_var] = lxplus_username
                elif xml_var == 'COMMENT_DESCRIPTION':
                    db_values[xml_var] = f'Si protomodule inspection environment condition for {proto_name}'
                elif entry.get('default_value'):
                    db_values[xml_var] = entry['default_value']

                else:
                    dbase_col = entry['dbase_col']
                    dbase_table = entry['dbase_table']

                    # Skip entries without a database column or table
                    if not dbase_col or not dbase_table:
                        continue

                    # Ignore nested queries for now
                    if entry.get('nested_query'):
                        query = entry['nested_query'] + f" WHERE REPLACE(proto_assembly.proto_name,'-','') = '{proto_name}' /* AND xml_upload_success IS NULL */;"
                        
                    else:
                        # Modify the query to get the latest entry
                        if dbase_table == 'proto_assembly':
                            query = f"""
                            SELECT {dbase_col} FROM {dbase_table} 
                            WHERE REPLACE(proto_name,'-','') = '{proto_name}' 
                            -- AND xml_upload_success IS NULL
                            ORDER BY ass_run_date DESC, ass_time_begin DESC LIMIT 1
                            """
                        else:
                            query = f"""
                            SELECT {dbase_col} FROM {dbase_table} 
                            WHERE REPLACE(proto_name,'-','') = '{proto_name}' 
                            -- AND xml_upload_success IS NULL
                            ORDER BY date_inspect DESC, time_inspect DESC LIMIT 1
                            """
                    
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
                        elif xml_var == 'BASEPLATE':
                            db_values[xml_var] = format_part_name(results.get('bp_name'))
                        else:
                            db_values[xml_var] = results.get(dbase_col, '') if not entry.get('nested_query', False) else list(results.values())[0]
                
                if xml_type == 'part':
                    db_values_part[xml_var] = db_values[xml_var]
                elif xml_type == 'env':
                    db_values_env[xml_var] = db_values[xml_var]

            # Update the XML with the database values
            output_file_name_part = f'{proto_name}_{os.path.basename(xml_file_path)}'
            output_file_name_env = f'{proto_name}_env_{os.path.basename(xml_file_path)}'
            output_file_path_part = os.path.join(output_dir, output_file_name_part)
            output_file_path_env = os.path.join(output_dir, output_file_name_env)

            await update_xml_with_db_values(xml_file_path, output_file_path_part, db_values_part)
            await update_xml_with_db_values(xml_file_path_env, output_file_path_env, db_values_env)
            await update_timestamp_col(conn,
                                    update_flag=True,
                                    table_list=db_tables,
                                    column_name='xml_gen_datetime',
                                    part='protomodule',
                                    part_name=proto_name)
        
        except Exception as e:
            print('#'*15, f'ERROR for {proto_name}','#'*15 ); traceback.print_exc(); print('')
            
        

async def main(dbpassword, output_dir, date_start, date_end, lxplus_username, encryption_key = None, partsnamelist=None):
    # Configuration
    yaml_file = 'export_data/table_to_xml_var.yaml'  # Path to YAML file
    xml_file_path = 'export_data/template_examples/protomodule/inspection_upload.xml'# XML template file path
    xml_file_path_env = 'export_data/template_examples/protomodule/qc_env_cond.xml'# XML template file path
    xml_output_dir = output_dir + '/protomodule'  # Directory to save the updated XML

    # Create PostgreSQL connection pool
    conn = await get_conn(dbpassword, encryption_key)

    try:
        await process_module(conn, yaml_file, xml_file_path, xml_file_path_env, xml_output_dir, date_start, date_end, lxplus_username, partsnamelist)
    finally:
        await conn.close()

# Run the asyncio program
if __name__ == "__main__":
    today = datetime.datetime.today().strftime('%Y-%m-%d')

    parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
    parser.add_argument('-lxu', '--dbl_username', default=None, required=False, help="Username to access lxplus.")
    parser.add_argument('-dbp', '--dbpassword', default=None, required=False, help="Password to access database.")
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