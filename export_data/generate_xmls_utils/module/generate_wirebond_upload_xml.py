import asyncio, asyncpg, pwinput
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
from lxml import etree
import yaml, os, base64, sys, argparse, traceback, datetime, tzlocal, pytz
from cryptography.fernet import Fernet
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from export_data.define_global_var import LOCATION
from export_data.src import get_conn, fetch_from_db, update_xml_with_db_values, get_parts_name, get_kind_of_part, update_timestamp_col, format_part_name, get_run_num, format_datetime

async def process_module(conn, yaml_file, xml_file_path, output_dir, date_start, date_end):
    # Load the YAML file
    with open(yaml_file, 'r') as file:
        yaml_data = yaml.safe_load(file)

    # Retrieve wirebond data from the YAML file
    wb_data = yaml_data['wirebond']
    
    if not wb_data:
        print("No wirebond data found in YAML file")
        return

    db_tables = ['back_wirebond', 'front_wirebond', 'bond_pull_test', 'back_encap', 'front_encap']
    module_tables = ['module_assembly', 'mod_hxb_other_test', 'module_info', 'module_inspect', 'module_iv_test', 'module_pedestal_test', 'module_pedestal_plots', 'module_qc_summary']
    
    module_list = set()
    # Get the unique module names for the specified date
    for dbase_table in db_tables:
        if dbase_table.endswith(('_wirebond', '_test')):
            module_query = f"""
            SELECT DISTINCT REPLACE(module_name,'-','') AS module_name
            FROM {dbase_table}
            WHERE date_bond BETWEEN '{date_start}' AND '{date_end}' 
            """
        elif dbase_table.endswith('_encap'):
            module_query = f"""
            SELECT DISTINCT REPLACE(module_name,'-','') AS module_name
            FROM {dbase_table}
            WHERE date_encap BETWEEN '{date_start}' AND '{date_end}' 
            """

        results = await conn.fetch(module_query)
        module_list.update(row['module_name'] for row in results if 'module_name' in row)

    for module in module_list:
        print(f'--> {module}...')
        try:
            # Fetch database values for the XML template variables
            db_values = {}

            for entry in wb_data:
                xml_var = entry['xml_temp_val']

                if xml_var in ['LOCATION', 'INSTITUTION']:
                    db_values[xml_var] = LOCATION
                elif xml_var == 'ID':
                    db_values[xml_var] = format_part_name(module)
                elif xml_var == 'KIND_OF_PART':
                    db_values[xml_var] = get_kind_of_part(module)
                elif xml_var == 'RUN_NUMBER':
                    db_values[xml_var] = get_run_num(LOCATION)
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
                                -- AND xml_upload_success IS NULL
                                ORDER BY date_bond DESC, time_bond DESC
                                LIMIT 1
                            )
                            UNION ALL
                            (
                                SELECT comment AS front_wirebond_comment
                                FROM front_wirebond
                                WHERE REPLACE(module_name,'-','') = '{module}'
                                -- AND xml_upload_success IS NULL
                                ORDER BY date_bond DESC, time_bond DESC
                                LIMIT 1
                            );
                            """
                        else:
                            query = entry['nested_query'] + f" WHERE REPLACE({dbase_table}.module_name,'-','') = '{module}' /* AND xml_upload_success IS NULL */;"
                        
                        # print(f'Executing query: {query}')

                    else:
                        # Modify the query to get the latest entry
                        if dbase_table in ['back_encap', 'front_encap']:
                            query = f"""
                            SELECT {dbase_col} FROM {dbase_table}
                            WHERE REPLACE(module_name,'-','') = '{module}'
                            -- AND xml_upload_success IS NULL
                            ORDER BY date_encap DESC, time_encap DESC
                            LIMIT 1;
                            """
                        elif dbase_table == 'module_info':
                            query = f"""
                            SELECT {dbase_col} FROM {dbase_table}
                            WHERE REPLACE(module_name,'-','') = '{module}'
                            -- AND xml_upload_success IS NULL;
                            """
                        else:
                            query = f"""
                            SELECT {dbase_col} FROM {dbase_table} 
                            WHERE REPLACE(module_name,'-','') = '{module}'
                            -- AND xml_upload_success IS NULL
                            ORDER BY date_bond DESC, time_bond DESC LIMIT 1;
                            """
                    # print(f'Executing query -- \n\t{query}')
                    try:
                        results = await fetch_from_db(query, conn)  # Use conn directly
                    except Exception as e:
                        print('QUERY:', query)
                        print('ERROR:', e)

                    if results:
                        if xml_var == "RUN_BEGIN_TIMESTAMP_":
                            # Fetching both ass_run_date and ass_time_begin
                            run_date = results.get("date_bond", "")
                            time_begin = results.get("time_bond", "")
                            db_values[xml_var] = format_datetime(run_date, time_begin)
                        elif xml_var == "RUN_NAME_TIME_STAMP":
                            run_date = results.get("date_bond", "")
                            time_end = results.get("time_bond", "")
                            db_values[xml_var] = format_datetime(run_date, time_end)
                        elif xml_var == "WIREBOND_COMMENTS_CONCAT":
                            bk_comment = results.get("back_wirebond_comment", "")
                            fr_comment = results.get("front_wirebond_comment", "")
                            db_values[xml_var] = f"{bk_comment}-{fr_comment}"
                        elif xml_var == "ENCAPSULATION_COMMENTS_CONCAT":
                            bk_comment = results.get("back_encap_comment", "")
                            fr_comment = results.get("front_encap_comment", "")
                            db_values[xml_var] = f"{bk_comment}-{fr_comment}"
                        else:
                            db_values[xml_var] = results.get(dbase_col, '') if not entry['nested_query'] else list(results.values())[0]
                    
                        if 'BOND_PULL_AVG' in list(db_values.keys()):
                            db_values['BOND_PULL_AVG'] = str(round(float(db_values['BOND_PULL_AVG']), 3))
                        if 'BOND_PULL_STDDEV' in list(db_values.keys()):
                            db_values['BOND_PULL_STDDEV'] = str(round(float(db_values['BOND_PULL_STDDEV']), 3))

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
            

        
async def main(dbpassword, output_dir, date_start, date_end, encryption_key = None):
    # Configuration
    yaml_file = 'export_data/table_to_xml_var.yaml'  # Path to YAML file
    xml_file_path = 'export_data/template_examples/module/wirebond_upload.xml'# XML template file path
    xml_output_dir = output_dir + '/module'  # Directory to save the updated XML

    # Create PostgreSQL connection pool
    conn = await get_conn(dbpassword, encryption_key)

    try:
        await process_module(conn, yaml_file, xml_file_path, xml_output_dir, date_start, date_end)
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

    args = parser.parse_args()   

    dbpassword = args.dbpassword
    output_dir = args.directory
    encryption_key = args.encrypt_key
    date_start = args.date_start
    date_end = args.date_end

    asyncio.run(main(dbpassword = dbpassword, output_dir = output_dir, encryption_key = encryption_key, date_start=date_start, date_end=date_end))
