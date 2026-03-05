import asyncio
import asyncpg
import argparse
import json
import numpy as np
import sys, os, yaml, argparse, datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from export_data.src import *
from export_data.define_global_var import LOCATION, INSTITUTION
RED = '\033[91m'; RESET = '\033[0m'

resource_yaml = 'export_data/resource.yaml'
with open(resource_yaml, 'r') as file:
    yaml_content = yaml.safe_load(file)
    kind_of_part_yaml = yaml_content['kind_of_part']
    cell_count_for_res_geom = kind_of_part_yaml['cell_count_for_res_geom']

def fetch_module_grades(mod_corner_colors = None, all_letter_grades = None):
    worst_letter_grade = 'unknown' if not all_letter_grades else max(all_letter_grades, key=str.lower)
    MODULE_CORNER_COLORGRADE = 'unknown'
    if mod_corner_colors:
        mod_corner_colors = [c.lower() for c in mod_corner_colors]
        MODULE_CORNER_COLORGRADE = 'red' if 'red' in mod_corner_colors else ('purple' if 'purple' in mod_corner_colors else 'green')
    
    installation_score = 0 if (MODULE_CORNER_COLORGRADE == 'red' or worst_letter_grade.upper() == 'F') else 1
    return installation_score, MODULE_CORNER_COLORGRADE

async def process_module(conn, yaml_file, xml_file_path, output_dir, date_start, date_end, lxplus_username, partsnamelist=None):
    with open(yaml_file, 'r') as file:
        yaml_data = yaml.safe_load(file)
    xml_data  = yaml_data['module_grading']

    ## list module_names that we want to generate xmls
    module_list = set()
    if partsnamelist:
        query = f"""
            SELECT module_name, mod_qc_no
            FROM module_qc_summary
            WHERE module_name = ANY($1) 
            AND ((xml_gen_datetime IS NULL) OR (xml_gen_datetime IS NOT NULL AND (xml_upload_success IS NULL OR xml_upload_success = FALSE)))
            ORDER BY mod_qc_no DESC LIMIT 1
        """  ### Get the latest for that module
        results = await conn.fetch(query, partsnamelist)
    else:
        query = f"""
            SELECT m.module_name, m.mod_qc_no
            FROM module_qc_summary m
            JOIN (
            SELECT module_name, MAX(mod_qc_no) AS max_mod_qc_no
            FROM module_qc_summary WHERE xml_gen_datetime IS NULL OR (xml_upload_success IS NULL OR xml_upload_success = FALSE) GROUP BY module_name) latest
            ON latest.module_name = m.module_name AND latest.max_mod_qc_no = m.mod_qc_no ORDER BY m.module_name DESC """ ### Get the latest for all modules ever
        results = await conn.fetch(query)

    module_list = set((row['module_name'], row['mod_qc_no']) for row in results if 'module_name' in row)
    for module_name, mod_qc_no in module_list:
        ### time.sleep(2)
        datetimenow = datetime.datetime.now()
        combined_str = datetimenow ### "" ### initialize
        
        try:
            db_values, db_values = {}, {}
            for entry in xml_data:  ### loaded from table_to_xml_var.yaml
                xml_var = entry['xml_temp_val']
                if xml_var == 'LOCATION':
                    db_values[xml_var] = LOCATION 
                elif xml_var == 'INSTITUTION':
                    db_values[xml_var] = INSTITUTION
                elif xml_var == 'ID':
                    db_values[xml_var]     = module_name
                elif xml_var == 'KIND_OF_PART':
                    db_values[xml_var]     = await get_kind_of_part(module_name, 'module', conn)
                elif xml_var == 'INITIATED_BY_USER':
                    db_values[xml_var]     = lxplus_username
                elif xml_var == "RUN_BEGIN_TIMESTAMP_":
                    db_values[xml_var] = datetimenow
                elif xml_var == "RUN_END_TIMESTAMP_":
                    db_values[xml_var] = datetimenow
                elif xml_var == "RUN_TYPE":
                    db_values[xml_var] = "Si module grading" 
                elif xml_var == 'RUN_NUMBER':
                    dt_obj =  datetimenow                 
                    db_values[xml_var]     = get_run_num(LOCATION, dt_obj)
                elif xml_var == "COMMENT_DESCRIPTION":
                    db_values[xml_var] = f"MAC grades for {module_name}"
                else:
                    dbase_col = entry['dbase_col']
                    dbase_table = entry['dbase_table']

                    # Skip entries without a database column or table
                    if not dbase_col and not dbase_table:
                        continue
                
                    query = f"""SELECT {dbase_col} FROM {dbase_table} WHERE module_name = '{module_name}' AND mod_qc_no = {mod_qc_no} """
                    try:
                        results = await fetch_from_db(query, conn)
                    except Exception as e:
                            print('QUERY:', query)
                            print('ERROR:', e)
                    
                    if results:
                        # if xml_var == 'RUN_NUMBER':
                        #     dt_obj =  datetimenow                 
                        #     db_values[xml_var]     = get_run_num(LOCATION, dt_obj)
                        if xml_var == 'INSTALLATION_MODULE':
                            grades_to_get = ['final_grade','proto_grade','module_grade','iv_grade','readout_grade']
                            all_letter_grades = [results.get(grade_type, "") for grade_type in grades_to_get]
                            mod_corner_colors = results.get('module_corner_colorgrades', "")
                            installation_score, mod_colorgrade = fetch_module_grades(mod_corner_colors, all_letter_grades)
                            db_values[xml_var] = installation_score
                        elif xml_var == 'MODULE_CORNER_COLORGRADE':
                            mod_corner_colors = results.get('module_corner_colorgrades', "")
                            installation_score, mod_colorgrade = fetch_module_grades(mod_corner_colors, all_letter_grades=None)
                            db_values[xml_var] = mod_colorgrade
                        elif xml_var == 'PERCENT_BAD_CELLS':
                            count_bad_cells = results.get('count_bad_cells', "0")
                            total_cell_count = int(cell_count_for_res_geom[module_name[4:6]])
                            db_values[xml_var] = str(round(int(count_bad_cells)*100/total_cell_count,3))
                        else:
                            db_values[xml_var] = results.get(dbase_col, '')
                
                        
            # Update the XML with the database values
            combined_str_mod = str(combined_str).replace(" ","T").replace(":","").split('.')[0]
            output_file_name = f"{module_name}_{combined_str_mod}_grading_upload.xml"
            output_file_path = os.path.join(output_dir, output_file_name)
            print(output_file_path)
            await update_xml_with_db_values(xml_file_path,     output_file_path,  db_values)
            await update_timestamp_col(conn,
                                    update_flag=True,
                                    table_list=['module_qc_summary'],
                                    column_name='xml_gen_datetime',
                                    part='module',
                                    part_name=module_name)
        
        except Exception as e:
            print('#'*15, f'ERROR for {module_name}','#'*15 ); traceback.print_exc(); print('')

async def main(dbpassword, output_dir, date_start, date_end, lxplus_username, encryption_key=None, partsnamelist=None):
    # Configuration
    yaml_file = 'export_data/table_to_xml_var.yaml'  # Path to YAML file
    xml_file_path = 'export_data/template_examples/module/grading_upload.xml'# XML template file path
    xml_output_dir = output_dir + '/module'  # Directory to save the updated XML

    conn = await get_conn(dbpassword, encryption_key)
    try:
        await process_module(conn, yaml_file, xml_file_path, xml_output_dir, date_start, date_end, lxplus_username, partsnamelist)
    finally:
        await conn.close()

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


"""
https://gitlab.cern.ch/hgcal-database/qc-tables-implementation/-/issues/39
"""