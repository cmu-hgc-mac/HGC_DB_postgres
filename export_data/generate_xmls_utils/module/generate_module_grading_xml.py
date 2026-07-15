import asyncio
import asyncpg
import argparse
import json
import numpy as np
import sys, os, yaml, argparse, datetime, time
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
    MODULE_CORNER_COLORGRADE, worst_letter_grade = 'null', 'null'
    if all_letter_grades:
        all_letter_grades = [str(grade) for grade in all_letter_grades]
        worst_letter_grade = 'null' if 'None' in all_letter_grades else max(all_letter_grades, key=str.lower)
    if mod_corner_colors:
        mod_corner_colors = [c.lower() for c in mod_corner_colors]
        MODULE_CORNER_COLORGRADE = 'red' if 'red' in mod_corner_colors else ('yellow' if 'yellow' in mod_corner_colors else ('purple' if 'purple' in mod_corner_colors else 'green'))
    
    installation_score = 0 if (MODULE_CORNER_COLORGRADE in ['red','null','None'] or worst_letter_grade.upper() in ['F','null','None']) else 1
    return installation_score, MODULE_CORNER_COLORGRADE

async def process_module(conn, yaml_file, xml_file_path, output_dir, date_start, date_end, lxplus_username, partsnamelist=None, skip_uploaded=True):
    with open(yaml_file, 'r') as file:
        yaml_data = yaml.safe_load(file)
    xml_data  = yaml_data['module_grading']

    ## list module_names that we want to generate xmls
    module_list = set()
    try:
        if partsnamelist:
            if skip_uploaded:
                query = f"""SELECT m.module_name, m.mod_qc_no, m.grade_timestamp FROM module_qc_summary m JOIN (SELECT module_name, MAX(mod_qc_no) AS max_mod_qc_no FROM module_qc_summary WHERE module_name = ANY($1) AND (xml_upload_success IS NULL OR xml_upload_success = FALSE) GROUP BY module_name) latest
                    ON latest.module_name = m.module_name AND latest.max_mod_qc_no = m.mod_qc_no ORDER BY m.module_name DESC """  ### Get the latest for each requested module
            else:
                query = f"""SELECT m.module_name, m.mod_qc_no, m.grade_timestamp FROM module_qc_summary m JOIN (SELECT module_name, MAX(mod_qc_no) AS max_mod_qc_no FROM module_qc_summary WHERE module_name = ANY($1) GROUP BY module_name) latest
                    ON latest.module_name = m.module_name AND latest.max_mod_qc_no = m.mod_qc_no ORDER BY m.module_name DESC """  ### Get the latest for each requested module
            results = await conn.fetch(query, partsnamelist)
        else:
            if skip_uploaded:
                query = f"""SELECT m.module_name, m.mod_qc_no, m.grade_timestamp FROM module_qc_summary m JOIN (SELECT module_name, MAX(mod_qc_no) AS max_mod_qc_no FROM module_qc_summary WHERE (xml_upload_success IS NULL OR xml_upload_success = FALSE) GROUP BY module_name) latest
                    ON latest.module_name = m.module_name AND latest.max_mod_qc_no = m.mod_qc_no ORDER BY m.module_name DESC """ ### Get the latest for all modules ever
            else:
                query = f"""SELECT m.module_name, m.mod_qc_no, m.grade_timestamp FROM module_qc_summary m JOIN (SELECT module_name, MAX(mod_qc_no) AS max_mod_qc_no FROM module_qc_summary GROUP BY module_name) latest
                    ON latest.module_name = m.module_name AND latest.max_mod_qc_no = m.mod_qc_no ORDER BY m.module_name DESC """ ### Get the latest for all modules ever
            results = await conn.fetch(query)
        module_list = set((row['module_name'], row['mod_qc_no'], row['grade_timestamp']) for row in results if 'module_name' in row)
    except:   #### In the case that the MACs have not created the timestamp column
        if partsnamelist:
            if skip_uploaded:
                query = f"""SELECT m.module_name, m.mod_qc_no FROM module_qc_summary m JOIN (SELECT module_name, MAX(mod_qc_no) AS max_mod_qc_no FROM module_qc_summary WHERE module_name = ANY($1) AND (xml_upload_success IS NULL OR xml_upload_success = FALSE) GROUP BY module_name) latest
                    ON latest.module_name = m.module_name AND latest.max_mod_qc_no = m.mod_qc_no ORDER BY m.module_name DESC """  ### Get the latest for each requested module
            else:
                query = f"""SELECT m.module_name, m.mod_qc_no FROM module_qc_summary m JOIN (SELECT module_name, MAX(mod_qc_no) AS max_mod_qc_no FROM module_qc_summary WHERE module_name = ANY($1) GROUP BY module_name) latest
                    ON latest.module_name = m.module_name AND latest.max_mod_qc_no = m.mod_qc_no ORDER BY m.module_name DESC """  ### Get the latest for each requested module
            results = await conn.fetch(query, partsnamelist)
        else:
            if skip_uploaded:
                query = f"""SELECT m.module_name, m.mod_qc_no FROM module_qc_summary m JOIN (SELECT module_name, MAX(mod_qc_no) AS max_mod_qc_no FROM module_qc_summary WHERE (xml_upload_success IS NULL OR xml_upload_success = FALSE) GROUP BY module_name) latest
                    ON latest.module_name = m.module_name AND latest.max_mod_qc_no = m.mod_qc_no ORDER BY m.module_name DESC """ ### Get the latest for all modules ever
            else:
                query = f"""SELECT m.module_name, m.mod_qc_no FROM module_qc_summary m JOIN (SELECT module_name, MAX(mod_qc_no) AS max_mod_qc_no FROM module_qc_summary GROUP BY module_name) latest
                    ON latest.module_name = m.module_name AND latest.max_mod_qc_no = m.mod_qc_no ORDER BY m.module_name DESC """ ### Get the latest for all modules ever
            results = await conn.fetch(query)
            for row in results:
                time.sleep(1)  # Sleep for 1 second
                datetimenow = datetime.datetime.now()  # Get the current timestamp
                module_list.add((row['module_name'], row['mod_qc_no'], datetimenow))


    for module_name, mod_qc_no, grade_timestamp in module_list:
        if not grade_timestamp:
            time.sleep(1)  ### this is to ensure unique run numbers!
            grade_timestamp = datetime.datetime.now()

        combined_str = grade_timestamp ### "" ### initialize
        
        try:
            db_values = {
                'LOCATION': LOCATION,
                'INSTITUTION': INSTITUTION,
                'ID': module_name,
                'KIND_OF_PART': await get_kind_of_part(module_name, 'module', conn),
                'INITIATED_BY_USER': lxplus_username,
                'RUN_BEGIN_TIMESTAMP_': grade_timestamp,
                'RUN_END_TIMESTAMP_': grade_timestamp,
                'RUN_TYPE': "Si module grading",
                'RUN_NUMBER': get_run_num(LOCATION, grade_timestamp),
                'COMMENT_DESCRIPTION': f"MAC grades for {module_name}",
            }
            
            ### module_qc_summary backs most entries and always uses the same WHERE clause,
            ### so fetch that row once instead of once per column.
            qc_summary_query = f"""SELECT * FROM module_qc_summary WHERE module_name = '{module_name}' AND mod_qc_no = {mod_qc_no} """
            qc_summary_row = await fetch_from_db(qc_summary_query, conn) or {}

            mod_corner_colors = qc_summary_row.get('module_corner_colorgrades', "")
            _, mod_colorgrade = fetch_module_grades(mod_corner_colors, all_letter_grades=None)
            db_values['MODULE_CORNER_COLORGRADE'] = mod_colorgrade

            count_bad_cells = qc_summary_row.get('count_bad_cells') or 0
            total_cell_count = int(cell_count_for_res_geom[module_name[4:6]])
            db_values['PERCENT_BAD_CELLS'] = str(round(int(count_bad_cells)*100/total_cell_count, 3))

            for entry in xml_data:  ### loaded from table_to_xml_var.yaml
                xml_var = entry['xml_temp_val']
                if xml_var in db_values:
                    continue

                dbase_col = entry['dbase_col']

                # Skip entries without a database column or table
                if not dbase_col and not entry['dbase_table']:
                    continue

                if not qc_summary_row:
                    continue

                db_values[xml_var] = qc_summary_row.get(dbase_col, '')


            ### Determine Installation Criteria
            grades_reason = [''             ,'protoGeom'      ,'modGeom'         ,'IV'      , 'ROC']
            grades_to_get = ['OVERALL_GRADE','PROTO_MECH_GRADE','MODULE_MECH_GRADE','IV_GRADE','READOUT_GRADE']
            all_letter_grades = [db_values.get(grade_type, "F") for grade_type in grades_to_get]
            # mod_corner_colors = db_values.get('MODULE_CORNER_COLORS', ["purple"])
            # installation_score, mod_colorgrade = fetch_module_grades(mod_corner_colors, all_letter_grades)
            if db_values.get('INSTALLATION_MODULE') not in [0,1,2]:
                db_values['INSTALLATION_MODULE'] = 9 ### definition undefined

            final_grade_reason = qc_summary_row.get('final_grade_reason')

            if final_grade_reason:
                reason = f" ({final_grade_reason})"
            else:
                indices, reason = [], ""
                if all_letter_grades[0] != 'A':
                    indices = [i for i, grade in enumerate(all_letter_grades[1:]) if grade == all_letter_grades[0]]
                    if indices:
                        reason = " (" + ",".join(grades_reason[i] for i in indices) + ")"

            db_values['OVERALL_GRADE'] = all_letter_grades[0] + reason

            # Update the XML with the database values
            combined_str_mod = str(combined_str).replace("-","").replace(" ","T").replace(":","").split('.')[0]
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

async def main(dbpassword, output_dir, date_start, date_end, lxplus_username, encryption_key=None, partsnamelist=None, skip_uploaded=True):
    # Configuration
    yaml_file = 'export_data/table_to_xml_var.yaml'  # Path to YAML file
    xml_file_path = 'export_data/template_examples/module/grading_upload.xml'# XML template file path
    xml_output_dir = output_dir + '/module'  # Directory to save the updated XML

    conn = await get_conn(dbpassword, encryption_key)
    try:
        await process_module(conn, yaml_file, xml_file_path, xml_output_dir, date_start, date_end, lxplus_username, partsnamelist, skip_uploaded)
    finally:
        await conn.close()

if __name__ == "__main__":
    today = datetime.datetime.today().strftime('%Y-%m-%d')

    parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
    parser.add_argument('-dbp', '--dbpassword', default=None, required=False, help="Password to access database.")
    parser.add_argument('-lxu', '--lxpusername', default=None, required=False, help="Username to access lxplus.")
    parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
    parser.add_argument('-dir','--directory', default=None, help="The directory to process. Default is ../../xmls_for_dbloader_upload.")
    parser.add_argument('-datestart', '--date_start', type=lambda s: str(datetime.datetime.strptime(s, '%Y-%m-%d').date()), default=str(today), help=f"Date for XML generated (format: YYYY-MM-DD). Default is today's date: {today}")
    parser.add_argument('-dateend', '--date_end', type=lambda s: str(datetime.datetime.strptime(s, '%Y-%m-%d').date()), default=str(today), help=f"Date for XML generated (format: YYYY-MM-DD). Default is today's date: {today}")
    parser.add_argument("-pn", '--partnameslist', nargs="+", help="Space-separated list", required=False)
    parser.add_argument('-skup', '--skip_uploaded', default='True', required=False, help="Skip rows that have already been uploaded")
    args = parser.parse_args()   

    lxplus_username = args.lxpusername
    dbpassword = args.dbpassword
    output_dir = args.directory
    encryption_key = args.encrypt_key
    date_start = args.date_start
    date_end = args.date_end
    partsnamelist = args.partnameslist
    skip_uploaded = str2bool(args.skip_uploaded)

    asyncio.run(main(dbpassword = dbpassword, output_dir = output_dir, encryption_key = encryption_key, date_start=date_start, date_end=date_end, lxplus_username=lxplus_username, partsnamelist=partsnamelist, skip_uploaded = skip_uploaded))


"""
https://gitlab.cern.ch/hgcal-database/qc-tables-implementation/-/issues/39
"""