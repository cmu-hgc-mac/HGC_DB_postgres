import asyncio
import asyncpg
import argparse
import json
import numpy as np
import sys, os, yaml, argparse, datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from export_data.src import *
from export_data.define_global_var import LOCATION, INSTITUTION

def fetch_module_iv_data(prog_v, meas_v, meas_i, meas_r):
    return {
        'program_v': prog_v,
        'meas_v': meas_v,
        'meas_i': meas_i,
        'meas_r': meas_r
    }

async def process_module(conn, yaml_file, xml_file_path, output_dir, date_start, date_end, lxplus_username, partsnamelist=None):
    with open(yaml_file, 'r') as file:
        yaml_data = yaml.safe_load(file)
    xml_data = yaml_data['module_iv']

    ## list module_names that we want to generate xmls
    module_list = set()
    if partsnamelist:
        query = """
            SELECT module_name, status, mod_ivtest_no
            FROM module_iv_test
            WHERE module_name = ANY($1)
        """
        results = await conn.fetch(query, partsnamelist)
    else:
        query = f"""
            SELECT module_name, status, mod_ivtest_no
            FROM module_iv_test
            WHERE module_iv_test.date_test BETWEEN '{date_start}' AND '{date_end}'
        """
        results = await conn.fetch(query)

    module_status_list = set((row['module_name'], row['status'], row['mod_ivtest_no']) for row in results if 'module_name' in row and 'status' in row)
    for module_name, status, mod_ivtest_no in module_status_list:
        print(f'--> {module_name} with mod_ivtest_no {mod_ivtest_no}...')
        try:
            db_values = {}
            for entry in xml_data:
                xml_var = entry['xml_temp_val']

                if xml_var == 'LOCATION':
                    db_values[xml_var] = LOCATION
                elif xml_var == 'INSTITUTION':
                    db_values[xml_var] = INSTITUTION
                elif xml_var == 'ID':
                    db_values[xml_var] = module_name
                elif xml_var == 'KIND_OF_PART':
                    db_values[xml_var] = await get_kind_of_part(module_name, 'module', conn)
                elif xml_var == 'INITIATED_BY_USER':
                    db_values[xml_var] = lxplus_username
                else:
                    dbase_col = entry['dbase_col']
                    dbase_table = entry['dbase_table']

                    # Skip entries without a database column or table
                    if not dbase_col and not dbase_table:
                        continue
                
                    query = f"""
                    SELECT {dbase_col} FROM {dbase_table} WHERE module_name = '{module_name}' AND mod_ivtest_no = {mod_ivtest_no}
                    """
                    try:
                        results = await fetch_from_db(query, conn)
                    except Exception as e:
                            print('QUERY:', query)
                            print('ERROR:', e)
                    
                    if results:
                        ref_volt_a, ref_volt_b = 400, 500
                        if xml_var == "RUN_BEGIN_TIMESTAMP_":
                            # Fetching both ass_run_date and ass_time_begin
                            run_date = results.get("date_test", "")
                            time_begin = results.get("time_test", "")
                            db_values[xml_var] = format_datetime(run_date, time_begin)
                        elif xml_var == "RUN_END_TIMESTAMP_":
                            run_date = results.get("date_test", "")
                            time_end = results.get("time_test", "")
                            db_values[xml_var] = format_datetime(run_date, time_end)
                        elif xml_var == 'RUN_NUMBER':
                            run_date = results.get("date_test", "")
                            time_begin = results.get("time_test", "")
                            combined_str = f"{run_date} {time_begin}"

                            try:
                                dt_obj = datetime.datetime.strptime(combined_str, "%Y-%m-%d %H:%M:%S.%f")
                            except ValueError:
                                dt_obj = datetime.datetime.strptime(combined_str, "%Y-%m-%d %H:%M:%S")
                            
                            db_values[xml_var] = get_run_num(LOCATION, dt_obj)
                        elif xml_var == 'REF_VOLT_A':
                            db_values[xml_var] = ref_volt_a  #results.get("ratio_at_vs", "")[0]
                        elif xml_var == 'REF_VOLT_B':
                            db_values[xml_var] = ref_volt_b #results.get("ratio_at_vs", "")[1]
                        elif xml_var == 'DATA_POINT_COUNT':
                            _prog_v = results.get('program_v', "")
                            db_values[xml_var] = len(_prog_v)
                        elif xml_var == 'CURRENT_AMPS_AT_VOLT_A':
                            prog_v = np.array(results.get('program_v', ""))
                            # ratio_at_vs = results.get('ratio_at_vs', "")
                            # ref_volt_a = ratio_at_vs[0]
                            if max(prog_v) < ref_volt_a:
                                ind_volt_a = np.argmax(prog_v)
                            else:
                                ind_volt_a = np.argwhere(prog_v == ref_volt_a).flatten()[0]
                            meas_i = np.array(results.get('meas_i', ""))
                            db_values[xml_var] = meas_i[ind_volt_a]
                        elif xml_var == 'CURRENT_RATIO_B_OVER_A':
                            prog_v = np.array(results.get('program_v', ""))
                            # ratio_at_vs = results.get('ratio_at_vs', "")
                            # ref_volt_a = ratio_at_vs[0]
                            # ref_volt_b = ratio_at_vs[1]
                            if max(prog_v) < ref_volt_a:
                                ind_volt_a = np.argmax(prog_v)
                            else:
                                ind_volt_a = np.argwhere(prog_v == ref_volt_a).flatten()[0]
                            if max(prog_v) < ref_volt_b:
                                ind_volt_b = np.argmax(prog_v)
                            else:
                                ind_volt_b = np.argwhere(prog_v == ref_volt_b).flatten()[0]
                            meas_i = np.array(results.get('meas_i', ""))
                            db_values[xml_var] = meas_i[ind_volt_b]/meas_i[ind_volt_a]
                        elif xml_var == 'DATA_POINTS_JSON':
                            prog_v = results.get('program_v', "")
                            meas_i = results.get('meas_i', "")
                            meas_r = results.get('meas_r', "")
                            meas_v = results.get('meas_v', "")
                            db_values[xml_var] = fetch_module_iv_data(prog_v, meas_v, meas_i, meas_r)
                        else:
                            db_values[xml_var] = results.get(dbase_col, '')

            # Update the XML with the database values
            combined_str_mod = str(combined_str).replace(" ","T").replace(":","").split('.')[0]
            output_file_name = f"{module_name}_{combined_str_mod}_iv.xml"
            output_file_path = os.path.join(output_dir, output_file_name)
            await update_xml_with_db_values(xml_file_path, output_file_path, db_values)
            await update_timestamp_col(conn,
                                    update_flag=True,
                                    table_list=['module_iv_test'],
                                    column_name='xml_gen_datetime',
                                    part='module',
                                    part_name=module_name)
        
        except Exception as e:
            print('#'*15, f'ERROR for {module_name}','#'*15 ); traceback.print_exc(); print('')

async def main(dbpassword, output_dir, date_start, date_end, lxplus_username, encryption_key=None, partsnamelist=None):
    # Configuration
    yaml_file = 'export_data/table_to_xml_var.yaml'  # Path to YAML file
    xml_file_path = 'export_data/template_examples/testing/module_iv_test.xml'# XML template file path
    xml_output_dir = output_dir + '/testing/iv'  # Directory to save the updated XML

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
