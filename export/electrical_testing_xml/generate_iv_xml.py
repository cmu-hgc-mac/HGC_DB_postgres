import asyncpg, asyncio, os, yaml, pwinput
from jinja2 import Template
import numpy as np


def round_to_n_sig_figs(num, n = 4):
    if num == 0:
        return 0.0
    else:
        return round(num, n - int(np.floor(np.log10(abs(num)))) - 1)

async def get_conn():
    loc = '../../dbase_info/'
    yaml_file = f'{loc}tables.yaml'
    db_params = {
        'database': yaml.safe_load(open(yaml_file, 'r'))['dbname'],
        'user': 'postgres',
        'password': pwinput.pwinput(prompt='Enter superuser password: ', mask='*'),
        'host': yaml.safe_load(open(yaml_file, 'r'))['db_hostname']}   
    conn = await asyncpg.connect(**db_params)
    return conn

async def get_last_entry(tech_name):    
    conn = await get_conn()
    col_names = ['module_name', 
                 'status', 'status_desc', 'grade', 'ratio_i_at_vs', 'ratio_at_vs', 'rel_hum', 'temp_c',
                  'program_v', 'meas_v', 'meas_i', 'meas_r','date_test','time_test','comment',
                 'inspector',]
    col_query = ', '.join(col_names)
    query = f'''SELECT DISTINCT ON (module_name) {col_query} FROM module_iv_test ORDER BY module_name, mod_ivtest_no DESC;'''
    rows = await conn.fetch(query)
    if rows:
        data = [{key: row[key] for key in row.keys()} for row in rows]
        for rown in range(len(data)):
            for key in ['meas_v', 'meas_i', 'meas_r']:
                data[rown][key] = [round_to_n_sig_figs(num) for num in data[rown][key]]
        data = [{key: str(row[key]).replace(" ","") for key in row.keys()} for row in data]
        for row in data:
            row["run_type"] = f"SI MODULE IV TEST {row['module_name'].replace('-','')}"
            row["run_number"] = f"1"
            row["run_begin_timestamp"] = f"{row['date_test']} {row['time_test']}"
            row["run_end_timestamp"] = f"{row['date_test']} {row['time_test']}"
            row["initiated_by_user"] = str(tech_name)
            row["comment_description"] = row.pop("comment")
            row["serial_number"] = row["module_name"]
    await conn.close()
    return data

def save_xml(data, template_file):
    filedir = "./xmls"
    if not os.path.exists(filedir): os.makedirs(filedir)
    for prop_dict in data:
        template_file_name = os.path.basename(template_file)
        outfile = f"module_{prop_dict['module_name']}_iv_test_upload.xml"
        with open(template_file, 'r') as file:
            template_content = file.read()
            template = Template(template_content)
            rendered = template.render(prop_dict)
        with open(os.path.join(filedir, outfile), 'w') as file:
            print("Writing to", os.path.join(filedir, outfile))
            file.write(rendered)

def main():
    template_dir = "./"
    template = "module_iv_test_upload.xml"
    template_file = os.sep.join([template_dir, template])
    tech_name = "simurthy"
    try:
        data = asyncio.run(get_last_entry(tech_name)) ## python 3.7
    except:
        data = (asyncio.get_event_loop()).run_until_complete(get_last_entry(tech_name)) ## python 3.6

    save_xml(data, template_file)

main()