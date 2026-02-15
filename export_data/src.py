import asyncio, asyncpg, pexpect
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
from lxml import etree
import yaml, sys, base64, os, platform, subprocess, glob 
from pathlib import Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..')))
from datetime import datetime
from cryptography.fernet import Fernet
import traceback
import datetime
import tzlocal
import pytz
import re
import requests
import json
import webbrowser
import time
# from zoneinfo import ZoneInfo

resource_yaml = 'export_data/resource.yaml'
with open(resource_yaml, 'r') as file:
    yaml_content = yaml.safe_load(file)
    kind_of_part_yaml = yaml_content['kind_of_part']
    shipping_loc_yaml = yaml_content['shipping_location']

loc = 'dbase_info'
conn_yaml_file = os.path.join(loc, 'conn.yaml')
conn_info = yaml.safe_load(open(conn_yaml_file, 'r'))
db_source_dict = {'dev_db': {'dbname':'INT2R', 'url': 'hgcapi-intg'} , 'prod_db': {'dbname':'CMSR', 'url': 'hgcapi'}}
max_cern_db_request = int(conn_info.get('max_cern_db_request', 1000))
dbloader_hostname = conn_info.get('dbloader_hostname', "dbloader-hgcal") #, "hgcaldbloader.cern.ch")  

db_params = {
    'database': conn_info.get('dbname'),
    'user': 'editor',
    'host': conn_info.get('db_hostname'),
    'port': conn_info.get('port'),
}
partTrans = {'bp' :{'apikey':'baseplates', 'dbtabname': 'bp_inspect', 'db_col': 'bp_name', 'qc_cols': {'grade': 'grade' ,'thickness': 'thickness','comments': 'comment', 'flatness':'flatness', 'weight': 'weight'}},
             'sen':{'apikey':'sensors', 'dbtabname': 'sensor', 'db_col': 'sen_name', 'qc_cols': {'grade': 'grade' ,'thickness': 'thickness','comments': 'comment'}},
             'hxb':{'apikey':'pcbs', 'dbtabname': 'hxb_inspect', 'db_col': 'hxb_name', 'qc_cols': {'grade': 'grade' ,'thickness': 'thickness','comments': 'comment', 'flatness':'flatness', 'weight': 'weight'}},
             'pml':{'apikey':'protomodules', 'dbtabname': 'proto_inspect', 'db_col': 'proto_name' ,'qc_cols':  {'prto_grade': 'grade', 'prto_thkns_mm': 'avg_thickness', "prto_thkns_mm": 'max_thickness', 'prto_fltns_mm': 'flatness', "snsr_x_offst": 'x_offset_mu', "snsr_y_offst": 'y_offset_mu',"snsr_ang_offst": 'ang_offset_deg'}},
             'ml' :{'apikey':'modules', 'dbtabname': 'module_inspect', 'db_col': 'module_name', 'qc_cols':  {'mod_grade': 'grade', 'mod_ave_thkns_mm': 'avg_thickness', "mod_max_thkns_mm": 'max_thickness', 'mod_fltns_mm': 'flatness', "pcb_plcment_x_offset": 'x_offset_mu', "pcb_plcment_y_offset": 'y_offset_mu',"pcb_plcment_ang_offset": 'ang_offset_deg'}},
            }

def find_hgc_db_root(start_path=None):
    if start_path is None:
        start_path = Path.cwd()  # current working directory
    path = Path(start_path).resolve()
    for parent in [path] + list(path.parents):
        if parent.name == "HGC_DB_postgres":
            return parent
    raise FileNotFoundError("HGC_DB_postgres directory not found in current path or parents")


def get_url(partID = None, macID = None, partType = None, cern_db_url = 'hgcapi'):
    if partID is not None:
        return f"https://{cern_db_url}.web.cern.ch/mac/part/{partID}/full"
    elif partType is not None:
        if macID is not None:
            return f"https://{cern_db_url}.web.cern.ch/mac/parts/types/{partTrans[partType.lower()]['apikey']}?page=0&limit={max_cern_db_request}&location={macID}"
        return f"https://{cern_db_url}.web.cern.ch/mac/parts/types/{partTrans[partType.lower()]['apikey']}?page=0&limit={max_cern_db_request}"
    return

def read_from_cern_db(partID = None, macID = None, partType = None , cern_db_url = 'hgcapi'):
    headers = {'Accept': 'application/json'}
    response = requests.get(get_url(partID = partID, macID = macID, partType = partType, cern_db_url = cern_db_url), headers=headers)
    if response.status_code == 200:
        data = response.json() ; 
#         print(json.dumps(data, indent=2))
        return data
    elif response.status_code == 500:
        print(f"Internal Server ERROR for {cern_db_url.upper()}. Try again later.")
    elif response.status_code == 404:
        print(f"Part {partID} not found in {cern_db_url.upper()}. Contact the CERN database team on GitLab: https://gitlab.cern.ch/groups/hgcal-database/-/issues.")
    else:
        if partType:
            print(f"ERROR in reading from {cern_db_url.upper()} for partType : {partType} :: {response.status_code}")
        if partID:
            print(f"ERROR in reading from {cern_db_url.upper()} for partID : {partID} :: {response.status_code}")
        return None

def update_yaml_with_checkboxes(xml_list, checkbox_vars):
    if isinstance(xml_list, list):
        return [{k: checkbox_vars[i][k].get() == 1 for k in item.keys()} for i, item in enumerate(xml_list)]
    elif isinstance(xml_list, dict):
        return {key: update_yaml_with_checkboxes(value, checkbox_vars[key]) for key, value in xml_list.items()}
    return xml_list  

def process_xml_list(xml_list = None, get_yaml_data = False):
    list_of_xmls_yaml = 'export_data/list_of_xmls.yaml'
        
    if get_yaml_data:
        with open(list_of_xmls_yaml, "r") as file:
            return yaml.safe_load(file)
    
    def set_all_to_true(xml_list):
        if isinstance(xml_list, dict):
            for key in xml_list:
                xml_list[key] = set_all_to_true(xml_list[key])
        elif isinstance(xml_list, list):
            xml_list = [set_all_to_true(item) for item in xml_list]
        elif isinstance(xml_list, bool):
            return True
        return xml_list
    
    def set_build_to_true(xml_list):
        if isinstance(xml_list, dict):
            for key in xml_list:
                if not ("build" in key or "cond"in key or "assembly" in key or "pedestal" in key or "iv" in key or "wirebond" in key):
                    xml_list[key] = set_build_to_true(xml_list[key])
        elif isinstance(xml_list, list):
            xml_list = [set_build_to_true(item) for item in xml_list]
        elif isinstance(xml_list, bool):
            return False  ## Change this to True in the nominal case; False when doing development
        return xml_list
    
    if xml_list is None:
        with open(list_of_xmls_yaml, "r") as file:
            xml_list = yaml.safe_load(file)
        xml_list = set_all_to_true(xml_list)
        # xml_list = set_build_to_true(xml_list)

    with open(list_of_xmls_yaml, "w") as file:
        yaml.dump(xml_list, file, default_flow_style=False)

async def check_good_conn(dbpassword, encryption_key = None,  user_type = None):
    temp_conn = await get_conn(dbpassword = dbpassword, encryption_key = encryption_key, user_type = user_type)
    if temp_conn:
        await temp_conn.close()
        return True
    else:
        return False

async def get_conn(dbpassword, encryption_key = None, user_type = None, pool=False):
    user_type = user_type if user_type else 'shipper'
    '''
    Does: get connection to database
    Return: connection
    '''
    loc = 'dbase_info/'
    yaml_file = f'{loc}conn.yaml'
    db_params = {
            'database': yaml.safe_load(open(yaml_file, 'r'))['dbname'],
            'user': f'{user_type}',
            # 'user': 'viewer',
            'host': yaml.safe_load(open(yaml_file, 'r'))['db_hostname']}   
    
    if encryption_key is None:
        db_params.update({'password': dbpassword})
    else:
        cipher_suite = Fernet((encryption_key).encode())
        db_params.update({'password': cipher_suite.decrypt( base64.urlsafe_b64decode(dbpassword)).decode()})
    if pool:
        pool = await asyncpg.create_pool(**db_params)
        return pool
    else:
        try:
            conn = await asyncpg.connect(**db_params)
            return conn
        except Exception as e:
            print(e)
            return None


async def fetch_from_db(query, conn):
    '''
    params: sql query, connection
    return: {[db_col]:[retreived value from table]}
    '''
    result = await conn.fetchrow(query) 
    return dict(result) if result else {}  # Convert the row to a dictionary if it exists


async def update_xml_with_db_values(xml_file_path, output_file_path, db_values):
    try:
        """Update XML template with values from the database."""
        # Parse the XML file
        tree = etree.parse(xml_file_path)
        root = tree.getroot()

        # Convert db_values keys to lowercase for case-insensitive matching
        db_values_lower = {k.lower(): v for k, v in db_values.items()}

        # Iterate through the db_values and replace corresponding placeholders in XML
        for xml_var, value in db_values_lower.items():
            # XPath to find elements containing the placeholder (lowercase comparison)
            elements = root.xpath(f".//*[contains(text(), '{{{{ {xml_var} }}}}')]")

            if elements:
                for element in elements:
                    # Replace the placeholder with the actual value, or empty string if None
                    if value is None:
                        value = ""  # Default to an empty string for None values
                    elif isinstance(value, dict):
                        value = json.dumps(value, separators=(',', ':'))
                        element.text = element.text.replace(f"{{{{ {xml_var} }}}}", value)
                    # Replace the placeholder text
                    else:
                        element.text = element.text.replace(f"{{{{ {xml_var} }}}}", str(value))

        # Handle the 'ID' placeholder separately (case-sensitive)
        if 'ID' in db_values:
            id_value = db_values['ID']
            id_elements = root.xpath(".//*[contains(text(), '{{ ID }}')]")
            if id_elements:
                for element in id_elements:
                    if id_value is None:
                        id_value = ""
                    element.text = element.text.replace("{{ ID }}", str(id_value))

        # Save the updated XML to the output directory

        # Check if the directory to store outputted xml file exists
        output_dir_path = os.path.dirname(output_file_path)
        if not os.path.exists(output_dir_path):
            os.makedirs(output_dir_path)
        
        # save the file to the directory
        if not os.path.isdir(output_file_path):
            tree.write(output_file_path, pretty_print=True, xml_declaration=True, encoding='UTF-8')
            # print(f"XML file updated and saved to: {output_file_path}")
        else:
            print(f"Error: {output_file_path} is a directory, not a file.")
    except Exception as e:    
        print('update_xml_with_db_values', xml_file_path, output_file_path, db_values)        
        traceback.print_exc()
        raise

async def get_parts_name(name, table, conn):
    ##  returns part name in a specific table
    ##  i.e., baseplate-> get bp_name
    query = f"SELECT DISTINCT REPLACE({name},'-','') AS {name} FROM {table};"
    fetched_query = await conn.fetch(query)
    name_list = [record[name] for record in fetched_query]
    return name_list

async def update_timestamp_col(conn, update_flag: bool, table_list: list, column_name: str,  part: str, part_name: str):
    if not update_flag:
        print("Update flag is False. No update performed.")
        return
    try:
        _part_name_col = {'module':'module_name', 
                          'hexaboard':'hxb_name', 
                          'protomodule':'proto_name', 
                          'sensor': 'sen_name',
                          'baseplate':'bp_name'}
        part_name_col = _part_name_col[part]

        # Generate the current timestamp
        current_timestamp = datetime.datetime.now()
        for table in table_list:
            query = f"""
            UPDATE {table}
            SET {column_name} = $1
            WHERE {part_name_col} = $2;
            """
            await conn.execute(query, current_timestamp, part_name)
    except Exception as e:
        traceback.print_exc()
        print(f"Error updating {column_name}: {e}")

def format_part_name(part_name):
    part_name = ('320' + part_name[0:3].replace('320', '') + part_name[3:]).replace('-', '')
    return part_name

def get_run_num(location, timestamp):
    ##  format: SSSSYYMMDDTTTTTT
    shipping_code = shipping_loc_yaml[location]
    formatted_timestamp = timestamp.strftime('%y%m%d%S%f')[:12]
    run_num = f"{shipping_code}{formatted_timestamp}"
    return run_num

async def get_kind_of_part(part_name, part=None, conn=None):
    if part_name[0:4] not in ['320M', '320P']:
        part_name_db = {'baseplate': ['baseplate', 'bp_name'],
                        'hexaboard': ['hexaboard', 'hxb_name'],
                        'sensor':    ['sensor', 'sen_name']}
        
        if part_name[0:4] in ['320B', '320X']:
            part_id = '320' + (part_name[0:3].replace('320', '') + part_name[3:]).replace('-', '')
        else:
            part_id = part_name
        query = f"""SELECT kind FROM {part_name_db[part][0]} WHERE REPLACE({part_name_db[part][1]},'-','') = '{part_id}'; """
        results = await fetch_from_db(query, conn)

        if 'kind' in results:
            if results['kind'] is None:
                raise ValueError(f"Reimport data from INT2R/CMSR for {part_name} to obtain kind_of_part." )
            return f"{results['kind']}"
        else:
            raise ValueError(f"Reimport data from INT2R/CMSR for {part_name} if it exists.")
            return None

    ## part_name can be module_name, hxb_name, proto_name, sen_name, bp_name and so on. 
    else:
        part_type_dict = kind_of_part_yaml['part_type']
        resolution_dict = kind_of_part_yaml['resolution']
        geometry_dict = kind_of_part_yaml['geometry']
        thickness_dict = kind_of_part_yaml['sensor_thickness']
        material_dict = kind_of_part_yaml['material']
        sen_dict = kind_of_part_yaml['sensor']
        sen_geo_dict = kind_of_part_yaml['sensor_geometry']

        try:
            # Extract the information
            if part_name != '' or part_name != 'NoneType':
                part_id = (part_name[0:3].replace('320', '') + part_name[3:]).replace('-', '')
                part_type = part_type_dict[part_id[0]]
                resolution = resolution_dict[part_id[1]]
                geometry = geometry_dict[part_id[2]]
                sen_thickness = thickness_dict[part_id[3]]
                bp_material = material_dict[part_id[4]]
                module_type = ''
                if bp_material == 'CuW':
                    module_type = 'EM'
                elif bp_material in ['Ti', 'CF']:
                    module_type = 'HAD'

                kind_of_part = f'{module_type} {sen_thickness}um Si {part_type} {resolution} {geometry}'
                return kind_of_part
        except Exception as e:
                raise

def format_datetime(input_date, input_time):
    local_timezone = pytz.timezone(str(tzlocal.get_localzone()))
    # Normalize input_date
    if isinstance(input_date, datetime.date):
        input_date = input_date.strftime("%Y-%m-%d")
    if isinstance(input_time, datetime.time):
        input_time = input_time.strftime("%H:%M:%S")
        
    if input_time is None:
        # If time_begin is missing, use the current time with timezone
        current_dt = datetime.datetime.now(local_timezone).time()
        combined_str = f"{input_date} {current_dt}"
        current_dt = datetime.datetime.strptime(combined_str, "%Y-%m-%d %H:%M:%S.%f")
        current_dt = local_timezone.localize(current_dt)
    else:
        # time_begin_str = str(input_time) if isinstance(input_time, datetime.time) else input_time
        time_begin_str = str(input_time)
        if "." in time_begin_str:
            time_format = "%Y-%m-%d %H:%M:%S.%f"
        else:
            time_format = "%Y-%m-%d %H:%M:%S"
        # Combine run_date and time_begin into a full datetime object
        combined_str = f"{input_date} {time_begin_str}"
        current_dt = datetime.datetime.strptime(combined_str, time_format)

        # Attach the system's local timezone
        current_dt = local_timezone.localize(current_dt)
        ## uncomment below if you use python 3.9+
        # current_dt = current_dt.replace(tzinfo=ZoneInfo(str(local_timezone)))

    # Format the output as "YYYY-MM-DD HH:MM:SS+HH:MM"
    formatted_time = current_dt.strftime("%Y-%m-%d %H:%M:%S%z")
    
    # Ensure offset is in "+HH:MM" format
    formatted_time = formatted_time[:-2] + ":" + formatted_time[-2:]
    formatted_time = formatted_time[:-6] + '+' + formatted_time[-5:]

    ### returns the format of "2025-02-10 19:19:44+05:00"
    return formatted_time

def extract_unfilled_variables(filled_xml_file):
    """Extract unfilled variables (e.g., '{{ thickness }}') from the filled XML."""
    with open(filled_xml_file, 'r') as file:
        xml_content = file.read()
    
    # Find placeholders like '{{ thickness }}'
    unfilled_vars = re.findall(r'{{\s*(\w+)\s*}}', xml_content)
    return set(unfilled_vars)  # Use a set to avoid duplicates

def get_missing_db_mappings(yaml_data, filled_xml_file):
    unfilled_vars = extract_unfilled_variables(filled_xml_file)
    missing_entries = []

    for entry in yaml_data:
        xml_temp_val = entry.get('xml_temp_val')
        dbase_col = entry.get('dbase_col')
        dbase_table = entry.get('dbase_table')

        # Only process unfilled XML variables
        if xml_temp_val in unfilled_vars or dbase_col or dbase_table:
            missing_entries.append({
                'xml_temp_val': xml_temp_val,
                'dbase_col': dbase_col,
                'dbase_table': dbase_table
            })

    return missing_entries

def print_missing_entries(missing_entries):
    """Print missing database values in a terminal-friendly table."""
    print('>>> Variables not filled in XML <<<')
    print("=" * 50)
    print(f"{'XML Temp Val':<20} | {'Database Column':<20} | {'Database Table'}")
    print("=" * 50)
    
    for entry in missing_entries:
        print(f"{entry['xml_temp_val']:<20} | {entry['dbase_col']:<20} | {entry['dbase_table']}")
    
    print("=" * 50 + "\n")

def get_roc_version(module_name):
    identifier = module_name[-7]
    if identifier:
        roc_version = kind_of_part_yaml['roc_version'][identifier]
        if roc_version is None:
            return 'not specified'
        else:
            return roc_version
    else:
        raise ValueError(f"Cannot determine the roc version of {module_name}")
    
async def get_nearest_temp_humidity(conn, table_name, date_inspect, time_inspect):
    print(table_name, date_inspect, time_inspect)
    print(type(table_name), type(date_inspect), type(time_inspect))
    if isinstance(date_inspect, str):
        date_inspect = datetime.datetime.strptime(date_inspect.strip(), "%Y-%m-%d").date()

    if isinstance(time_inspect, str):
        # handle both HH:MM and HH:MM:SS
        time_format = "%H:%M:%S" if time_inspect.count(":") == 2 else "%H:%M"
        time_inspect = datetime.datetime.strptime(time_inspect.strip(), time_format).time()

    target_dt = datetime.datetime.combine(date_inspect, time_inspect)
    
    # Step 1: Check which columns exist
    query_columns = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = $1
    """
    columns_result = await conn.fetch(query_columns, table_name)
    columns = {row['column_name'] for row in columns_result}

    temp_c, rel_hum = None, None

    # Step 2: Try fetching from the target table (if columns exist)
    if 'temp_c' in columns and 'rel_hum' in columns:
        try:
            row = await conn.fetchrow(f"SELECT temp_c, rel_hum FROM {table_name} LIMIT 1;")
            if row:
                temp_c, rel_hum = row['temp_c'], row['rel_hum']
        except Exception:
            pass  # In case the table is empty or fetch fails

    # Step 3: If missing or None, get nearest from temp_humidity
    if temp_c is None or rel_hum is None:
        query_nearest = """
            SELECT temp_c, rel_hum
            FROM temp_humidity
            ORDER BY ABS(EXTRACT(EPOCH FROM (log_timestamp - $1))) ASC
            LIMIT 1;
        """
        row = await conn.fetchrow(query_nearest, target_dt)
        if row:
            temp_c, rel_hum = row['temp_c'], row['rel_hum']

    return {'temp_c': temp_c, 'rel_hum': rel_hum}

################################################################################
### Below is for checking part exisistence and combination with location ###
################################################################################
def get_url(partID = None, macID = None, partType = None, cern_db_url = 'hgcapi'):
    if partID is not None:
        return f'https://{cern_db_url}.web.cern.ch/mac/part/{partID}/full'
    elif partType is not None:
        if macID is not None:
            return f'https://{cern_db_url}.web.cern.ch/mac/parts/types/{partTrans[partType.lower()]["apikey"]}?page=0&limit={max_cern_db_request}&location={macID}'
        return f'https://{cern_db_url}.web.cern.ch/mac/parts/types/{partTrans[partType.lower()]["apikey"]}?page=0&limit={max_cern_db_request}'
    return

def read_from_cern_db(partID = None, macID = None, partType = None , cern_db_url = 'hgcapi'):
    headers = {'Accept': 'application/json'}
    response = requests.get(get_url(partID = partID, macID = macID, partType = partType, cern_db_url = cern_db_url), headers=headers)
    if response.status_code == 200:
        data = response.json() ; 
#         print(json.dumps(data, indent=2))
        return data
    elif response.status_code == 500:
        print(f'Internal Server ERROR for {cern_db_url.upper()}. Try again later.')
    elif response.status_code == 404:
        print(f'Part {partID} not found in {cern_db_url.upper()}. Contact the CERN database team on GitLab: https://gitlab.cern.ch/groups/hgcal-database/-/issues.')
    else:
        if partType:
            print(f'ERROR in reading from {cern_db_url.upper()} for partType : {partType} :: {response.status_code}')
        if partID:
            print(f'ERROR in reading from {cern_db_url.upper()} for partID : {partID} :: {response.status_code}')
        return None

def get_location_and_partid(part_id: str, part_type: str, cern_db_url: str = "hgcapi") -> list:
    try:
        data = read_from_cern_db(partID=part_id, partType=part_type, cern_db_url=cern_db_url)
        if not data:
            print(f"Warning: No data found for part_id = {part_id}")
            return []

        location = data.get("location")
        serial_number = data.get("serial_number")

        if location and serial_number:
            print(f'output --- {[location, serial_number]}')
            return [location, serial_number]
        else:
            print(f"Warning: Missing fields in API response for part_id = {part_id}")
            return []
    except Exception as e:
        print(f"Exception occurred while querying part_id = {part_id}: {e}")
        return []

async def run_async_subprocess():
    controlpathname = "ctrl_dbloader"
    current_file = Path(__file__).resolve()
    PROJECT_ROOT = next(p for p in current_file.parents if p.name == "HGC_DB_postgres") ## Global path of HGC_DB_postgres
    print('Running run_async_subprocess')
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        f"import sys; sys.path.insert(0, r'{PROJECT_ROOT}'); "
        "from task_scheduler.scheduler_helper import run_ssh_master; "
        "run_ssh_master()",
        stdout=asyncio.subprocess.DEVNULL,  ### Comment these to see terminal output for debugging
        stderr=asyncio.subprocess.DEVNULL,  ### Comment these to see terminal output for debugging
        stdin=asyncio.subprocess.DEVNULL,   ### Comment these to see terminal output for debugging
        start_new_session=True  )

    print('Waiting for asynchronous control master process to start...')
    time.sleep(15)  ### Wait a good few seconds for process to start!
    cp_check = Path(f"~/.ssh/{controlpathname}").expanduser().exists()
    print(cp_check, 'THISS')
    return cp_check 


def open_scp_connection(dbl_username = None, scp_persist_minutes = 240, scp_force_quit = False, get_scp_status = False):
    controlpathname = "ctrl_dbloader"
    test_cmd = ["ssh", "-Y",
                "-o", f"ControlPath=~/.ssh/{controlpathname}",
                "-O", "check",     # <-- ask the master process if it’s alive
                f"{dbl_username}@{controlpathname}"]
    if get_scp_status:
        result = subprocess.run(test_cmd, capture_output=True, text=True)
        return result.returncode

    if scp_force_quit:
        quit_cmd = ["ssh", "-O", "exit",
                    "-o", f"ControlPath=~/.ssh/{controlpathname}", f"{dbl_username}@{controlpathname}"]
        subprocess.run(quit_cmd, check=True)
        result = subprocess.run(test_cmd, capture_output=True, text=True)
        if result.returncode != 255: ## or result.returncode == 0:
            print("Failed to close ControlMaster Process. Do it manually.")
            print(f"`ssh -O exit -o ControlPath=~/.ssh/{controlpathname} {dbl_username}@{controlpathname}`")
        else:
            print("ControlMaster process closed.")
        return result.returncode

    result = subprocess.run(test_cmd, capture_output=True, text=True)    
    if result.returncode != 0 and dbl_username:
        ### Process is not alive but residual files exist that need to be deletes
        pattern = os.path.expanduser(f"~/.ssh/{controlpathname}") 
        controlfiles =  glob.glob(pattern)
        if len(controlfiles) > 0:
            try:
                for cf in controlfiles:
                    os.remove(cf)
                    print(f"Removed existing control file: {cf}")
            except:
                print(f"Failed to remove control files: {controlfiles}")
        try:
            # print(f"Running on {platform.system()}")
            if platform.system() == "Windows":
                print("")
                print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                print("SSH ControlMaster unavailabele for Windows.")
                print("Install Windows Subsystem for Linux (WSL) and reclone this repository in a Linux space.")
                print("https://learn.microsoft.com/en-us/windows/wsl/install")
                print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                print("")
                webbrowser.open(f"https://learn.microsoft.com/en-us/windows/wsl/install")
               
            else: ## platform.system() == "Linux" or platform.system() == "Darwin" 
                print("")
                print("****************************************")
                print("******* LXPLUS LOGIN CREDENTIALS *******")
                print("****************************************")
                print("")

                scp_timeout_cond = scp_persist_minutes if scp_persist_minutes == 'yes' else f"{scp_persist_minutes}m"    
                ### opens to only dbloader_hostname via lxplus
                ssh_cmd = ["ssh", "-MNfY",
                    "-o", "ControlMaster=yes",
                    "-o", f"ControlPath=~/.ssh/{controlpathname}",    
                    "-o", f"ControlPersist={scp_timeout_cond}",
                    "-o", f"ProxyJump={dbl_username}@lxtunnel.cern.ch",
                    f"{dbl_username}@{dbloader_hostname}"]    
                
                if False: #Path("/tmp/my_cron_job.running").exists():
                    asyncio.run(run_async_subprocess())
                    print("** SSH ControlMaster session started. **")
                    print("****************************************")
                    print(f"To force quit this open connection manually, run below command in your terminal:")
                    print(f"`ssh -O exit -o ControlPath=~/.ssh/{controlpathname} {dbl_username}@{controlpathname}`")
                    print("****************************************")
                    print("")
                else:    
                    subprocess.run(ssh_cmd, check=True)
                    print("** SSH ControlMaster session started. **")
                    print("****************************************")
                    print("")
                    print("************* PLEASE NOTE **************")
                    print(f"ControlMaster process will be alive for {scp_persist_minutes} minutes.")
                    print(f"To change this, define 'scp_persist_minutes: 240' in dbase_info/conn.yaml.")
                    print(f"To allow password-free SCP to your LXPLUS for {scp_persist_minutes} minutes...")
                    print(f"define 'scp_force_quit: False' in dbase_info/conn.yaml.")
                    print(f"To force quit this open connection manually, run below command in your terminal:")
                    print(f"`ssh -O exit -o ControlPath=~/.ssh/{controlpathname} {dbl_username}@{controlpathname}`")
                    print("****************************************")
                    print("")


        except Exception as e:
            print(f"Failed to create control file.")
            traceback.print_exc()
    
    result = subprocess.run(test_cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print("ControlMaster process alive.")
    else:
        print("ControlMaster process failed.")
    ## ssh -O exit -o ControlPath=~/.ssh/scp-{dbl_username}@{controlpathname} {dbl_username}@{controlpathname} ## To kill process
    # ssh -O exit -o ControlPath=~/.ssh/ctrl_lxplus_dbloader simurthy@ctrl_lxplus_dbloader
    return result.returncode
    
    # print(result.stdout)
    # print(result.stderr)
