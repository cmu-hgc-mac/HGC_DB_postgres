import asyncio, asyncpg, pwinput
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
from lxml import etree
import yaml, sys, argparse, base64, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..')))
from datetime import datetime
from cryptography.fernet import Fernet
import traceback
import datetime
import tzlocal
import pytz
import re
# from zoneinfo import ZoneInfo

resource_yaml = 'export_data/resource.yaml'
with open(resource_yaml, 'r') as file:
    yaml_content = yaml.safe_load(file)
    kind_of_part_yaml = yaml_content['kind_of_part']
    shipping_loc_yaml = yaml_content['shipping_location']


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
                if "build" not in key:
                    xml_list[key] = set_build_to_true(xml_list[key])
        elif isinstance(xml_list, list):
            xml_list = [set_build_to_true(item) for item in xml_list]
        elif isinstance(xml_list, bool):
            return False
        return xml_list
    
    if xml_list is None:
        with open(list_of_xmls_yaml, "r") as file:
            xml_list = yaml.safe_load(file)
        xml_list = set_all_to_true(xml_list)
        xml_list = set_build_to_true(xml_list)

    with open(list_of_xmls_yaml, "w") as file:
        yaml.dump(xml_list, file, default_flow_style=False)


async def get_conn(dbpassword, encryption_key = None):
    '''
    Does: get connection to database
    Return: connection
    '''
    loc = 'dbase_info/'
    yaml_file = f'{loc}conn.yaml'
    db_params = {
            'database': yaml.safe_load(open(yaml_file, 'r'))['dbname'],
            'user': 'shipper',
            # 'user': 'viewer',
            'host': yaml.safe_load(open(yaml_file, 'r'))['db_hostname']}   
    
    if encryption_key is None:
        db_params.update({'password': dbpassword})
    else:
        cipher_suite = Fernet((encryption_key).encode())
        db_params.update({'password': cipher_suite.decrypt( base64.urlsafe_b64decode(dbpassword)).decode()})

    conn = await asyncpg.connect(**db_params)
    return conn

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

                    # Replace the placeholder text
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

def get_run_num(location):
    ##  format: SSSSYYMMDDTTTTTT
    shipping_code = shipping_loc_yaml[location]
    timestamp = datetime.datetime.now()
    formatted_timestamp = timestamp.strftime('%y%m%d%S%f')[:12]
    run_num = str(shipping_code) + formatted_timestamp
    return run_num

def get_kind_of_part(part_name):
    ## part_name can be module_name, hxb_name, proto_name, sen_name, bp_name and so on. 
    
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
            if part_name.replace('_', '').isdigit() == True:
                ## this is for sensor. 
                ## 2) convension v2
                ## TXXXXX_N: [thickness / resolution]XXXXX_[geometry]
                part_id = part_name
                sen_thickness = sen_dict[part_id[0]][0]
                resolution = sen_dict[part_id[0]][1]
                sen_geometry = sen_geo_dict[part_id[-1]]
                part_type = 'Sensor'
                kind_of_part = f'{sen_thickness}um Si {part_type} {resolution} {sen_geometry}'  

            else:
                part_id = (part_name[0:3].replace('320', '') + part_name[3:]).replace('-', '')
                part_type = part_type_dict[part_id[0]]
                if part_type == 'Hexaboard':
                    ## below is updated version (rev.4.0)
                    ## eg. 320-XL-F03-PN-00063
                    resolution = resolution_dict[part_id[1]]
                    geometry = geometry_dict[part_id[2]]
                    kind_of_part = f'Hexaboard {resolution} {geometry}'  

                elif part_type == 'Baseplate':
                    ## 320-BA-TTT-VB-NNNN
                    ### TTT: [geometry][resolution][bp_material]
                    ## below is updated version (rev.4.0)
                    geometry = geometry_dict[part_id[2]]
                    resolution = resolution_dict[part_id[3]]
                    bp_material = material_dict[part_id[4]]
                    module_type = ''
                    if bp_material == 'CuW':
                        module_type = 'EM'
                    elif bp_material in ['Ti', 'CF']:
                        module_type = 'HAD'
                    kind_of_part = f'{bp_material}/Kapton {part_type} {resolution} {geometry}'  

                elif part_type == 'Sensor':
                    '''
                    As soon as sensor id is updated to 6-digit, please comment out the following and uncomment the above. 
                    '''
                    ## 1) convension v1
                    ## 320-ST-TTT-NNNNNN
                    ### T-TTT: [resolution]-[sen_thickness][geometry][sensor structure]
                    resolution = resolution_dict[part_id[1]]
                    sen_thickness = thickness_dict[part_id[2]]
                    geometry = geometry_dict[part_id[3]]
                    # sen_structure = sen_structure_dict[part_id[4:6]]
                    kind_of_part = f'{sen_thickness}um Si {part_type} {resolution} {geometry}'  


                else:
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

        else:
            kind_of_part = ''
        return kind_of_part
    except Exception as e:
        raise

def format_datetime(input_date, input_time):
    local_timezone = pytz.timezone(str(tzlocal.get_localzone()))
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

