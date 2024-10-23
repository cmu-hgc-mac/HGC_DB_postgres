import asyncio
import asyncpg
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
from lxml import etree
import yaml
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..')))
import pwinput
from datetime import datetime

async def get_conn():
    '''
    Does: get connection to database
    Return: connection
    '''

    loc = '../dbase_info/'
    yaml_file = f'{loc}conn.yaml'
    db_params = {
        'database': yaml.safe_load(open(yaml_file, 'r'))['dbname'],
        'user': 'postgres',
        'password': pwinput.pwinput(prompt='Enter superuser password: ', mask='*'),
        'host': yaml.safe_load(open(yaml_file, 'r'))['db_hostname']}   
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
        print(f"XML file updated and saved to: {output_file_path}")
    else:
        print(f"Error: {output_file_path} is a directory, not a file.")

async def get_parts_name(name, table, conn):
    ##  returns part name in a specific table
    ##  i.e., baseplate-> get bp_name
    query = f"SELECT DISTINCT {name} FROM {table};"
    fetched_query = await conn.fetch(query)
    name_list = [record[name] for record in fetched_query]
    return name_list

# def get_xmlrelated_dbase_tables(yaml_file):
#     dbase_tables = set()
#     for entry in yaml_file:
#         dbase_table = entry.get('dbase_table')
#         if dbase_table and dbase_table != 'null':
#             dbase_tables.add(dbase_table)
#     return list(dbase_tables)

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
        current_timestamp = datetime.now()
        for table in table_list:
            query = f"""
            UPDATE {table}
            SET {column_name} = $1
            WHERE {part_name_col} = $2;
            """
            await conn.execute(query, current_timestamp, part_name)
    except Exception as e:
        print(f"Error updating {column_name}: {e}")

def get_kind_of_part(part_name):
    ## part_name can be module_name, hxb_name, proto_name, sen_name, bp_name and so on. 
        
    part_type_dict = {'P': 'ProtoModule', 'M':'Module', 'S': 'Sensor', 'B': 'Baseplate', 'X':'Hexaboard'}
    resolution_dict = {'L': 'LD', 'H': 'HD'}
    geometry_dict = {'F': 'Full', 'T': 'Top', 'B': 'Bottom', 'L':'Left', 'R':'Right', '5': 'Five', 
                     'S': 'Whole', 'M': 'Half-moons'}
    thickness_dict = {'1': '120', '2': '200', '3': '300'}
    material_dict = {'W': 'CuW', 'T': 'Ti', 'C': 'CF', 'P': 'PCB', 'X':''}
    sen_structure_dict = {'XX': 'Whole', 
                          'TP': 'Top-Half-Moon', 
                          'BT': 'Bottom-Half-Moon', 
                          'TL': 'Top-Left Half-Moon',
                          'TR': 'Top-Right Half-Moon',
                          'BL': 'Bottom-Left Half-Moon',
                          'BR': 'Bottom-Right Half-Moon'}
    
    # Extract the information
    if part_name != '':
        part_id = ((str(part_name).replace("-","")).replace("320","")).replace("_0","")## remove "-" 
        part_type = part_type_dict[part_id[0]]
        if part_type == 'Hexaboard':## Fill out here once it's finalized. 
            kind_of_part = ''

        elif part_type == 'Baseplate':
            ## 320-BA-TTT-VB-NNNN
            ### TTT: [geometry][resolution][bp_material]
            kind_of_part = ''
            ## below is updated version (rev.4.0)
            # geometry = geometry_dict[part_id[2]]
            # resolution = resolution_dict[part_id[3]]
            # bp_material = material_dict[part_id[4]]
            # module_type = ''
            # if bp_material == 'CuW':
            #     module_type = 'EM'
            # elif bp_material in ['Ti', 'CF']:
            #     module_type = 'HAD'
            # kind_of_part = f'{module_type} Si {part_type} {resolution} {geometry}'  

        elif part_type == 'Sensor':
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
    