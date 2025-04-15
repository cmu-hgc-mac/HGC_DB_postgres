import asyncio
import asyncpg
import numpy as np
import copy
import xml.etree.ElementTree as ET
import sys, os, yaml
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from export_data.src import *

chip_idxMap_yaml = 'export_data/chip_idxMap.yaml'
resource_yaml = 'export_data/resource.yaml'
with open(chip_idxMap_yaml, 'r') as file:
    chip_idx_yaml = yaml.safe_load(file)

with open(resource_yaml, 'r') as file:
    yaml_content = yaml.safe_load(file)
    kind_of_part_yaml = yaml_content['kind_of_part']
    
async def find_rocID(module_name, module_num, conn, yaml_content=yaml_content):
    '''
    1. Get a geometry of a module (=hxb)
    2. get a corresponding chip number and name from yaml using the geometry
    3. get a ROC-ID from hxb table 
    '''
    part_id = (module_name[0:3].replace('320', '') + module_name[3:]).replace('-', '')
    resolution_dict = kind_of_part_yaml['resolution']
    geometry_dict = kind_of_part_yaml['geometry']
    resolution = resolution_dict[part_id[1]]
    geometry = geometry_dict[part_id[2]]
    chip_idx_data = chip_idx_yaml[f'{resolution} {geometry}']
    chip_names = [item['name'] for item in chip_idx_data] ##i.e., [M1, M2, M3]

    query = f'''
    SELECT roc_name, roc_index FROM hexaboard where module_no = {module_num} 
    '''
    roc_name, roc_index = await conn.fetchrow(query)
    if roc_index == chip_names:
        return roc_name## i.e., ['SU02-0124-001061', 'SU02-0124-001067', 'SU02-0124-001076']
    else:
        print('roc_index unmatched. No ROC was found')
        return False

def remap_channels(channel, channel_type):
    for i in range(len(channel)):
        if channel_type[i] == 1:
            channel[i] += 80
        elif channel_type[i] == 100:
            channel[i] += 90
    return channel, channel_type

def check_duplicate_combo(chip, channel):
    chip_channel = np.column_stack((chip, channel))
    unq, count = np.unique(chip_channel, axis=0, return_counts=True)
    duplicates = unq[count > 1]

    if duplicates.size == 0:
        # print('No duplicates in chips and channels are found\n')
        return True
    else:
        print(f'the followings are duplicated pairs of [chip, channel]\n{duplicates}')
        return False
    
async def fetch_test_data(conn, date_start, date_end):
    # Retrieve the first row of chip and channel arrays
    query = f"""
        SELECT m.module_name,
               m.chip, 
               m.channel, 
               m.adc_mean, 
               m.adc_stdd, 
               m.channeltype, 
               m.module_name, 
               m.module_no, 
               m.date_test, 
               m.time_test,
               m.inspector,
               h.roc_name, 
               h.roc_index
        FROM module_pedestal_test m
        LEFT JOIN hexaboard h ON m.module_no = h.module_no
        WHERE m.date_test BETWEEN '{date_start}' AND '{date_end}'
    """
    rows = await conn.fetch(query)
    
    if rows is None:
        raise ValueError("No data found in pedestal_table.")

    test_data = {}
    for row in rows:
        date_test = row['date_test']
        time_test = str(row['time_test']).split('.')[0]
        run_begin_timestamp = f"{date_test}T{time_test}"
    
        if run_begin_timestamp not in test_data:
            _channel = list(row['channel'])
            _channeltype = list(row['channeltype'])
            _chip = list(row['chip'])
            channel, channeltype = remap_channels(_channel, _channeltype)
            check_duplicate_combo(_chip, channel)

            test_data[run_begin_timestamp] = {
                'module_name': row['module_name'],
                'module_no': row['module_no'],
                'inspector': row['inspector'],
                'chip': _chip,
                'channel': channel,
                'channeltype': channeltype,
                'adc_mean': row['adc_mean'],
                'adc_stdd': row['adc_stdd'],
                'roc_name': row['roc_name']
            }
    return test_data

# def retreive_roc_name(module_name, chips):
#     '''
#     To-Do: retreive ROC name
#     chip -> chip_name (M1, M2, etc) from spreadsheet -> ROC_NAME from hxb_table
#     '''
#     resolution_dict = kind_of_part_yaml['resolution']
#     geometry_dict = kind_of_part_yaml['geometry']
#     if module_name != '' or module_name != 'NoneType':
#         module_name = (module_name[0:3].replace('320', '') + module_name[3:]).replace('-', '')
#         _resolution = resolution_dict[module_name[1]]
#         _geometry = geometry_dict[module_name[2]]
#         geometry = f'{_resolution} {_geometry}' ## LD Full, LD Right, etc...

#     unique_chip = list(set(chips))## 0, 1, 2
#     roc_chip_mapping = {}
#     for i in range(len(unique_chip)):
#         _chip_name = next((entry["name"] for entry in chip_idx_yaml[geometry] if entry["chip"] == unique_chip[i]), None)
#         roc_chip_mapping = {_chip_name: unique_chip[i]}

#     print(roc_chip_mapping)



def duplicate_xml_children(xml_temp_path, roc_test_data, output_path):

    '''
    #######################################
    Major change on this function. See chatgpt!!!
    #######################################
    '''
    tree = ET.parse(xml_temp_path)
    root = tree.getroot()

    # Get all <DATA_SET> and use the first one as a template
    original_data_sets = root.findall('DATA_SET')
    if not original_data_sets:
        raise ValueError("No <DATA_SET> found in template.")
    
    # Remove all existing <DATA_SET> blocks from the template
    for ds in original_data_sets:
        root.remove(ds)

    template_data_set = copy.deepcopy(original_data_sets[0])
    data_template = template_data_set.find('DATA')

    # For each ROC, create a new <DATA_SET> block
    for roc_name, test_data in roc_test_data.items():
        data_set = copy.deepcopy(template_data_set)

        # Set ROC name
        serial_elem = data_set.find('PART').find('SERIAL_NUMBER')
        serial_elem.text = roc_name
        
        # Re-locate the <DATA> inside this new data_set
        data_template = data_set.find('DATA')
        if data_template is not None:
            data_set.remove(data_template)

        # Add one <DATA> per row
        for row in test_data:
            data_elem = fill_data_element(data_template, row)
            data_set.append(data_elem)

        # Append this filled <DATA_SET> back to root
        root.append(data_set)
    
    ## customize the file name
    ####################
    # Write to file
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    tree.write(output_path, encoding='utf-8', xml_declaration=True)


async def main():
    yaml_file = 'export_data/table_to_xml_var.yaml'  # Path to YAML file
    xml_temp_path = 'export_data/template_examples/testing/module_pedestal_test.xml'
    output_dir = 'export_data/xmls_for_upload/testing'

    conn = await get_conn(dbpassword='hgcal')
    date_start, date_end = '2025-04-04', '2025-04-04'

    try:
        test_data = await fetch_test_data(conn, date_start, date_end)
        print(list(test_data.items())[0])
        #     duplicate_xml_children(xml_temp_path, test_data, output_dir)
        
    finally:
        await conn.close()

# Run if this is the main script
if __name__ == "__main__":
    asyncio.run(main())
