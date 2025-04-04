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



async def fetch_test_data(conn, date_start, date_end):
    # Retrieve the first row of chip and channel arrays
    query = f"""
        SELECT DISTINCT ON (module_name)
        chip, channel, adc_mean, adc_stdd, channeltype, module_name, module_no, date_test, time_test
        FROM module_pedestal_test 
        WHERE date_test BETWEEN '{date_start}' AND '{date_end}'
        ORDER BY module_name, date_test DESC, time_test DESC
    """
    rows = await conn.fetch(query)
    
    if rows is None:
        raise ValueError("No data found in pedestal_table.")

    for row in rows:
        chip = row['chip']
        channel = row['channel']
        channel_type = row['channeltype']
        adc_mean = row['adc_mean']
        adc_stdd = row['adc_stdd']
        date_test = row['date_test']
        time_test = row['time_test']
        module_name = row['module_name']
        module_num = row['module_no']

    # Check if the lengths match
    if len(chip) != len(channel):
        raise ValueError("chip and channel arrays must have the same length.")

    # Create test_data as list of [chip, channel, channel_type, adc_mean, adc_stdd] pairs
    test_data = np.array(list(zip(chip, channel, channel_type, adc_mean, adc_stdd)))## DO NOT CHANGE THE ORDER!!!!!
    
    return module_name, module_num, test_data

def remap_channels(data):
    updated_data = data.copy()
    for i in range(len(updated_data)):
        channel = updated_data[i, 1]
        channel_type = updated_data[i, 2]
        
        if channel_type == 1:
            updated_data[i, 1] += 80
        elif channel_type == 100:
            updated_data[i, 1] += 90
    
    ## drop channel_type column
    updated_data = np.delete(updated_data, index=2, axis=1)

    return updated_data## [:, 3]

def check_duplicate_combo(data):
    skim_data = data[:, :2]## takes only chip and channel
    unq, count = np.unique(skim_data, axis=0, return_counts=True)
    duplicates = unq[count > 1]

    if duplicates.size == 0:
        print('No duplicates are found')
        return True
    else:
        print(f'the followings are duplicated pairs of [chip, channel]\n{duplicates}')
        return False

def convert_nparray_to_dict(data):
    return [
        {
            'channel': int(row[0]),
            'adc_mean': float(row[1]),
            'adc_stdd': float(row[2]),
            'frac_unc': float(row[3]),
            # 'flags': int(row[4]),
        }
        for row in data
    ]

def fill_data_element(template, row):
    data = copy.deepcopy(template)
    data.find('CHANNEL').text = str(int(row[0]))
    data.find('ADC_MEAN').text = str(row[1])
    data.find('ADC_STDD').text = str(row[2])
    data.find('FRAC_UNC').text = str(row[3])
    # data.find('FLAGS').text = str(int(row[4]))
    return data


def duplicate_xml_children(xml_temp_path, roc_test_data, output_path):
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
    
    # params = {'module_name': '320MLF3W2CM0115', 'module_num': 33, 'conn': conn}
    # roc_1, roc_2, roc_3 = await find_rocID(**params)

    # roc_test_data_map = {'ROC_1_dummy': test_data}
    # duplicate_xml_children(xml_temp_path, roc_test_data_map, output_path)
    date_start, date_end = '2025-02-14', '2025-02-17'

    try:
        module_name, module_num, data = await fetch_test_data(conn, date_start, date_end)
        print(module_name)
        # params = {'module_name': module_name, 'module_num': module_num, 'conn': conn}
        # test_data = remap_channels(data)#[:, 3], chip, channel, adc_mean, adc_stdd

        # print(f'module -- {module_name}')
        # if check_duplicate_combo(test_data):## if no duplicates, then return True
        #     rocs = await find_rocID(**params)
        #     mapped_test_data = {}
        #     for i, roc in enumerate(rocs):
        #         if np.any(test_data[:, 0] == i):
        #             mapped_test_data[roc] = test_data[test_data[:, 0] == i, 3]

        #     duplicate_xml_children(xml_temp_path, test_data, output_dir)
        
    finally:
        await conn.close()

# Run if this is the main script
if __name__ == "__main__":
    asyncio.run(main())
