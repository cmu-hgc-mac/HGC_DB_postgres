import asyncio
import asyncpg
import numpy as np
import copy
import xml.etree.ElementTree as ET
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from export_data.src import *

async def fetch_test_data(conn):
    # Retrieve the first row of chip and channel arrays
    query = """
        SELECT chip, channel, adc_mean, adc_stdd, channeltype, module_name FROM module_pedestal_test LIMIT 1;
    """
    row = await conn.fetchrow(query)
    
    if row is None:
        raise ValueError("No data found in pedestal_table.")

    chip = row['chip']
    channel = row['channel']
    channel_type = row['channeltype']
    adc_mean = row['adc_mean']
    adc_stdd = row['adc_stdd']
    module_name = row['module_name']

    # Check if the lengths match
    if len(chip) != len(channel):
        raise ValueError("chip and channel arrays must have the same length.")

    # Create test_data as list of [chip, channel, channel_type, adc_mean, adc_stdd] pairs
    test_data = np.array(list(zip(chip, channel, channel_type, adc_mean, adc_stdd)))## DO NOT CHANGE THE ORDER!!!!!
    
    return module_name, test_data

def remap_channels(data):
    updated_data = data.copy()
    for i in range(len(updated_data)):
        channel = updated_data[i, 1]
        channel_type = updated_data[i, 2]
        
        if channel_type == 1:
            updated_data[i, 1] += 80
        elif channel_type == 100:
            updated_data[i, 1] += 90
    return updated_data

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
            'flags': int(row[4]),
        }
        for row in data
    ]

def fill_data_element(template, row):
    data = copy.deepcopy(template)
    data.find('CHANNEL').text = str(int(row[0]))
    data.find('ADC_MEAN').text = str(row[1])
    data.find('ADC_STDD').text = str(row[2])
    data.find('FRAC_UNC').text = str(row[3])
    data.find('FLAGS').text = str(int(row[4]))
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
    
    # Write to file
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    tree.write(output_path, encoding='utf-8', xml_declaration=True)

async def main():
    yaml_file = 'export_data/table_to_xml_var.yaml'  # Path to YAML file
    xml_temp_path = 'export_data/template_examples/testing/module_pedestal_test.xml'
    output_dir = 'export_data/xmls_for_upload/testing'
    module_name = 'dummy_1'
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f'{module_name}.xml')

    
    # conn = await get_conn(dbpassword='hgcal')
    
    test_data = [[ 0,          0,          0,         89.40699768,  6.00158834,],
                [ 0,          1,          0,         91.44507599,  5.80598879,],
                [ 0,          2,          0,         93.568183,   5.11564207,],
                [ 0,          3,          0,         91.98484802,  5.57028532,],
                [ 0,          4,          0,         92.0732650,  3.73036003,]]
    
    roc_test_data_map = {'ROC_1_dummy': test_data}
    duplicate_xml_children(xml_temp_path, roc_test_data_map, output_path)

    # try:
    #     module_name, data = await fetch_test_data(conn)
    #     test_data = remap_channels(data)
    #     roc_test_data_map = {'roc_1': test_data}

    #     print(f'module -- {module_name}')
    #     if check_duplicate_combo(test_data):## if no duplicates, then return True
    #         duplicate_xml_children(xml_temp_path, test_data, output_path)
        
    # finally:
    #     await conn.close()

# Run if this is the main script
if __name__ == "__main__":
    asyncio.run(main())
