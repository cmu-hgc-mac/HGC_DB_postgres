import asyncio
import asyncpg
import numpy as np
import copy
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
from datetime import datetime
from collections import defaultdict
from tqdm import tqdm
import sys, os, yaml, argparse
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from export_data.src import *
from export_data.define_global_var import LOCATION, INSTITUTION

chip_idxMap_yaml = 'export_data/chip_idxMap.yaml'
resource_yaml = 'export_data/resource.yaml'
with open(chip_idxMap_yaml, 'r') as file:
    chip_idx_yaml = yaml.safe_load(file)

with open(resource_yaml, 'r') as file:
    yaml_content = yaml.safe_load(file)
    kind_of_part_yaml = yaml_content['kind_of_part']
    
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
    
async def fetch_test_data(conn, date_start, date_end, partsnamelist=None):
    # Retrieve the first row of chip and channel arrays
    if partsnamelist:
        query = f"""
        SELECT m.hxb_name,
               m.hxb_no, 
               m.chip, 
               m.channel, 
               m.adc_mean, 
               m.adc_stdd, 
               m.channeltype, 
               m.date_test, 
               m.time_test,
               m.inspector,
               m.temp_c,
               m.rel_hum,
               h.roc_name, 
               h.roc_index
        FROM hxb_pedestal_test m
        LEFT JOIN hexaboard h ON m.hxb_no = h.hxb_no
        WHERE m.hxb_name = ANY($1)
        """  # OR m.date_test BETWEEN '{date_start}' AND '{date_end}'
        rows = await conn.fetch(query, partsnamelist)
    else:
        query = f"""
            SELECT m.hxb_name,
                m.hxb_no, 
                m.chip, 
                m.channel, 
                m.adc_mean, 
                m.adc_stdd, 
                m.channeltype, 
                m.date_test, 
                m.time_test,
                m.inspector,
                m.temp_c,
                m.rel_hum,
                h.roc_name, 
                h.roc_index
            FROM hxb_pedestal_test m
            LEFT JOIN hexaboard h ON m.hxb_no = h.hxb_no
            WHERE m.date_test BETWEEN '{date_start}' AND '{date_end}'
        """
        rows = await conn.fetch(query)

    if rows is None:
        raise ValueError("No data found in pedestal_table.")

    test_data, test_data_env = {}, {}
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
                'test_timestamp': f"{row['date_test']} {row['time_test']}",
                'hxb_name': row['hxb_name'],
                'hxb_no': row['hxb_no'],
                'inspector': row['inspector'],
                'chip': _chip,
                'channel': channel,
                'channeltype': channeltype,
                'adc_mean': row['adc_mean'],
                'adc_stdd': row['adc_stdd'],
                'roc_name': row['roc_name']
            }

            test_data_env[run_begin_timestamp] = {
                'test_timestamp': f"{row['date_test']} {row['time_test']}",
                'hxb_name': row['hxb_name'],
                'hxb_no': row['hxb_no'],
                'inspector': row['inspector'],
                'rel_hum': row['rel_hum'],
                'temp_c': row['temp_c'],
                'roc_name': row['roc_name']
            }
    return test_data, test_data_env


async def generate_hxb_pedestal_xml(test_data, run_begin_timestamp, template_path, output_path, template_path_env = None, test_data_env = None, lxplus_username = None):
    tree = ET.parse(template_path)
    root = tree.getroot()
    test_timestamp = test_data['test_timestamp']
    test_timestamp = datetime.datetime.strptime(test_timestamp, "%Y-%m-%d %H:%M:%S.%f")

    # === Fill in <RUN> metadata ===
    run_info = root.find("HEADER/RUN")
    if run_info is not None:
        run_info.find("RUN_TYPE").text = "MAC hexaboard pedestal and noise"
        run_info.find("RUN_NUMBER").text = get_run_num(LOCATION, test_timestamp)
        run_info.find("INITIATED_BY_USER").text = lxplus_username if lxplus_username is not None else "None"
        run_info.find("RUN_BEGIN_TIMESTAMP").text = format_datetime(run_begin_timestamp.split('T')[0], run_begin_timestamp.split('T')[1])
        run_info.find("RUN_END_TIMESTAMP").text = format_datetime(run_begin_timestamp.split('T')[0], run_begin_timestamp.split('T')[1])
        run_info.find("LOCATION").text = LOCATION
        run_info.find("COMMENT_DESCRIPTION").text = f"MAC pedestal and noise data for {test_data['hxb_name']}"
        
      

    # Get and remove the original <DATA_SET> template block
    data_set_template = root.find("DATA_SET")
    root.remove(data_set_template)

    # Prepare chip-to-ROC mapping
    chips = test_data["chip"]
    channels = test_data["channel"]
    adc_means = test_data["adc_mean"]
    adc_stdds = test_data["adc_stdd"]
    roc_names = test_data["roc_name"]

    chip_to_roc = {}
    for idx, chip in enumerate(sorted(set(chips))):
        if idx < len(roc_names):
            chip_to_roc[chip] = roc_names[idx]

    # Group data by ROC
    roc_grouped_data = defaultdict(list)
    for i in range(len(channels)):
        chip = chips[i]
        roc = chip_to_roc.get(chip, "UNKNOWN")
        roc_grouped_data[roc].append({
            "channel": channels[i],
            "adc_mean": adc_means[i],
            "adc_stdd": adc_stdds[i]
        })

    # Create one <DATA_SET> per ROC and add all <DATA> blocks under it
    for roc, entries in roc_grouped_data.items():
        # Deep copy the template DATA_SET element
        data_set = copy.deepcopy(data_set_template)

        # Set the correct SERIAL_NUMBER inside PART
        serial_elem = data_set.find("PART/SERIAL_NUMBER")
        if serial_elem is not None:
            serial_elem.text = roc
        kindofpart = data_set.find("PART/KIND_OF_PART")
        if kindofpart is not None:
            kindofpart.text = f"{test_data['hxb_name'][4]}D HGCROC"

        # Remove placeholder DATA blocks (direct children of DATA_SET)
        for data_elem in data_set.findall("DATA"):
            data_set.remove(data_elem)

        # Add actual DATA blocks under DATA_SET (NOT under PART)
        for entry in entries:
            data = ET.Element("DATA")
            ET.SubElement(data, "CHANNEL").text = str(entry["channel"])
            ET.SubElement(data, "MEAN").text = str(entry["adc_mean"])
            ET.SubElement(data, "STDEV").text = str(entry["adc_stdd"])
            ET.SubElement(data, "FRAC_UNC").text = "0.0"
            ET.SubElement(data, "FLAGS").text = "0"
            data_set.append(data)  # <== append directly under DATA_SET

        # Append the completed DATA_SET to ROOT
        root.append(data_set)

    # Pretty-print the XML
    rough_string = ET.tostring(root, encoding="utf-8")
    pretty_xml = minidom.parseString(rough_string).toprettyxml(indent="\t")
    pretty_xml = "\n".join(line for line in pretty_xml.split("\n") if line.strip())
    
    # delete the first <ROOT> for formatting
    lines = pretty_xml.splitlines()
    if lines[1].strip().startswith("<ROOT"):
        lines.pop(1)
    pretty_xml = "\n".join(lines)
    fixed_declaration = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n<ROOT xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">\n'
    
    if pretty_xml.startswith('<?xml'):
        pretty_xml = fixed_declaration + '\n'.join(pretty_xml.split('\n')[1:])

    ### DO the same for the enviromental conditions data
    tree = ET.parse(template_path_env)
    root = tree.getroot()
    test_timestamp = test_data_env['test_timestamp']
    test_timestamp = datetime.datetime.strptime(test_timestamp, "%Y-%m-%d %H:%M:%S.%f")

    # === Fill in <RUN> metadata ===
    run_info = root.find("HEADER/RUN")
    if run_info is not None:
        run_info.find("RUN_TYPE").text = "MAC hexaboard pedestal and noise"
        run_info.find("RUN_NUMBER").text = get_run_num(LOCATION, test_timestamp)
        run_info.find("INITIATED_BY_USER").text = lxplus_username if lxplus_username is not None else "None"
        run_info.find("RUN_BEGIN_TIMESTAMP").text = format_datetime(run_begin_timestamp.split('T')[0], run_begin_timestamp.split('T')[1])
        run_info.find("RUN_END_TIMESTAMP").text = format_datetime(run_begin_timestamp.split('T')[0], run_begin_timestamp.split('T')[1])
        run_info.find("LOCATION").text = LOCATION
        run_info.find("COMMENT_DESCRIPTION").text = f"MAC pedestal and noise data for {test_data['hxb_name']}"

    # Get and remove the original <DATA_SET> template block
    data_set_template = root.find("DATA_SET")
    root.remove(data_set_template)
    roc_names = test_data_env["roc_name"]

    # Group data by ROC
    roc_grouped_data = defaultdict(list)
    
    # Create one <DATA_SET> per ROC and add all <DATA> blocks under it
    for roc in roc_names:
        # Deep copy the template DATA_SET element
        data_set = copy.deepcopy(data_set_template)

        # Set the correct SERIAL_NUMBER inside PART
        serial_elem = data_set.find("PART/SERIAL_NUMBER")
        if serial_elem is not None:
            serial_elem.text = roc
        
        kindofpart = data_set.find("PART/KIND_OF_PART")
        if kindofpart is not None:
            kindofpart.text = f"{test_data_env['hxb_name'][4]}D HGCROC"

        # Remove placeholder DATA blocks (direct children of DATA_SET)
        for data_elem in data_set.findall("DATA"):
            data_set.remove(data_elem)

        # Add actual DATA blocks under DATA_SET (NOT under PART)
        data = ET.Element("DATA")
        ET.SubElement(data, "TEMP_C").text = test_data_env['temp_c']
        ET.SubElement(data, "HUMIDITY_REL").text = test_data_env['rel_hum']
        ET.SubElement(data, "MEASUREMENT_TIME").text = format_datetime(run_begin_timestamp.split('T')[0], run_begin_timestamp.split('T')[1])
        # ET.SubElement(data, "TEMPSENSOR_ID").text = "0"
        data_set.append(data)  # <== append directly under DATA_SET

        # Append the completed DATA_SET to ROOT
        root.append(data_set)

    # Pretty-print the XML COND
    rough_string = ET.tostring(root, encoding="utf-8")
    pretty_xml_env = minidom.parseString(rough_string).toprettyxml(indent="\t")
    pretty_xml_env = "\n".join(line for line in pretty_xml_env.split("\n") if line.strip())
    
    # delete the first <ROOT> for formatting
    lines = pretty_xml_env.splitlines()
    if lines[1].strip().startswith("<ROOT"):
        lines.pop(1)
    pretty_xml_env = "\n".join(lines)
    fixed_declaration = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n<ROOT xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">\n'
    
    if pretty_xml_env.startswith('<?xml'):
        pretty_xml_env = fixed_declaration + '\n'.join(pretty_xml_env.split('\n')[1:])

    # Write to output file
    os.makedirs(output_path, exist_ok=True)
    temp = str(run_begin_timestamp).replace(":","").split('.')[0]
    file_path = os.path.join(output_path, f"{test_data['hxb_name']}_{temp}_pedestal.xml")
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(pretty_xml)
    file_path_env = os.path.join(output_path, f"{test_data['hxb_name']}_{temp}_pedestal_cond.xml")
    with open(file_path_env, "w", encoding="utf-8") as f:
        f.write(pretty_xml_env)

    return file_path


async def main(dbpassword, output_dir, date_start, date_end, encryption_key=None, partsnamelist=None, lxplus_username = None):
    yaml_file = 'export_data/table_to_xml_var.yaml'  # Path to YAML file
    temp_dir = 'export_data/template_examples/testing/module_pedestal_test.xml'
    temp_dir_env = 'export_data/template_examples/testing/qc_env_cond.xml'
    output_dir = 'export_data/xmls_for_upload/testing/pedestal'

    conn = await get_conn(dbpassword, encryption_key)

    try:
        test_data, test_data_env = await fetch_test_data(conn, date_start, date_end, partsnamelist)
        for run_begin_timestamp in tqdm(list(test_data.keys())):
            output_file     = await generate_hxb_pedestal_xml(test_data[run_begin_timestamp], run_begin_timestamp, temp_dir, output_dir, template_path_env = temp_dir_env, test_data_env = test_data_env[run_begin_timestamp], lxplus_username=lxplus_username)
    finally:
        await conn.close()

# Run if this is the main script
# Run the asyncio program
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

    asyncio.run(main(dbpassword = dbpassword, output_dir = output_dir, encryption_key = encryption_key, date_start=date_start, date_end=date_end, partsnamelist=partsnamelist, lxplus_username = lxplus_username))
