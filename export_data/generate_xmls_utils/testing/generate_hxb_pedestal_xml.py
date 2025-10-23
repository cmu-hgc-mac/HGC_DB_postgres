import asyncio
import asyncpg
import numpy as np
import copy
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
from datetime import datetime
from collections import defaultdict
from tqdm import tqdm
import sys, os, yaml, argparse, json, traceback
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from export_data.src import *
from export_data.define_global_var import LOCATION, INSTITUTION
from export_data.generate_xmls_utils.testing.generate_module_pedestal_xml import find_toa_vref, get_roc_name
RED = '\033[91m'; RESET = '\033[0m'

conn_yaml_file = os.path.join(loc, 'conn.yaml')
config_data  = yaml.safe_load(open(conn_yaml_file, 'r'))
statusdict_test_upload = config_data.get('statusdict_test_upload', None)
if statusdict_test_upload:
    statusdict_select = tuple([k for d in statusdict_test_upload for k, v in d.items() if v])
    statusdict_select = [f"'{s}'" for s in statusdict_select]
    statusdict_select = f"({', '.join(statusdict_select)})" if statusdict_select else None
else:
    statusdict_select = None # f"('Untaped')"

roc_idxMap_yaml = 'export_data/roc_idxMap.yaml'
resource_yaml = 'export_data/resource.yaml'
with open(roc_idxMap_yaml, 'r') as file:
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
               m.cell,
               m.adc_mean, 
               m.adc_stdd, 
               m.channeltype, 
               m.date_test, 
               m.time_test,
               m.inspector,
               m.temp_c,
               m.rel_hum,
               m.status_desc,
               m.comment,
               m.pedestal_config_json,
               m.list_dead_cells,
               m.list_noisy_cells, 
               m.inverse_sqrt_n,
               h.roc_name, 
               h.roc_index
        FROM hxb_pedestal_test m
        LEFT JOIN hexaboard h ON m.hxb_name = h.hxb_name
        WHERE m.hxb_name = ANY($1)
        """  # OR m.date_test BETWEEN '{date_start}' AND '{date_end}'
        if statusdict_select:
            query += f" AND status_desc IN {statusdict_select}"
        rows = await conn.fetch(query, partsnamelist)
    else:
        query = f"""
            SELECT m.hxb_name,
                m.hxb_no, 
                m.chip, 
                m.channel, 
                m.cell,
                m.adc_mean, 
                m.adc_stdd, 
                m.channeltype, 
                m.date_test, 
                m.time_test,
                m.inspector,
                m.temp_c,
                m.rel_hum,
                m.status_desc,
                m.comment,
                m.pedestal_config_json,
                m.list_dead_cells,
                m.list_noisy_cells,
                m.inverse_sqrt_n,
                h.roc_name, 
                h.roc_index
            FROM hxb_pedestal_test m
            LEFT JOIN hexaboard h ON m.hxb_name = h.hxb_name
            WHERE m.date_test BETWEEN '{date_start}' AND '{date_end}' 
        """
        if statusdict_select:
            query += f" AND status_desc IN {statusdict_select}"
        rows = await conn.fetch(query)

    if rows is None:
        raise ValueError("No data found in pedestal_table.")

    test_data = {}
    for row in rows:
        if row['roc_name'] is None:
            hxb_data = read_from_cern_db(partID = row['hxb_name'], cern_db_url = 'hgcapi-cmsr')
            hgcroc_children = [{"serial_number": child["serial_number"], "attribute": child["attribute"]} for child in hxb_data["children"] if "HGCROC" in child["kind"]]
            hgcroc_children_sorted = sorted(hgcroc_children, key=lambda x: x["attribute"])
            row['roc_name'], row['roc_index'] = [roc['serial_number'] for roc in hgcroc_children_sorted], [roc['attribute'] for roc in hgcroc_children_sorted]

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
                'cell': row['cell'],
                'chip': _chip,
                'channel': channel,
                'channeltype': channeltype,
                'adc_mean': row['adc_mean'],
                'adc_stdd': row['adc_stdd'],
                'roc_name': row['roc_name'],
                'roc_index': row['roc_index'],
                'comment' : row['comment'],
                'inverse_sqrt_n': row['inverse_sqrt_n'],
                'status_desc': row["status_desc"],
                'inspector': row['inspector'],  ###### for env
                'rel_hum': row['rel_hum'] if row['rel_hum'] is not None else 999,
                'temp_c': row['temp_c'] if row['temp_c'] is not None else 999,
                'list_dead_cells': row['list_dead_cells'],
                'list_noisy_cells': row['list_noisy_cells'],
                'pedestal_config_json': row['pedestal_config_json'], ## if row['pedestal_config_json'] is not None else "N/A", #### for config
            }
    return test_data


async def generate_hxb_pedestal_xml(test_data, run_begin_timestamp, output_path, template_path_test, template_path_env = None, template_path_config = None, lxplus_username = None):
    chips = test_data["chip"]   # Prepare chip-to-ROC mapping
    channels = test_data["channel"]
    adc_means = test_data["adc_mean"]
    adc_stdds = test_data["adc_stdd"]

    chip_dead_channels = {chip: [] for chip in set(chips)} 
    for c in test_data['list_dead_cells']:      ### fill this dict with `cell` as keys
        chip_dead_channels[test_data['chip'][test_data['cell'].index(c)]].append(test_data['channel'][test_data['cell'].index(c)])
    
    chip_noisy_channels = {chip: [] for chip in set(chips)} 
    for c in test_data['list_noisy_cells']:      ### fill this dict with `cell` as keys
        chip_noisy_channels[test_data['chip'][test_data['cell'].index(c)]].append(test_data['channel'][test_data['cell'].index(c)])

    roc_grouped_data = defaultdict(list)  # Group data by ROC
    for i, chip in enumerate(chips):
        roc_grouped_data[chip].append({
            "channel": channels[i],
            "adc_mean": adc_means[i],
            "adc_stdd": adc_stdds[i] })
    
    chip_to_roc_name, json_key_to_roc_name = get_roc_name(module_name = test_data['hxb_name'], roc_name = test_data['roc_name'], roc_index = test_data['roc_index'])

    for chip in set(chips): ## replace chip number with roc name
        roc = chip_to_roc_name[chip]
        roc_grouped_data[roc] = roc_grouped_data.pop(chip)
        chip_dead_channels[roc] = chip_dead_channels.pop(chip)  
        chip_noisy_channels[roc] = chip_noisy_channels.pop(chip)

    if test_data['pedestal_config_json']:
        pedestal_config_json_full = json.loads(f'''{test_data['pedestal_config_json']}''')
        
    chip_config = {}    
    for key, roc in json_key_to_roc_name.items():   
        chip_config[roc] = pedestal_config_json_full[key]["sc"] if test_data['pedestal_config_json'] else None   
    
    os.makedirs(output_path, exist_ok=True)
    timestamp_formatted = str(run_begin_timestamp).replace(":","").split('.')[0]
    file_path_test = os.path.join(output_path, f"{test_data['hxb_name']}_{timestamp_formatted}_pedestal.xml")
    file_path_env = os.path.join(output_path, f"{test_data['hxb_name']}_{timestamp_formatted}_pedestal_cond.xml")
    file_path_config = os.path.join(output_path, f"{test_data['hxb_name']}_{timestamp_formatted}_pedestal_config.xml")
    outfile_names = {'test': file_path_test, 'env': file_path_env, 'config': file_path_config}
    xml_types = {'test': template_path_test, 'env': template_path_env, 'config': template_path_config}

    test_timestamp = test_data['test_timestamp']
    test_timestamp = datetime.datetime.strptime(test_timestamp, "%Y-%m-%d %H:%M:%S.%f") if "." in test_timestamp else datetime.datetime.strptime(test_timestamp, "%Y-%m-%d %H:%M:%S")

    # === Fill in <RUN> metadata ===
    for xml_type in list(xml_types.keys()): ####### Common for all three XML types
        if xml_type == 'config' and test_data['pedestal_config_json'] == None: 
            continue
        tree = ET.parse(xml_types[xml_type])
        root = tree.getroot()
        run_info = root.find("HEADER/RUN")
        if run_info is not None:
            run_info.find("RUN_TYPE").text = "MAC hexaboard pedestal and noise" if not test_data['status_desc'] else f"MAC hexaboard pedestal and noise - {test_data['status_desc']}"
            run_info.find("RUN_NUMBER").text = get_run_num(LOCATION, test_timestamp)
            run_info.find("INITIATED_BY_USER").text = lxplus_username if lxplus_username is not None else "None"
            run_info.find("RUN_BEGIN_TIMESTAMP").text = format_datetime(run_begin_timestamp.split('T')[0], run_begin_timestamp.split('T')[1])
            run_info.find("RUN_END_TIMESTAMP").text = format_datetime(run_begin_timestamp.split('T')[0], run_begin_timestamp.split('T')[1])
            run_info.find("LOCATION").text = LOCATION
            run_info.find("COMMENT_DESCRIPTION").text = f"MAC pedestal and noise data for {test_data['hxb_name']}"

        data_set = root.find("DATA_SET")  # Get and remove the original <DATA_SET> template block in all three XML types
        root.remove(data_set)

        # Create one <DATA_SET> per ROC and add all <DATA> blocks under it
        for roc, entries in roc_grouped_data.items():
            data_set = copy.deepcopy(data_set)       # Deep copy the template DATA_SET element for the test
            data_set.find("COMMENT_DESCRIPTION").text = "NULL" if not test_data["comment"] else test_data["comment"].replace("\n","; ")  # Insert the comments from testing

            serial_elem = data_set.find("PART/SERIAL_NUMBER") # Set the correct SERIAL_NUMBER inside PART
            if serial_elem is not None:
                serial_elem.text = roc
            kindofpart = data_set.find("PART/KIND_OF_PART")
            if kindofpart is not None:
                kindofpart.text = f"{test_data['hxb_name'][4]}D HGCROC"

            for data_elem in data_set.findall("DATA"): # Remove placeholder DATA blocks (direct children of DATA_SET)
                data_set.remove(data_elem)

            if xml_type == 'test':
                for entry in entries:  # Add actual DATA blocks under DATA_SET (NOT under PART)
                    data = ET.Element("DATA")
                    ET.SubElement(data, "CHANNEL").text = str(entry["channel"])
                    ET.SubElement(data, "MEAN").text = str(entry["adc_mean"])
                    ET.SubElement(data, "STDEV").text = str(entry["adc_stdd"])
                    flag = "0"      if entry["adc_mean"] == 0  else ""
                    flag = flag+"D" if entry["channel"] in chip_dead_channels[roc]  else flag
                    flag = flag+"N" if entry["channel"] in chip_noisy_channels[roc] else flag
                    if flag:
                        ET.SubElement(data, "FLAGS").text = flag
                    if test_data["inverse_sqrt_n"]:
                        ET.SubElement(data, "FRAC_UNC").text = str(round(test_data["inverse_sqrt_n"],14)) ### 1/sqrt(N) where N=10032
                    # ET.SubElement(data, "FLAGS").text = "0"
                    data_set.append(data)  # <== append directly under DATA_SET
            elif xml_type == 'env':
                data = ET.Element("DATA")  # Add actual DATA blocks under DATA_SET (NOT under PART)
                ET.SubElement(data, "TEMP_C").text = test_data['temp_c']
                ET.SubElement(data, "HUMIDITY_REL").text = test_data['rel_hum']
                ET.SubElement(data, "MEASUREMENT_TIME").text = format_datetime(run_begin_timestamp.split('T')[0], run_begin_timestamp.split('T')[1])
                data_set.append(data)  # <== append directly under DATA_SET
            elif xml_type == 'config':
                data = ET.Element("DATA")
                toa_vref = find_toa_vref(chip_config[roc])
                ET.SubElement(data, "PURPOSE").text = f"Tuned for TOA_vref={toa_vref[0]}" if toa_vref else "TOA_vref N/A"
                ET.SubElement(data, "CONFIG_JSON").text = f'''{chip_config[roc]}'''
                data_set.append(data)  # <== append directly under DATA_SET 
            
            root.append(data_set)  # Append the completed DATA_SET to ROOT for each ROC
        
        # Pretty-print the XML
        rough_string = ET.tostring(root, encoding="utf-8")
        pretty_xml = minidom.parseString(rough_string).toprettyxml(indent="\t")
        pretty_xml = pretty_xml.replace("&quot;", '"')
        pretty_xml = "\n".join(line for line in pretty_xml.split("\n") if line.strip())
        
        # delete the first <ROOT> for formatting
        lines = pretty_xml.splitlines()
        if lines[1].strip().startswith("<ROOT"):
            lines.pop(1)
        pretty_xml = "\n".join(lines)
        fixed_declaration = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n<ROOT xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">\n'
        
        if pretty_xml.startswith('<?xml'):
            pretty_xml = fixed_declaration + '\n'.join(pretty_xml.split('\n')[1:])
        
        with open(outfile_names[xml_type], "w", encoding="utf-8") as f:  ### Write to output file
            f.write(pretty_xml)

    return outfile_names


async def main(dbpassword, output_dir, date_start, date_end, encryption_key=None, partsnamelist=None, lxplus_username = None):
    yaml_file = 'export_data/table_to_xml_var.yaml'  # Path to YAML file
    temp_dir = 'export_data/template_examples/testing/module_pedestal_test.xml'
    temp_dir_config = 'export_data/template_examples/testing/module_pedestal_config.xml'
    temp_dir_env = 'export_data/template_examples/testing/qc_env_cond.xml'
    output_dir = 'export_data/xmls_for_upload/testing/pedestal'

    conn = await get_conn(dbpassword, encryption_key)

    try:
        test_data = await fetch_test_data(conn, date_start, date_end, partsnamelist)
        for run_begin_timestamp in tqdm(list(test_data.keys())):
            try:
                float(test_data[run_begin_timestamp]['rel_hum'])
                float(test_data[run_begin_timestamp]['temp_c'])
            except:
                print(f"{test_data[run_begin_timestamp]['hxb_name']}: {run_begin_timestamp} {RED}Cannot upload any test data when humidity or temperature is null.{RESET}") 
                continue
            output_file = await generate_hxb_pedestal_xml(test_data[run_begin_timestamp], run_begin_timestamp, output_dir, template_path_test=temp_dir, template_path_env = temp_dir_env, template_path_config=temp_dir_config, lxplus_username=lxplus_username)
    except Exception as e:
        print(f"{RED}An error occurred: {traceback.print_exc()}.{RESET}")
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
