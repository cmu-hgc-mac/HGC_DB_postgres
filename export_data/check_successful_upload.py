import requests, time, os
from datetime import datetime, timedelta
from src import get_kind_of_part

LOG_DIR = Path("export_data/mass_upload_logs")
UPLOAD_STATUS_MAP = {
    "Already Uploaded": True,
    "Success": True,
    "State Timeout": False,
    "Error": False,
    "Copy Failed": False,
    "Processing Exception": False
}
PART_NAME_MAP = {
    'BA': 'bp',
    'XL': 'hxb',
    '_': 'sensor',
    'PL': 'proto',
    'ML': 'module'
}


YAML_MAP = "export_data/resource.yaml"        # local mapping file
with open(YAML_MAP) as f:
    yaml_data = yaml.safe_load(f)

def get_reflected_tables(xml_path: str) -> str:
    """Return mapping key like 'bp_cond' based on part name and path."""
    """xml_path: /afs/cern.ch/user/u/username/hgc_xml_temp/320MLF2W2CM0102_wirebond_upload.xml"""

    table_map = yaml_data["postgres_table_to_xml"]

    ## get prefix
    part_name = xml_path.split('/')[-1].split('_')[0]
    m = re.search(r'(BA|XL|PL|ML|_)', part_name)
    typecode = m.group(1) if m else None
    prefix = PART_NAME_MAP.get(typecode, "")
    if not prefix:
        raise ValueError(f"Cannot determine part type for part={part_name}, path={xml_path}")
    
    suffix_matching = {
        "wirebond": "wirebond",
        "_cure_cond": f"{prefix}_cure_cond",
        "_inspection": f"{prefix}_inspection",
        "_build_upload": f"{prefix}_build",
        "_assembly": f"{prefix}_assembly",
        "_iv_cond": f"{prefix}_iv_cond",
        "_iv": f"{prefix}_iv",
        "_pedestal": f"{prefix}_pedestal"
    }
    for suffix, xml_type in suffix_matching.items():
        if suffix in xml_path:
            tables = table_map[xml_type]
            if not suffix:
                raise ValueError(f"Cannot determine xml type for part={part_name}, path={xml_path}")
            return prefix, part_name, tables ##i.e., module, 320MLF2W2CM0102, [module_pedestal, module_cond]
        
def get_upload_status_csv(csv_path):
    '''
    xml_path,upload_status,upload_state_value,upload_state_path,upload_log_path     
    '''
    csv_output = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            xml_path = row["xml_path"]
            upload_status = row["upload_status"].strip()## Refer to UPLOAD_STATUS_MAP
            prefix, part_name, db_tables_to_be_updated = get_reflected_tables(xml_path)
            csv_output.append((prefix, part_name, upload_status, db_tables_to_be_updated))
            # Skip if unknown status
            if upload_status not in UPLOAD_STATUS_MAP:
                continue
        return csv_output ## [(part_name, upload_status, db_tables_to_be_updated), (...), ...]

def get_api_data(search_id, db_type):
    if db_type == 'cmsr':
        url = f"https://hgcapi.web.cern.ch/mac/part/{search_id}/full"
    elif db_type == 'int2r':
        url = f"https://hgcapi-intg.web.cern.ch/mac/part/{search_id}/full"

    headers = {'Accept': 'application/json'}
    selected_keys = ['kind', 'record_insertion_user', 'record_insertion_time', 'serial_number']

    try:
        response = requests.get(url, headers=headers)
        if  response.status_code==404: 
            return None
        response.raise_for_status()  # Raise an HTTPError for bad responses
        if not response.text.strip():  # Check if the response is empty
            print("Error: API response is empty.")
            return None
        try:
            data = response.json()
        except ValueError:
            print("Error: Response is not in JSON format.")
            print("Raw response:", response.text)
            return None
        return {key: data.get(key, None) for key in selected_keys}
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    
def get_part_id_fromXML(base_dir="export_data/xmls_for_upload", time_limit=90):
    '''
    output: export_data/xmls_for_upload/protomodule/320PLF3W2CM0122_build_upload.xml
    '''
    part_ids = []
    current_time = time.time()
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".xml"):
                file_path = os.path.join(root, file)
                if time_limit is None or (current_time - os.path.getmtime(file_path)) <= time_limit:
                    # xml_files.append(file_path)
                    part_ids.append(file_path.split('/')[-1].split('_')[0])
    
    return part_ids

async def check_upload(db_type):
    '''
    We say, if serial id and kind_of_part match in API match with our xmls, then the data is successfully uploaded. 
    '''
    part_ids = get_part_id_fromXML() ## list of part_ids whose xml are just generated

    for search_id in part_ids:
        print(f'------ checking {search_id} upload ------')

        cern_data = get_api_data(search_id, db_type)
        if cern_data:
            # record_datetime = datetime.strptime(cern_data['record_insertion_time'], '%Y-%m-%d%H:%M:%S.%f')
            kind = cern_data['kind']
            part_id = cern_data['serial_number']
            kind_of_part = await get_kind_of_part(search_id)

            # time_diff = abs(record_datetime - today)
            if kind == kind_of_part:
                if part_id == search_id:
                    # print('Data matched, Upload successful')
                    return True
                else:
                    print('Data unmatched, Upload failed')
        else:
            print("Presponse empty, part didn't upload")