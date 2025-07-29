import requests, time, os
from datetime import datetime, timedelta
from src import get_kind_of_part
    

def get_api_data(search_id, db_type):
    if db_type == 'cmsr':
        url = f"https://hgcapi-cmsr.web.cern.ch/mac/part/{search_id}/full"
    elif db_type == 'int2r':
        url = f"https://hgcapi.web.cern.ch/mac/part/{search_id}/full"

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