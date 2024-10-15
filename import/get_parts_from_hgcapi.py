import requests, json, yaml, os, argparse
import pwinput

parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
parser.add_argument('-p', '--password', default=None, required=False, help="Password to access database.")
args = parser.parse_args()

dbpassword = str(args.password).replace(" ", "")
if dbpassword is None:
    dbpassword = pwinput.pwinput(prompt='Enter superuser password: ', mask='*')

loc = 'dbase_info'
conn_yaml_file = os.path.join(loc, 'conn.yaml')
conn_info = yaml.safe_load(open(conn_yaml_file, 'r'))
inst_code  = conn_info.get('institution_abbr')

db_params = {
    'database': conn_info.get('dbname'),
    'user': 'postgres',
    'password': dbpassword,
    'host': conn_info.get('db_hostname'),
    'port': conn_info.get('port'),
}

def get_url(partID = None, macID = None, partType = None):
    if partID is not None:
        return f'https://hgcapi.web.cern.ch/mac/part/{partID}/full'
    elif partType is not None:
        partTrans = {'bp':'baseplates','sen':'sensors','hxb':'pcbs','pml':'protomodules','ml':'modules'}
        if macID is not None:
            return f'https://hgcapi.web.cern.ch/mac/parts/types/{partTrans[partType.lower()]}?page=0&limit=100&location={macID}'
        return f'https://hgcapi.web.cern.ch/mac/parts/types/{partTrans[partType.lower()]}?page=0&limit=100'
    return

def process_part_id(partID = None, partType = None):
    part_id = ((str(partID).replace("-","")).replace("320","")).replace("_0","")
    pos_list = {'bp': [2, 5, 7], 'sen': [2, 5, 7], 'hxb': [2, 5, 7]}
    parts = [part_id[i:j] for i, j in zip([0] + pos_list[partType], pos_list[partType] + [None])]
    output_string = f'320-{"-".join(parts)}'
    return output_string

def read_from_cern_db(partID = None, macID = None, partType = None ):
    headers = {'Accept': 'application/json'}
    response = requests.get(get_url(partID = partID, macID = macID, partType = partType), headers=headers)
    if response.status_code == 200:
        data = response.json() ; 
        # print(json.dumps(data, indent=2))
        return data
    else:
        print(f'Error: {response.status_code}')

for pt in ['bp','hxb','sen']:
    data_mod = read_from_cern_db(macID = inst_code.upper(), partType = pt)
    temp = data_mod['parts']
    for t in temp:
        print(process_part_id(t["barcode"],pt), t['serial_number'],t['kind'])
        # print(process_part_id(t["barcode"],pt), process_part_id(t['serial_number'],pt),t['kind'])

# print(json.dumps(data_mod['parts'], indent=2))

# data_mod = read_from_cern_db(partID = '320-ML-F2CX-CM-0006')
# print(json.dumps(data_mod['qc']['module_assembly'], indent=2))
# print(json.dumps(data_mod['qc']['module_wirebond'], indent=2)) 
# print(json.dumps(data_mod['children'], indent=2))

# data_proto = read_from_cern_db(partID = '320-PL-F2CX-CM-0006') ## protomodule example
# print(json.dumps(data_proto['children'], indent=2))