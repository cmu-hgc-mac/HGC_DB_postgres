import requests, json, yaml, os, argparse, datetime, sys
import pwinput, asyncio, asyncpg, base64, traceback
from cryptography.fernet import Fernet
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

loc = 'dbase_info'
conn_yaml_file = os.path.join(loc, 'conn.yaml')
conn_info = yaml.safe_load(open(conn_yaml_file, 'r'))
kop_yaml = yaml.safe_load(open(os.path.join('export_data', 'resource.yaml'), 'r')).get('kind_of_part')
inst_code  = conn_info.get('institution_abbr')
# source_db_cern = conn_info.get('cern_db')
db_source_dict = {'dev_db': {'dbname':'INT2R', 'url': 'hgcapi'} , 'prod_db': {'dbname':'CMSR', 'url': 'hgcapi-cmsr'}}

db_params = {
    'database': conn_info.get('dbname'),
    'user': 'editor',
    'host': conn_info.get('db_hostname'),
    'port': conn_info.get('port'),
}

def str2bool(boolstr):
    dictstr = {'True': True, 'False': False}
    return dictstr[boolstr]

partTrans = {'bp' :{'apikey':'baseplates', 'dbtabname': 'bp_inspect', 'db_col': 'bp_name', 'qc_cols': {'grade': 'grade' ,'thickness': 'thickness','comments': 'comment', 'flatness':'flatness', 'weight': 'weight'}},
             'sen':{'apikey':'sensors', 'dbtabname': 'sensor', 'db_col': 'sen_name', 'qc_cols': {'grade': 'grade' ,'thickness': 'thickness','comments': 'comment'}},
             'hxb':{'apikey':'pcbs', 'dbtabname': 'hxb_inspect', 'db_col': 'hxb_name', 'qc_cols': {'grade': 'grade' ,'thickness': 'thickness','comments': 'comment', 'flatness':'flatness', 'weight': 'weight'}},
             'pml':{'apikey':'protomodules', 'dbtabname': 'proto_inspect', 'db_col': 'proto_name' ,'qc_cols':  {'prto_grade': 'grade', 'prto_thkns_mm': 'avg_thickness', "prto_thkns_mm": 'max_thickness', 'prto_fltns_mm': 'flatness', "snsr_x_offst": 'x_offset_mu', "snsr_y_offst": 'y_offset_mu',"snsr_ang_offst": 'ang_offset_deg'}},
             'ml' :{'apikey':'modules', 'dbtabname': 'module_inspect', 'db_col': 'module_name', 'qc_cols':  {'mod_grade': 'grade', 'mod_ave_thkns_mm': 'avg_thickness', "mod_max_thkns_mm": 'max_thickness', 'mod_fltns_mm': 'flatness', "pcb_plcment_x_offset": 'x_offset_mu', "pcb_plcment_y_offset": 'y_offset_mu',"pcb_plcment_ang_offset": 'ang_offset_deg'}},
            }

partTransInit = {'bp': {'apikey':'baseplates', 'dbtabname': 'baseplate', 'db_cols': {'serial_number': 'bp_name', 'kind': 'kind', 'comment_description': 'comment'}},
                'sen': {'apikey':'sensors',    'dbtabname': 'sensor',    'db_cols': {'serial_number': 'sen_name', 'kind': 'kind', 'comment_description': 'comment'}},
                'hxb': {'apikey':'pcbs',       'dbtabname': 'hexaboard', 'db_cols': {'serial_number': 'hxb_name', 'kind': 'kind', 'comment_description': 'comment'}},
            }

def get_query_write(table_name, column_names, check_conflict_col = None, db_upload_data = None):
    pre_query = f""" INSERT INTO {table_name} ({', '.join(column_names)}) SELECT """
    data_placeholder = ', '.join([f'${i+1}' for i in range(len(column_names))])
    query = f""" {pre_query} {f'{data_placeholder}'}  """
    if check_conflict_col is not None:
        query += f" WHERE NOT EXISTS ( SELECT 1 FROM {table_name} WHERE {check_conflict_col} = '{db_upload_data[check_conflict_col]}' AND kind IS NOT NULL); "
    return query


def get_query_update(table_name, column_names, check_conflict_col = None, db_upload_data = None):
    update_columns = ', '.join([f"{column} = ${i+1}" for i, column in enumerate(column_names)])
    query = f""" UPDATE {table_name} SET {update_columns} WHERE {check_conflict_col} = '{db_upload_data[check_conflict_col]}' AND kind IS NULL;"""
    return query
# def check_exists_query(table_name, column_names):
#     pre_query = f"""SELECT EXISTS ( SELECT 1 FROM {table_name} WHERE """ 
#     data_placeholder = [f'{col_name} = ${n+1}' for n, col_name in enumerate(column_names)]
#     query = f'{pre_query} {" AND ".join(data_placeholder)} );'
#     return query

async def write_to_db(pool, db_upload_data, partType = None, check_conflict_col=None):
    table_name = partTransInit[partType]["dbtabname"]
    async with pool.acquire() as conn:
        query = get_query_write(table_name, db_upload_data.keys(), check_conflict_col=check_conflict_col, db_upload_data=db_upload_data)
        await conn.execute(query, *db_upload_data.values())
        query = get_query_update(table_name, db_upload_data.keys(), check_conflict_col=check_conflict_col, db_upload_data=db_upload_data)
        await conn.execute(query, *db_upload_data.values())

def get_url(partID = None, macID = None, partType = None, cern_db_url = 'hgcapi-cmsr'):
    if partID is not None:
        return f'https://{cern_db_url}.web.cern.ch/mac/part/{partID}/full'
    elif partType is not None:
        if macID is not None:
            return f'https://{cern_db_url}.web.cern.ch/mac/parts/types/{partTrans[partType.lower()]["apikey"]}?page=0&limit=500&location={macID}'
        return f'https://{cern_db_url}.web.cern.ch/mac/parts/types/{partTrans[partType.lower()]["apikey"]}?page=0&limit=500'
    return

def read_from_cern_db(partID = None, macID = None, partType = None , cern_db_url = 'hgcapi-cmsr'):
    headers = {'Accept': 'application/json'}
    response = requests.get(get_url(partID = partID, macID = macID, partType = partType, cern_db_url = cern_db_url), headers=headers)
    if response.status_code == 200:
        data = response.json() ; 
#         print(json.dumps(data, indent=2))
        return data
    elif response.status_code == 500:
        print(f'Internal Server ERROR for {cern_db_url.upper()}. Try again later.')
    else:
        if partType:
            print(f'ERROR in reading from {cern_db_url.upper()} for partType : {partType} :: {response.status_code}')
        if partID:
            print(f'ERROR in reading from {cern_db_url.upper()} for partID : {partID} :: {response.status_code}')
        return None

def form(data):
    if type(data) is str:
        if data.lstrip('-').replace('.',"").isdigit():
            return float(data)
        elif (data.lower() == 'none' or data.lower() == 'null' or data == ''):
            return None
    return data

def get_part_type(partName, partType):
    return_dict = {}
    if partType == 'bp':
        return_dict.update({'geometry' : kop_yaml['geometry'][partName[5]]})
        return_dict.update({'resolution' : kop_yaml['resolution'][partName[6]]})
        return_dict.update({'bp_material' : kop_yaml['material'][partName[7]]})
    elif partType == 'hxb':
        return_dict.update({'resolution': kop_yaml['resolution'][partName[4]]})  
        return_dict.update({'geometry': kop_yaml['geometry'][partName[5]]})  
        return_dict.update({'roc_version': kop_yaml['roc_version'][partName[7]]})  
    elif partType == 'sen':
        return_dict.update({'resolution': kop_yaml['sensor'][partName[0]][1]})  
        return_dict.update({'geometry': kop_yaml['sensor_geometry'][partName[-1]]})  
        return_dict.update({'thickness': int(kop_yaml['sensor'][partName[0]][0])})  
    return return_dict

def get_dict_for_db_upload(data_full, partType):
    try:
        db_dict = {partTransInit[partType]["db_cols"][k]: data_full[k] for k in (partTransInit[partType]["db_cols"]).keys()}
        db_dict.update(get_part_type(data_full['serial_number'], partType))
        return db_dict
    except Exception as e:
        traceback.print_exc()
        print(f"ERROR in acquiring data from API output for {data_full['serial_number']}", e)
        # print(json.dumps(data_full, indent=2))
        # print('*'*100)
        return None

async def main():
    parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
    parser.add_argument('-p', '--password', default=None, required=False, help="Password to access database.")
    # parser.add_argument('-pid', '--partID', default=None, required=False, help="Part ID to query from HGC API.")
    parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
    parser.add_argument('-downld', '--download_dev_stat', default='False', required=False, help="Download from dev DBLoader without generate.")
    parser.add_argument('-downlp', '--download_prod_stat', default='True', required=False, help="Download from prod DBLoader without generate.")
    args = parser.parse_args()

    if args.password is None:
        dbpassword = pwinput.pwinput(prompt='Enter superuser password: ', mask='*')
    else:
        if args.encrypt_key is None:
            print("Encryption key not provided. Exiting..."); exit()
        cipher_suite = Fernet((args.encrypt_key).encode())
        dbpassword = cipher_suite.decrypt( base64.urlsafe_b64decode(args.password)).decode() ## Decode base64 to get encrypted string and then decrypt
        db_params.update({'password': dbpassword})

    if len(inst_code) == 0:
        print("Check institution abbreviation in conn.py"); exit()

    db_list = []
    dev_bool = str2bool(args.download_dev_stat) 
    prod_bool = str2bool(args.download_prod_stat)
    if dev_bool:
        db_list.append('dev_db')
    if prod_bool or (not dev_bool and not prod_bool):
        db_list.append('prod_db')

    for source_db_cern in db_list:
        cern_db_url = db_source_dict[source_db_cern]['url']
        pool = await asyncpg.create_pool(**db_params)
        for pt in ['bp','hxb','sen']:  #, 'pml', 'ml']:
            print(f'Reading {partTrans[pt]["apikey"]} from {cern_db_url.upper()} ...' )
            parts = (read_from_cern_db(macID = inst_code.upper(), partType = pt, cern_db_url = cern_db_url))
            if parts:
                for p in parts['parts']:
                    try:
                        db_dict = get_dict_for_db_upload(p, partType = pt)
                        if db_dict is not None:
                            try:
                                await write_to_db(pool, db_dict, partType = pt, check_conflict_col = partTransInit[pt]['db_cols']['serial_number'])
                            except Exception as e:
                                print(f"ERROR for single part upload for {p['serial_number']}", e)
                                traceback.print_exc()
                                print('Dictionary:', (db_dict))
                    except:
                        traceback.print_exc()
                print(f'Writing {partTrans[pt]["apikey"]} to postgres from {cern_db_url.upper()} complete.')
                print('-'*40); print('\n')
    
    async with pool.acquire() as conn:
        try:
            query_v3c = f"""UPDATE hexaboard SET roc_version = 'HGCROCV3c' WHERE comment LIKE '%44%' OR comment LIKE '%4c%'; """
            await conn.execute(query_v3c)
        except:
            print('v3c query failed')
    await pool.close()
    print('Refresh postgres tables')

asyncio.run(main())


