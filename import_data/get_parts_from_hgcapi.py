import requests, json, yaml, os, argparse, datetime, sys
import pwinput, asyncio, asyncpg, base64, traceback
from cryptography.fernet import Fernet
from natsort import natsorted, natsort_keygen
from tqdm import tqdm
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from export_data.src import read_from_cern_db
RED = '\033[91m'; RESET = '\033[0m'

"""
Logic of writing to parts tables:
- Parts can either be inserted from the HGCAPI or by verifying shipment
- Don't insert IF (part exists and kind has been filled)
- Update API details if kind has NOT been filled for that part

For parts verify
- Part created if it doesn't exist
- Date verify updated if value is null

Parts are also created by LabVIEW during assembly
"""

loc = 'dbase_info'
conn_yaml_file = os.path.join(loc, 'conn.yaml')
conn_info = yaml.safe_load(open(conn_yaml_file, 'r'))
kop_yaml = yaml.safe_load(open(os.path.join('export_data', 'resource.yaml'), 'r')).get('kind_of_part')
inst_code  = conn_info.get('institution_abbr')
# source_db_cern = conn_info.get('cern_db')
db_source_dict = {'dev_db': {'dbname':'INT2R', 'url': 'hgcapi-intg'} , 'prod_db': {'dbname':'CMSR', 'url': 'hgcapi'}}
max_cern_db_request = int(conn_info.get('max_cern_db_request', 1000))

db_params = {
    'database': conn_info.get('dbname'),
    'user': 'editor',
    'host': conn_info.get('db_hostname'),
    'port': conn_info.get('port'),
}

def str2bool(boolstr):
    dictstr = {'True': True, 'False': False}
    return dictstr[boolstr]

# children_for_import = {'bp': {'apikey':'baseplates', 'dbtabname': 'baseplate', 'db_cols': {'serial_number': 'bp_name',  'kind': 'kind', 'comment_description': 'comment'}},
#                 'sen': {'apikey':'sensors',    'dbtabname': 'sensor',    'db_cols': {'serial_number': 'sen_name', 'kind': 'kind', 'comment_description': 'comment', 'batch_number': 'sen_batch_id'}},
#                 'hxb': {'apikey':'pcbs',       'dbtabname': 'hexaboard', 'db_cols': {'serial_number': 'hxb_name', 'kind': 'kind', 'comment_description': 'comment'}},
#             }

# bp_qc_cols = {'qc_cols': {'tolerance_grade': 'tolerance_grade',
#                   'flatness_grade': 'flatness_grade',
#                   'height_lam_avg': 'avg_thickness_init',
#                   'height_lam_max': 'max_thickness_init',
#                   'flatness_lam': 'flatness_init',
#                   'weight_lam': 'weight_grams',  }}

# children_for_import['bp'].update(bp_qc_cols)

with open('import_data/cmsr_to_postgres_column_names.yaml', 'r') as f:
    children_for_import = yaml.safe_load(f)['children_for_import']

def check_roc_count(hxb_name, roc_count):
    ### https://gitlab.cern.ch/hgcal-database/new-attribute-schema/-/issues/6
    roc_count_dict = kop_yaml['roc_count_for_res_geom']
    if roc_count_dict[hxb_name[4:6]] == roc_count:
        return True
    return False

async def get_missing_roc_hxb(pool):
    get_missing_roc_hxb_query = """SELECT REPLACE(hxb_name,'-','') AS hxb_name FROM hexaboard WHERE roc_name IS NULL OR roc_index IS NULL;"""
    async with pool.acquire() as conn:
        rows = await conn.fetch(get_missing_roc_hxb_query)
    return [row['hxb_name'] for row in rows]

async def get_missing_qc_bp(pool):
    get_missing_qc_bp_query = """SELECT REPLACE(bp_name,'-','') AS bp_name FROM baseplate WHERE tolerance_grade IS NULL OR flatness_grade IS NULL;"""
    async with pool.acquire() as conn:
        rows = await conn.fetch(get_missing_qc_bp_query)
    return [row['bp_name'] for row in rows]

async def get_mmts_inv_in_local(pool):
    get_mmts_inv_in_local_query = """SELECT part_name FROM mmts_inventory WHERE kind IS NULL;"""
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(get_mmts_inv_in_local_query)
        return [row['part_name'] for row in rows]
    except Exception as e:
        print(f"{RED}Error: {e}{RESET}")

async def get_missing_batch_sen(pool):
    get_missing_batch_sen_query = """SELECT REPLACE(sen_name,'-','') AS sen_name FROM sensor WHERE sen_batch_id IS NULL OR sen_batch_id = '';"""
    async with pool.acquire() as conn:
        rows = await conn.fetch(get_missing_batch_sen_query)
    return [row['sen_name'] for row in rows]

def get_query_write(table_name, column_names, check_conflict_col = None, db_upload_data = None):
    pre_query = f""" INSERT INTO {table_name} ({', '.join(column_names)}) SELECT """
    data_placeholder = ', '.join([f'${i+1}' for i in range(len(column_names))])
    query = f""" {pre_query} {f'{data_placeholder}'}  """
    if check_conflict_col is not None:
        query += f" WHERE NOT EXISTS ( SELECT 1 FROM {table_name} WHERE {check_conflict_col} = '{db_upload_data[check_conflict_col]}'); "
    return query

def get_query_update(table_name, column_names, check_conflict_col = None, db_upload_data = None):
    update_columns = ', '.join([f"{column} = ${i+1}" for i, column in enumerate(column_names)])
    if check_conflict_col in ['hxb_name', 'bp_name']:
        query = f""" UPDATE {table_name} SET {update_columns} WHERE {check_conflict_col} = '{db_upload_data[check_conflict_col]}' AND (kind IS NULL OR obsolete IS NULL);"""
    else:
        query = f""" UPDATE {table_name} SET {update_columns} WHERE {check_conflict_col} = '{db_upload_data[check_conflict_col]}' AND kind IS NULL;"""
    return query

def get_query_update_secondary(table_name, column_names, check_conflict_col = None, db_upload_data = None):
    update_columns = ', '.join([f"{column} = ${i+1}" for i, column in enumerate(column_names)])
    query = f""" UPDATE {table_name} SET {update_columns} WHERE {check_conflict_col} = '{db_upload_data[check_conflict_col]}';"""
    return query

# def check_exists_query(table_name, column_names):
#     pre_query = f"""SELECT EXISTS ( SELECT 1 FROM {table_name} WHERE """ 
#     data_placeholder = [f'{col_name} = ${n+1}' for n, col_name in enumerate(column_names)]
#     query = f'{pre_query} {" AND ".join(data_placeholder)} );'
#     return query

async def write_to_db(pool, db_upload_data, partType = None, check_conflict_col=None):
    table_name = children_for_import[partType]["dbtabname"]
    async with pool.acquire() as conn:
        query = get_query_write(table_name, db_upload_data.keys(), check_conflict_col=check_conflict_col, db_upload_data=db_upload_data)
        await conn.execute(query, *db_upload_data.values())
        query = get_query_update(table_name, db_upload_data.keys(), check_conflict_col=check_conflict_col, db_upload_data=db_upload_data)
        await conn.execute(query, *db_upload_data.values())

async def write_to_db_secondary(pool, db_upload_data, partType = None, check_conflict_col=None):
    table_name = children_for_import[partType]["dbtabname"]
    async with pool.acquire() as conn:
        query = get_query_update_secondary(table_name, db_upload_data.keys(), check_conflict_col=check_conflict_col, db_upload_data=db_upload_data)
        await conn.execute(query, *db_upload_data.values())

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
        try:
            return_dict.update({'geometry' : kop_yaml['geometry'][partName[5]]})
            return_dict.update({'resolution' : kop_yaml['resolution'][partName[6]]})
            return_dict.update({'bp_material' : kop_yaml['material'][partName[7]]})
        except:
            None
            # print(f"{partType} {partName} could be a legacy part since it does not follow current naming convention.")
    elif partType == 'hxb':
        try:
            return_dict.update({'resolution': kop_yaml['resolution'][partName[4]]})  
            return_dict.update({'geometry': kop_yaml['geometry'][partName[5]]})  
            return_dict.update({'roc_version': kop_yaml['roc_version'][partName[7]]})  
        except:
            None
            # print(f"{partType} {partName} could be a legacy part since it does not follow current naming convention.")
    elif partType == 'sen':
        if partName[:2] == 25:
            return_dict.update({'resolution': kop_yaml['sensor'][partName[:2]][1]})  
        else:
            return_dict.update({'resolution': kop_yaml['sensor'][partName[0]][1]})  

        return_dict.update({'geometry': kop_yaml['sensor_geometry'][partName[-1]]})  
        return_dict.update({'thickness': int(kop_yaml['sensor'][partName[0]][0])})  
    return return_dict

def get_dict_for_db_upload(data_full, partType):
    try:
        db_dict = {children_for_import[partType]["db_cols"][k]: data_full[k] for k in (children_for_import[partType]["db_cols"]).keys()}
        db_dict.update(get_part_type(data_full['serial_number'], partType))
        if partType in ['hxb', 'bp']:
            db_dict['obsolete'] = True if db_dict['obsolete'].lower() == 'obsolete' else False
        return db_dict
    except Exception as e:
        traceback.print_exc()
        print(f"ERROR in acquiring data from API output for {data_full['serial_number']}", e)
        # print(json.dumps(data_full, indent=2))
        # print('*'*100)
        return None
    
def get_roc_dict_for_db_upload(hxb_name, cern_db_url = 'hgcapi'):
    try:
        data_full = read_from_cern_db(partID = hxb_name, cern_db_url=cern_db_url)
        roc_names, roc_indices = [], []
        if data_full:
            child_list, hxb_name = data_full["children"], data_full["serial_number"]
            for child in child_list:
                if "HGCROC" in child["kind"]:
                    roc_names.append(child["serial_number"])
                    roc_indices.append(child["attribute"])
                    key_func = natsort_keygen()    
            if roc_names:
                roc_indices_sorted = natsorted(roc_indices, key=key_func)
                roc_names_sorted = natsorted(roc_names, key=key_func)
                roc_indices_sorted = None if None in roc_indices_sorted else roc_indices_sorted
                roc_names_sorted = None if None in roc_names_sorted else roc_names_sorted
                db_dict = {"hxb_name": hxb_name, "roc_name": roc_names_sorted, "roc_index": roc_indices_sorted}
                if not check_roc_count(hxb_name, roc_count = len(roc_names_sorted)):
                    print(f"Part {hxb_name} has an incomplete list of ROCs. Add manually to postgres after contacting the CERN database team on GitLab: https://gitlab.cern.ch/groups/hgcal-database/-/issues.")
                return db_dict
    except Exception as e:
        traceback.print_exc()
        print(f"ERROR in acquiring ROC data from API output for {hxb_name}: ", e)
        # print(json.dumps(data_full, indent=2))
        # print('*'*100)
        return None

def get_bp_qc_for_db_upload(bp_name, cern_db_url = 'hgcapi', part_qc_cols = None):
    """
    According to Jay Mathew Lawhorn, (ETP):
    We will not send you any parts for installation that have any of the booleans false (and if we send you rejected baseplates as dummies, the tolerance grade will be a C).
    Tolerance grade and flatness grade are separate, with tolerance grade covering everything except flatness. We thought the flatness grading might change, so we kept it separate.
    """
    try:
        data_full = read_from_cern_db(partID = bp_name, cern_db_url=cern_db_url)
        db_dict = None
        if data_full:
            if "baseplate_raw" in data_full["qc"]:
                db_dict = {"bp_name": bp_name}
                child_dict, bp_name = data_full["qc"]["baseplate_raw"], data_full["serial_number"]
                for qck in part_qc_cols.keys():
                    db_dict.update({part_qc_cols[qck]: child_dict[qck]})
        return db_dict
    except Exception as e:
        traceback.print_exc()
        print(f"ERROR in acquiring baseplate QC data from API output for {bp_name}: ", e)
        # print(json.dumps(data_full, indent=2))
        # print('*'*100)
        return None

def get_sen_batch_for_db_upload(sen_name, cern_db_url = 'hgcapi'):
    try:
        data_full = read_from_cern_db(partID = sen_name, cern_db_url=cern_db_url)
        db_dict = None
        if data_full:
            db_dict = {"sen_name": sen_name,'sen_batch_id': data_full["batch_number"]}
        return db_dict
    except Exception as e:
        traceback.print_exc()
        print(f"ERROR in acquiring sensor batch data from API output for {sen_name}: ", e)
        # print(json.dumps(data_full, indent=2))
        # print('*'*100)
        return None

def get_mmts_inv_for_db_upload(part_name, cern_db_url = 'hgcapi'):
    try:
        data_full = read_from_cern_db(partID = part_name, cern_db_url=cern_db_url)
        db_dict = get_dict_for_db_upload(data_full, 'mmtsinv')
        db_dict['qc_details'] = f"{db_dict['qc_details']}"
        return db_dict
    except Exception as e:
        # traceback.print_exc()
        print(f"ERROR in acquiring data from API output for {part_name}: ", e)
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
    parser.add_argument('-getbp', '--get_baseplate', default='True', required=False, help="Get baseplates.")
    parser.add_argument('-gethxb', '--get_hexaboard', default='True', required=False, help="Get hexaboards.")
    parser.add_argument('-getsen', '--get_sensor', default='True', required=False, help="Get sensors.")
    parser.add_argument('-getmmtsinv', '--mmts_inventory', default='True', required=False, help="Get trophyboards, mezzanines, etc for the MMTS.")
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

    part_types_to_get = []
    if str2bool(args.get_baseplate):
        part_types_to_get.append('bp')
    if str2bool(args.get_hexaboard):
        part_types_to_get.append('hxb')
    if str2bool(args.get_sensor):
        part_types_to_get.append('sen')
    if str2bool(args.mmts_inventory):
        part_types_to_get.append('mmtsinv')

    for source_db_cern in db_list:
        cern_db_url = db_source_dict[source_db_cern]['url']
        pool = await asyncpg.create_pool(**db_params)
        for pt in part_types_to_get:  #, 'pml', 'ml']:
            if pt != 'mmtsinv': ### We don't have a way to request mmts parts from the api based on institution    
                print(f"Reading {children_for_import[pt]['apikey']} from {cern_db_url.upper()} ..." )
                parts = (read_from_cern_db(macID = inst_code.upper(), partType = pt, cern_db_url = cern_db_url))
                if parts:
                    for p in tqdm(parts['parts']): ### p is the data_full from the HGCAPI
                        try:
                            db_dict = get_dict_for_db_upload(p, partType = pt)
                            if db_dict is not None:
                                try:
                                    await write_to_db(pool, db_dict, partType = pt, check_conflict_col = children_for_import[pt]['db_cols']['serial_number'])
                                except Exception as e:
                                    print(f"ERROR for single part upload for {p['serial_number']}", e)
                                    traceback.print_exc()
                                    print('Dictionary:', (db_dict))
                        except:
                            traceback.print_exc()

            secondary_upload, db_dict_secondary = None, None
            if pt == 'hxb':
                secondary_upload = await get_missing_roc_hxb(pool)
            elif pt == 'bp':
                secondary_upload = await get_missing_qc_bp(pool)
                part_qc_cols = children_for_import[pt]['qc_cols']
            elif pt == 'sen':
                secondary_upload = await get_missing_batch_sen(pool)
            elif pt == 'mmtsinv':
                secondary_upload = await get_mmts_inv_in_local(pool)
                # secondary_upload = ['320TSYMM1020192', '320TSYLFP010104']
            if secondary_upload:
                print(f"Fetching other necessary {children_for_import[pt]['apikey']} data ...")
                for p in tqdm(secondary_upload):
                    try:
                        if pt == 'hxb':
                            db_dict_secondary = get_roc_dict_for_db_upload(p, cern_db_url = cern_db_url)
                        elif pt == 'bp':
                            db_dict_secondary = get_bp_qc_for_db_upload(p, cern_db_url = cern_db_url, part_qc_cols = part_qc_cols)
                        elif pt == 'sen':
                            db_dict_secondary = get_sen_batch_for_db_upload(p, cern_db_url = cern_db_url)
                        elif pt == 'mmtsinv':
                            db_dict_secondary = get_mmts_inv_for_db_upload(p, cern_db_url = cern_db_url)
                        if db_dict_secondary is not None:
                            try:
                                await write_to_db_secondary(pool, db_dict_secondary, partType = pt, check_conflict_col = children_for_import[pt]['db_cols']['serial_number'])
                            except Exception as e:
                                print(f"ERROR for single part upload for {p['serial_number']}", e)
                                traceback.print_exc()
                                print('Dictionary:', (db_dict_secondary))
                    except:
                        traceback.print_exc()
                
                print(f"Writing {children_for_import[pt]['apikey']} to postgres from {cern_db_url.upper()} complete.")
                print('-'*40); print('\n')
            break
                            
    async with pool.acquire() as conn:
        try:
            query_v3c = f"""UPDATE hexaboard SET roc_version = 'HGCROCV3c' WHERE comment LIKE '%44-4c%' AND (roc_version IS NULL OR roc_version <> 'HGCROCV3c'); """
            await conn.execute(query_v3c)
        except:
            print('v3c query failed')
    await pool.close()
    print('Refresh postgres tables')

asyncio.run(main())

