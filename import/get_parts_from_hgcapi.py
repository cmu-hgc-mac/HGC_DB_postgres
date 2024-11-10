import requests, json, yaml, os, argparse, datetime
import pwinput, asyncio, asyncpg

loc = 'dbase_info'
conn_yaml_file = os.path.join(loc, 'conn.yaml')
conn_info = yaml.safe_load(open(conn_yaml_file, 'r'))
inst_code  = conn_info.get('institution_abbr')

db_params = {
    'database': conn_info.get('dbname'),
    'user': 'ogp_user',
    'host': conn_info.get('db_hostname'),
    'port': conn_info.get('port'),
}

partTrans = {'bp' : {'apikey':'baseplates', 'dbtabname': 'bp_inspect', 'db_col': 'bp_name', 'qc_cols': {'grade': 'grade' ,'thickness': 'thickness','comments': 'comment', 'flatness':'flatness', 'weight': 'weight'}},
             'sen':{'apikey':'sensors', 'dbtabname': 'sensor', 'db_col': 'sen_name', 'qc_cols': {'grade': 'grade' ,'thickness': 'thickness','comments': 'comment'}},
             'hxb':{'apikey':'pcbs', 'dbtabname': 'hxb_inspect', 'db_col': 'hxb_name', 'qc_cols': {'grade': 'grade' ,'thickness': 'thickness','comments': 'comment', 'flatness':'flatness', 'weight': 'weight'}},
             'pml':{'apikey':'protomodules', 'dbtabname': 'proto_inspect', 'db_col': 'proto_name' ,'qc_cols':  {'prto_grade': 'grade', 'prto_thkns_mm': 'ave_thickness', "prto_thkns_mm": 'max_thickness', 'prto_fltns_mm': 'flatness', "snsr_x_offst": 'x_offset_mu', "snsr_y_offst": 'y_offset_mu',"snsr_ang_offst": 'ang_offset_deg'}},
             'ml' :{'apikey':'modules', 'dbtabname': 'module_inspect', 'db_col': 'module_name', 'qc_cols':  {'mod_grade': 'grade', 'mod_ave_thkns_mm': 'ave_thickness', "mod_max_thkns_mm": 'max_thickness', 'mod_fltns_mm': 'flatness', "pcb_plcment_x_offset": 'x_offset_mu', "pcb_plcment_y_offset": 'y_offset_mu',"pcb_plcment_ang_offset": 'ang_offset_deg'}},
            }

def get_query_write(table_name, column_names):
    pre_query = f""" INSERT INTO {table_name} ({', '.join(column_names)}) VALUES """
    data_placeholder = ', '.join(['${}'.format(i) for i in range(1, len(column_names)+1)])
    query = f"""{pre_query} {'({})'.format(data_placeholder)}"""
    return query

def check_exists_query(table_name, column_names):
    pre_query = f"""SELECT EXISTS ( SELECT 1 FROM {table_name} WHERE """ 
    data_placeholder = [f'{col_name} = ${n+1}' for n, col_name in enumerate(column_names)]
    query = f'{pre_query} {" AND ".join(data_placeholder)} );'
    return query

async def write_to_db(pool, db_upload_data, partType = None):
    table_name = partTrans[partType]["dbtabname"]
    check_col = {key: db_upload_data[key] for key in [partTrans[partType]["db_col"], "date_inspect"]}
    async with pool.acquire() as conn:
        check_exists = await conn.fetchval( check_exists_query(table_name, check_col.keys()), *check_col.values())
        if not check_exists:
            query = get_query_write(table_name, db_upload_data.keys())
            await conn.execute(query, *db_upload_data.values())

def get_url(partID = None, macID = None, partType = None):
    if partID is not None:
        return f'https://hgcapi.web.cern.ch/mac/part/{partID}/full'
    elif partType is not None:
        if macID is not None:
            return f'https://hgcapi.web.cern.ch/mac/parts/types/{partTrans[partType.lower()]["apikey"]}?page=0&limit=100&location={macID}'
        return f'https://hgcapi.web.cern.ch/mac/parts/types/{partTrans[partType.lower()]["apikey"]}?page=0&limit=100'
    return

def read_from_cern_db(partID = None, macID = None, partType = None ):
    headers = {'Accept': 'application/json'}
    response = requests.get(get_url(partID = partID, macID = macID, partType = partType), headers=headers)
    if response.status_code == 200:
        data = response.json() ; 
#         print(json.dumps(data, indent=2))
        return data
    else:
        print(f'ERROR in reading from HGCAPI for partID : {partID} :: {response.status_code}')
        return None

def form(data):
    if type(data) is str:
        if data.lstrip('-').replace('.',"").isdigit():
            return float(data)
        elif (data.lower() == 'none' or data.lower() == 'null' or data == ''):
            return None
    return data

def get_data_for_db(data_full, partType):
    try:
        db_dict = {partTrans[partType]["db_col"] : data_full['serial_number'],}
        if bool(data_full["record_lastupdate_time"]):
            datetime_object = datetime.datetime.fromisoformat(data_full["record_lastupdate_time"] )
        else:
            datetime_object = datetime.datetime.fromisoformat(data_full["record_insertion_time"] )
        db_dict.update({'date_inspect': datetime_object.date()})
        db_dict.update({'time_inspect': datetime_object.time()})
        qc_cols = partTrans[partType]["qc_cols"]
        if bool(data_full['qc']):
            if partType in ['bp','hxb','sen']:
                qc_data = data_full['qc'][partTrans[partType]["apikey"][0:-1]]
                db_dict.update({qc_cols[key]: form(qc_data[key]) if key in qc_data.keys() else None for key in qc_cols.keys() })
            elif partType in ['pml', 'ml']:
                if bool(data_full['qc'][f'{partTrans[partType]["apikey"][0:-1]}_assembly']):
                        qc_data = data_full['qc'][f'{partTrans[partType]["apikey"][0:-1]}_assembly']
                        db_dict.update({qc_cols[key]: form(qc_data[key]) if key in qc_data.keys() else None for key in qc_cols.keys() })
        else:
            db_dict.update({qc_cols[key]: None for key in qc_cols.keys()})
        return db_dict
    except Exception as e:
        # print('*'*100)
        print(f'ERROR in acquiring data from API output for {data_full["serial_number"]}', e)
        # print(json.dumps(data_full, indent=2))
        # print('*'*100)
        return None

# def process_part_id(partID = None, partType = None):
#     part_id = partID[0:3].replace("320","") + partID[3:]
#     part_id = (str(part_id).replace("-",""))
#     pos_list = {'bp': [2, 5, 7], 'sen': [2, 5, 7], 'hxb': [2, 5, 7]}
#     parts = [part_id[i:j] for i, j in zip([0] + pos_list[partType], pos_list[partType] + [None])]
#     output_string = f'320{"".join(parts)}'
#     return output_string

async def main():
    parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
    parser.add_argument('-p', '--password', default=None, required=False, help="Password to access database.")
    parser.add_argument('-pid', '--pardID', default=None, required=False, help="Part ID to query from HGC API.")
    args = parser.parse_args()

    dbpassword = args.password
    if dbpassword is None:
        dbpassword = (pwinput.pwinput(prompt='Enter ogp_user password: ', mask='*')).replace(" ", "")
    db_params.update({'password': dbpassword})

    pool = await asyncpg.create_pool(**db_params)
    for pt in ['bp','hxb','sen', 'pml', 'ml']:
        print(f'Reading {partTrans[pt]["apikey"]} from HGCAPI ...' )
        parts = (read_from_cern_db(macID = inst_code.upper(), partType = pt))['parts']
        for p in parts:
            data_full = read_from_cern_db(partID = p['serial_number'])
            if data_full is not None:
                db_dict = get_data_for_db(data_full, partType = pt)
                if db_dict is not None:
                    try:
                        # print(db_dict)
                        await write_to_db(pool, db_dict, partType = pt)
                    except Exception as e:
                        print(f'ERROR for single part upload for {data_full} {db_dict}', e)
                        print('Dictionary:', (db_dict))
        print(f'Writing {partTrans[pt]["apikey"]} to postgres complete.')
        print('-'*40); print('\n')
    await pool.close()
    print('Refresh postgres tables')

asyncio.run(main())