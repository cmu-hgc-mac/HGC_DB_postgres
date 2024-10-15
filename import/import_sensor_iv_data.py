import csv, os, glob, subprocess, random
import asyncio, asyncpg, yaml, pwinput
from datetime import datetime

async def get_conn_pool():
    loc = '../dbase_info/'
    yaml_file = f'{loc}tables.yaml'
    db_params = {
        'database': yaml.safe_load(open(yaml_file, 'r'))['dbname'],
        'user': 'shipper',
        'password': pwinput.pwinput(prompt='Enter user password: ', mask='*'),
        'host': yaml.safe_load(open(yaml_file, 'r'))['db_hostname']}   
    pool = await asyncpg.create_pool(**db_params)
    return pool

def get_query_write(table_name, column_names):
    pre_query = f""" INSERT INTO {table_name} ({', '.join(column_names)}) VALUES """
    data_placeholder = ', '.join(['${}'.format(i) for i in range(1, len(column_names)+1)])
    query = f"""{pre_query} {'({})'.format(data_placeholder)}"""
    return query

async def upload_PostgreSQL(conn, table_name_list, db_upload_data_list):    
    schema_name = 'public'
    table_exists_query = """SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2);"""
    if type(table_name_list) is not list:
        table_name_list, db_upload_data_list = [table_name_list], [db_upload_data_list]
    for table_name, db_upload_data in zip(table_name_list, db_upload_data_list):
        table_exists = await conn.fetchval(table_exists_query, schema_name, table_name)  ### Returns True/False
        if table_exists:
            query = get_query_write(table_name, [dat.lower() for dat in db_upload_data.keys()]  ) ## make keys lower case
            await conn.execute(query, *db_upload_data.values())
            # print(f'Executing query: {query}')
            print(f'Data successfully uploaded to the {table_name}!')
        else:
            print(f'Table {table_name} does not exist in the database.')
    await conn.close()
    return 'Upload Success'

def get_sensor_iv_data(filename):
    data = {
        'VOLTS': [],
        'CURNT_NANOAMP': [],
        'ERR_CURNT_NANOAMP': [],
        'TOT_CURNT_NANOAMP': [],
        'ACTUAL_VOLTS': [],
        'TIME_SECS': [],
    }
    
    cond_list = ['SENSOR_ID','SCRATCHPAD_ID','TEMP_DEGC','HUMIDITY_PRCNT']
    with open(filename, newline='') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)
        column_indices = {key: header.index(key.upper()) for key in data.keys()}
        cell_nr_idx = header.index('CELL_NR')

        max_cell_nr = 0
        for row in reader:
            cell_nr = int(row[cell_nr_idx])
            if cell_nr > max_cell_nr:
                max_cell_nr = cell_nr
        for key in data:
            data[key] = [[] for _ in range(max_cell_nr)]
            
        csvfile.seek(0); next(reader)  # Skip the header row

        for row in reader:
            cell_nr = int(row[cell_nr_idx]) - 1  
            for key, idx in column_indices.items():
                data[key][cell_nr].append(float(row[idx]))
        
        csvfile.seek(0)
        header = next(reader)
        column_indices = {key: header.index(key.upper()) for key in cond_list}
        for row in reader:
            data.update({key: str(row[idx]) for key, idx in column_indices.items()})
            print(data['SCRATCHPAD_ID'])
            return data

def get_sensor_summary_data(filename):        
    with open(filename, mode='r') as file:
        csv_reader = csv.reader(file)
        keys = next(csv_reader)   # First row for keys
        values = next(csv_reader) # Second row for values
        data_dict = dict(zip(keys, values))
        data_dict['SENSOR_PASS'] = data_dict['PASS']
        del data_dict['']
        del data_dict['PASS']
        print(data_dict['SCRATCHPAD_ID'])
    return data_dict

def read_and_write_sensor_data(sensor_id, pascal_path = '/Users/sindhu/Downloads/pascal/'):
    output_dir = os.path.join(pascal_path, 'outputs')
    os.chdir(pascal_path)
    command = ['python', 'lxplus.py', '-IV_Full', '-SensorID', f'{sensor_id}']
    result = subprocess.run(command, capture_output=True, text=True, cwd=pascal_path)
    # try: 
    #     print(result.stdout, result.stderr)
    # except Exception as e:
    #         print("An error occurred:", str(e))
    files = sorted(glob.glob(os.path.join(output_dir, '*')), key=os.path.getmtime, reverse=True)
    if 'iv' not in files[0]:
        os.rename(files[0], f"{str(files[0]).split('.csv')[0]}_ivfull_{sensor_id}.csv")
        print(f'iv data saved {sensor_id}')
    else:
        print(f'no iv data found for {sensor_id}')
    command = ['python', 'lxplus.py', '-IV_grade', '-SensorID', f'{sensor_id}']
    result = subprocess.run(command, capture_output=True, text=True, cwd=pascal_path)
    # try: 
    #     print(result.stdout, result.stderr)
    # except Exception as e:
    #         print("An error occurred:", str(e))
    files = sorted(glob.glob(os.path.join(output_dir, '*')), key=os.path.getmtime, reverse=True)
    if 'iv' not in files[0]:
        os.rename(files[0], f"{str(files[0]).split('.csv')[0]}_ivsummary_{sensor_id}.csv") 
        print(f'summary data saved {sensor_id}')
    else:
        print(f'no summary data found for {sensor_id}')

    files = sorted(glob.glob(os.path.join(output_dir, '*')), key=os.path.getmtime, reverse=True)
    if len(files) < 2:
        print("There are fewer than two files in the directory.")
    else:
        last_two_files = files[:2] ## last file & second last file
        filename_iv_data, filename_iv_summary = last_two_files[1], last_two_files[0]
        if sensor_id in filename_iv_data and sensor_id in filename_iv_summary:
            print('files read: ', filename_iv_data, filename_iv_summary)
            data_upload_dict = get_sensor_iv_data(filename_iv_data)
            data_upload_dict.update(get_sensor_summary_data(filename_iv_summary))
            for file in last_two_files: os.remove(file)
            return data_upload_dict

async def main():
    pool = await get_conn_pool()
    pascal_path = '/Users/sindhu/Downloads/pascal/'
    sensor_local_list = ['100114', '200120', '200119', '200118', '200117', '200116', '100147', '100137', '100136', '100144', '100143', '100190', '200165', '100191', '200166']
    sensor_local_list = [110190, 200165, 100191, 200166, 100136, 100137]
    for sensor_id in sensor_local_list:
        try:
            db_upload_dict = read_and_write_sensor_data(str(sensor_id), pascal_path)
            async with pool.acquire() as conn:
                await upload_PostgreSQL(conn, 'sen_iv_data', db_upload_dict)
        except Exception as e:
            print("An error occurred:", str(e))
        print('\n')
    await pool.close()

asyncio.run(main())