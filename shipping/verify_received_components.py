import yaml, os, argparse, sys, csv, datetime
import pwinput, asyncio, asyncpg, base64, traceback
from cryptography.fernet import Fernet
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

loc = 'dbase_info'
conn_yaml_file = os.path.join(loc, 'conn.yaml')
conn_info = yaml.safe_load(open(conn_yaml_file, 'r'))
kop_yaml = yaml.safe_load(open(os.path.join('export_data', 'resource.yaml'), 'r')).get('kind_of_part')
inst_code  = conn_info.get('institution_abbr')

db_params = {
    'database': conn_info.get('dbname'),
    'user': 'editor',
    'host': conn_info.get('db_hostname'),
    'port': conn_info.get('port'),
}

def get_query_write(table_name, part_id_col = None, part_id = None, date_verified = None):
    # pre_query = f""" INSERT INTO {table_name} ({', '.join(column_names)}) SELECT """
    # data_placeholder = ', '.join([f'${i+1}' for i in range(len(column_names))])
    query = f""" INSERT INTO {table_name} ({part_id_col}, date_verified) SELECT {part_id}, {date_verified}"""
    query += f""" WHERE NOT EXISTS ( SELECT 1 FROM {table_name} WHERE {part_id_col} = '{part_id}' AND date_verify_received IS NOT NULL); """
    query += f""" UPDATE {table_name} SET date_verified = {date_verified} WHERE {part_id_col} = '{part_id}' AND date_verify_received IS NULL;"""
    return query

async def write_to_db(partType=None, part_id_list=None, date_verified=None):
    pk_dict = {'baseplate': 'bp_name', 'sensor':'sen_name', 'hexaboard':'hxb_name'}
    part_id_col = pk_dict[partType]
    conn = await asyncpg.connect(**db_params)
    for part_id in part_id_list:
        query = get_query_write(table_name = partType, part_id_col = part_id_col , part_id=part_id, date_verified=date_verified)
        await conn.execute(query)
    await conn.close()

async def main():
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
    parser.add_argument('-p', '--password', default=None, required=False, help="Password to access database.")
    parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
    parser.add_argument('-pt', '--partType', default=None, required=True, help="Part type being verified.")
    parser.add_argument('-fp', '--file_path', default=None, required=False, help="Location of file containing parts")
    parser.add_argument('-dv', '--date_verify', type=lambda s: str(datetime.datetime.strptime(s, '%Y-%m-%d').date()), default=str(today), help=f"Date when part was verified (format: YYYY-MM-DD). Default is today's date: {today}")
    args = parser.parse_args()
    
    date_verified = datetime.datetime.strptime(args.date_verify, '%Y-%m-%d').date()
    if args.password is None:
        dbpassword = pwinput.pwinput(prompt='Enter superuser password: ', mask='*')
    else:
        if args.encrypt_key is None:
            print("Encryption key not provided. Exiting..."); exit()
        cipher_suite = Fernet((args.encrypt_key).encode())
        dbpassword = cipher_suite.decrypt( base64.urlsafe_b64decode(args.password)).decode() ## Decode base64 to get encrypted string and then decrypt
        db_params.update({'password': dbpassword})

    def read_parts_from_file(filename):
        file_name, file_extension = os.path.splitext(filename)
        part_names = []
        with open(filename, mode='r') as file:
            if file_extension == '.csv':
                reader = csv.reader(file)
                for line in reader:
                    part_names.append(line[0].strip())
            elif file_extension == '.txt':
                for line in file:
                    part_names.append(line.strip())
        return part_names

    part_names = read_parts_from_file(args.file_path)
    await write_to_db(partType=args.partType, part_id_list=part_names, date_verified=date_verified)
    print(f"--> {len(part_names)} {args.partType}(s) marked as verified.")
    

asyncio.run(main())
    
    