import asyncio, asyncpg, yaml, os, argparse, base64, pwinput
from cryptography.fernet import Fernet

async def update_passwords(mac_dict, new_user_password = None, new_viewer_password = None):
    ## change HOSTNAME, database name
    # mac_dict = {'host': 'HOSTNAME',   'database':'hgcdb', 'password': pwinput.pwinput(prompt='Enter superuser password: ', mask='*'), 'user': 'postgres'}
    
    conn = await asyncpg.connect(**mac_dict)
    if new_user_password:
        user_types = ['shipper', 'ogp_user', 'gantry_user', 'teststand_user', 'wirebond_user', 'encap_user',  'editor']
        for u in user_types:
            query = f"""ALTER USER {u} WITH PASSWORD '{new_user_password}';"""
            result = await conn.execute(query)
            print(u, result)

    if new_viewer_password:
        user_types = ['viewer']
        for u in user_types:
            query = f"""ALTER USER {u} WITH PASSWORD '{new_viewer_password}';"""
            result = await conn.execute(query)
            print(u, result)

    await conn.close()

def main():
    parser = argparse.ArgumentParser(description="A script that updates viewer and user passwords.")
    parser.add_argument('-p', '--password', default=None, required=False, help="Password to access database.")
    parser.add_argument('-up', '--userpass', default=None, required=False, help="Password to write to database.")
    parser.add_argument('-vp', '--viewerpass', default=None, required=False, help="Password to view database.")
    parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
    args = parser.parse_args()

    loc = 'dbase_info'
    conn_yaml_file = os.path.join(loc, 'conn.yaml')
    conn_info = yaml.safe_load(open(conn_yaml_file, 'r'))
    db_params = {
        'database': conn_info.get('dbname'), 
        'user': 'postgres',
        'host': conn_info.get('db_hostname'),
        'port': conn_info.get('port'),}

    if args.password is None:
        dbpassword = pwinput.pwinput(prompt='Enter superuser password: ', mask='*')
        db_params.update({'password': dbpassword})
        user_password = input('Set user password: ')    
        viewer_password = input('Set viewer password: ')
    else:
        if args.encrypt_key is None:
            print("Encryption key not provided. Exiting..."); exit()
        cipher_suite = Fernet((args.encrypt_key).encode())
        dbpassword = cipher_suite.decrypt( base64.urlsafe_b64decode(args.password)).decode() ## Decode base64 to get encrypted string and then decrypt
        db_params.update({'password': dbpassword})
        user_password = args.userpass
        viewer_password = args.viewerpass

    asyncio.run(update_passwords(mac_dict=db_params, new_user_password=user_password.strip(), new_viewer_password=viewer_password.strip()))

if __name__ == '__main__':
    main()