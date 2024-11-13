import platform, os, argparse, base64
from scp import SCPClient
import numpy as np
import datetime, yaml, paramiko, pwinput, sys
from tqdm import tqdm
from cryptography.fernet import Fernet


loc = 'dbase_info'
conn_yaml_file = os.path.join(loc, 'conn.yaml')
# cern_dbase  = yaml.safe_load(open(conn_yaml_file, 'r')).get('cern_db')
cern_dbase  = 'dev_db'## for testing purpose, otherwise uncomment above.
cerndb_types = {"dev_db": {'dbtype': 'Development', 'dbname': 'INT2R'}, 
                "prod_db": {'dbtype': 'Production','dbname':'CMSR'}}
cern_dbname = (cerndb_types[cern_dbase]['dbname']).lower()

def valid_directory(path):
    if os.path.isdir(path):
        return path
    else:
        raise argparse.ArgumentTypeError(f"Invalid directory: {path}")

def find_files_by_date(directory, target_date):
    matched_files = []
    target_date = datetime.datetime.strptime(target_date, '%Y-%m-%d').date()
    
    # Check if the directory exists
    if not os.path.exists(directory):
        print(f"Directory does not exist: {directory}")
        return matched_files

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.lower().endswith('.xml'):
                file_path = os.path.join(root, file)
                file_stat = os.stat(file_path)
                mod_time = datetime.date.fromtimestamp(file_stat.st_mtime)
                create_time = datetime.date.fromtimestamp(file_stat.st_ctime)
                if mod_time == target_date or create_time == target_date:
                    matched_files.append(file_path)
    return matched_files


def get_build_files(files_list):
    build_files = []
    other_files = []
    for fname in files_list:
        if 'build' in fname.lower(): 
            build_files.append(fname)
        else:
            other_files.append(fname)
    return build_files, other_files


def scp_to_dbloader(dbl_username, dbl_password, fname, encryption_key = None):
    ssh_server1 = paramiko.SSHClient()
    ssh_server1.load_system_host_keys()
    ssh_server1.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        if encryption_key is not None:
            cipher_suite = Fernet(encryption_key.encode())  ## Decode base64 to get encrypted string and then decrypt
            ssh_server1.connect(hostname='lxplus.cern.ch', username=dbl_username, password = cipher_suite.decrypt( base64.urlsafe_b64decode(dbl_password)).decode() )
        else:
            ssh_server1.connect(hostname='lxplus.cern.ch', username=dbl_username, password=dbl_password)

        transport = ssh_server1.get_transport()
        dest_addr = ('dbloader-hgcal', 22)  
        local_addr = ('127.0.0.1', 22) # localhost
        channel = transport.open_channel("direct-tcpip", dest_addr, local_addr)
        ssh_server2 = paramiko.SSHClient()
        ssh_server2.load_system_host_keys()
        ssh_server2.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if encryption_key is not None:
            ssh_server2.connect(hostname='dbloader-hgcal', username=dbl_username, password = cipher_suite.decrypt( base64.urlsafe_b64decode(dbl_password)).decode() , sock=channel)
        else:
            ssh_server2.connect(hostname='dbloader-hgcal', username=dbl_username, password=dbl_password, sock=channel)

        with SCPClient(ssh_server2.get_transport()) as scp:
            scp.put(fname, f'/home/dbspool/spool/hgc/{cern_dbname}/')

        scp.close()
        ssh_server2.close()
        ssh_server1.close()    
        
    except paramiko.AuthenticationException:
        print("Authentication failed, please verify your credentials.")
    except paramiko.SSHException as e:
        print(f"SSH exception occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
        
        
def main(): #dbl_username, dbl_password, directory_to_search, search_date, encryption_key = None):
    default_dir = os.path.abspath(os.path.join(os.getcwd(), "../../xmls_for_dbloader_upload"))
    today = str(datetime.datetime.today().strftime('%Y-%m-%d'))
    parser = argparse.ArgumentParser(description="Script to process files in a directory.")
    parser.add_argument('-dir','--directory', type=valid_directory, default=default_dir, help="The directory to process. Default is ../../xmls_for_dbloader_upload.")
    parser.add_argument('-date', '--date', type=lambda s: str(datetime.datetime.strptime(s, '%Y-%m-%d').date()), default=today, help=f"Date for XML generated (format: YYYY-MM-DD). Default is today's date: {today}")
    parser.add_argument('-lxu', '--dbl_username', default=None, required=False, help="Username to access lxplus.")
    parser.add_argument('-lxp', '--dbl_password', default=None, required=False, help="Password to access lxplus.")
    parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
    args = parser.parse_args()

    dbl_username = args.dbl_username
    dbl_password = args.dbl_password
    directory_to_search = args.directory
    encryption_key = args.encrypt_key
    search_date = args.date

    print(f"Searching XML files in {directory_to_search} genetated on {search_date} ...")
    files_found = find_files_by_date(directory_to_search, search_date)

    if files_found:
        print("Files found: ")
        for file in files_found: print(file)
        print('\n')
        # dbl_username = input('LXPLUS Username: ')
        # dbl_password = pwinput.pwinput(prompt='LXPLUS Password: ', mask='*')
        
        build_files, other_files = get_build_files(files_found)
        print("Uploading build files ...")
        for fname in tqdm(build_files):
            scp_to_dbloader(dbl_username = dbl_username, dbl_password = dbl_password, fname = fname, encryption_key = encryption_key)

        print("Uploading other files ...")
        for fname in tqdm(other_files):
            scp_to_dbloader(dbl_username = dbl_username, dbl_password = dbl_password, fname = fname, encryption_key = encryption_key)
    else:
        print("No files found for the given date.")

if __name__ == "__main__":
    main()


