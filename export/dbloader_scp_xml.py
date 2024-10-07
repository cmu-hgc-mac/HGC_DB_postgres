import platform, os, argparse, paramiko, pwinput
from scp import SCPClient
import numpy as np
import datetime


def valid_directory(path):
    if os.path.isdir(path):
        return path
    else:
        raise argparse.ArgumentTypeError(f"Invalid directory: {path}")

def find_files_by_date(directory, target_date):
    # target_date = datetime.datetime.strptime(target_date, '%Y-%m-%d').date()
    matched_files = []
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


def scp_to_dbloader(dbl_username, dbl_password, fname):
    ssh_server1 = paramiko.SSHClient()
    ssh_server1.load_system_host_keys()
    ssh_server1.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh_server1.connect(hostname='lxplus.cern.ch', username=dbl_username, password=dbl_password)
        transport = ssh_server1.get_transport()
        dest_addr = ('dbloader-hgcal', 22)  
        local_addr = ('127.0.0.1', 22) # localhost
        channel = transport.open_channel("direct-tcpip", dest_addr, local_addr)
        ssh_server2 = paramiko.SSHClient()
        ssh_server2.load_system_host_keys()
        ssh_server2.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_server2.connect(hostname='dbloader-hgcal', username=dbl_username, password=dbl_password, sock=channel)

        with SCPClient(ssh_server2.get_transport()) as scp:
            scp.put(fname, '/home/dbspool/spool/hgc/int2r/')

        scp.close()
        ssh_server2.close()
        ssh_server1.close()    
        
    except paramiko.AuthenticationException:
        print("Authentication failed, please verify your credentials.")
    except paramiko.SSHException as e:
        print(f"SSH exception occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
        
        
def main():
    default_dir = os.path.abspath(os.path.join(os.getcwd(), "../../xmls_for_dbloader_upload"))
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    parser = argparse.ArgumentParser(description="Script to process files in a directory.")
    parser.add_argument('-dir','--directory', type=valid_directory, default=default_dir, help="The directory to process. Default is ../../xmls_for_dbloader_upload.")
    parser.add_argument('-date', '--date', type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d').date(), default=today, help=f"Date for XML generated (format: YYYY-MM-DD). Default is today's date: {today}")
    args = parser.parse_args()

    directory_to_search = args.directory
    search_date = args.date
    print(f"Searching XML files in {directory_to_search} genetated on {search_date} ...")
    files_found = find_files_by_date(directory_to_search, search_date)

    if files_found:
        print("Files found: ")
        for file in files_found: print(file)

        print('\n')
        dbl_username = input('LXPLUS Username: ')
        dbl_password = pwinput.pwinput(prompt='LXPLUS Password: ', mask='*')
        
        build_files, other_files = get_build_files(files_found)
        for fname in build_files:
            scp_to_dbloader(dbl_username, dbl_password, fname)
        for fname in other_files:
            scp_to_dbloader(dbl_username, dbl_password, fname)
    else:
        print("No files found for the given date.")


main()
