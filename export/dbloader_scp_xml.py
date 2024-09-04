import platform, os, datetime, pwinput
import numpy as np
if platform.system() == 'Windows':
    import wexpect as pexpect
else:
    import pexpect


def find_files_by_date(directory, target_date):
    target_date = datetime.datetime.strptime(target_date, '%Y-%m-%d').date()
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
    scp_command = f'scp -o ProxyJump={dbl_username}@lxplus.cern.ch {fname} {dbl_username}@dbloader-hgcal:/home/dbspool/spool/hgc/int2r/'
    child = pexpect.spawn(scp_command)
    logfile = open('scp_output.log', 'wb')  
    child.logfile = logfile  
    index = child.expect(['Password: ', pexpect.EOF, pexpect.TIMEOUT])
    if index == 0:
        child.sendline(dbl_password)
        index = child.expect(['Password: ', pexpect.EOF, pexpect.TIMEOUT])
        if index == 0:
            child.sendline(dbl_password)
            index = child.expect([pexpect.EOF, pexpect.TIMEOUT])
            if index == 0:
                print(child.before.decode('utf-8'))    
    if index != 0:
        print('\n')
        print("Upload Unsuccessful :( ")
        print("Troubleshooting tips --")
        print("(1) Check username and password.")
        print("(2) Check if SSH key is in known_hosts. If not, add with")
        print("\t'ssh-keyscan lxplus.cern.ch >> ~/.ssh/known_hosts' in local terminal")
        print("\t'ssh-keyscan dbloader-hgcal >> ~/.ssh/known_hosts' in lxplus")
        print("(3) Check SCP command and output:")
        print(f"\t'{scp_command}'\n")
        print('WARNING!!! YOUR PASSWORD WILL BE VISIBLE!!! in the terminal output log.')
        see_log = input('Do you want to see the terminal output log (yes/no)? ')
        if 'y' in see_log.lower():
            print('\n')
            print('******** scp output log ********')
            with open('scp_output.log', 'r') as log:
                print(log.read())
        else:
            print("Try the above command in a terminal to see output.")


def main():
    directory_to_search = '.'
    search_date = input("Input date in YYYY-MM-DD: ")
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
