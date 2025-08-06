import platform, os, argparse, base64, subprocess
from pathlib import Path
from scp import SCPClient
from src import process_xml_list
import numpy as np
import datetime, yaml, paramiko, pwinput, sys
from tqdm import tqdm
import traceback

xml_list = process_xml_list(get_yaml_data = True)
xml_list = {t: {list(d.keys())[0]: d[list(d.keys())[0]]  for d in xml_list[t]}  for t in xml_list.keys()}

loc = 'dbase_info'
conn_yaml_file = os.path.join(loc, 'conn.yaml')
# cern_dbase  = yaml.safe_load(open(conn_yaml_file, 'r')).get('cern_db')
# cern_dbase  = 'dev_db'## for testing purpose, otherwise uncomment above.
cerndb_types = {"dev_db": {'dbtype': 'Development', 'dbname': 'INT2R'}, 
                "prod_db": {'dbtype': 'Production','dbname':'CMSR'}}

def get_selected_type_files(files_found_all):
    files_selected = []
    for fi in files_found_all:
        parent_directory = str(Path(fi).parent.name)
        file_type = str(Path(fi).name)

        if parent_directory == 'sensor':
            file_type = file_type.split('_',2)[2] ## since sensor name has extra _
        elif parent_directory == 'iv' or parent_directory == 'pedestal':
            # file_type = 'module' + (file_type[file_type.index('_'+ parent_directory):]).replace('.xml', '')
            file_type = f"module_{parent_directory}_xml" if "320M" in str(fi) else f"hxb_{parent_directory}_xml"
            parent_directory = 'testing'
        else:
            parent_directory, file_type = str(Path(fi).parent.name) , str(Path(fi).name).replace('upload.xml', 'xml').split('_',1)[1]
        
        for xmlt in list(xml_list[parent_directory].keys()):
            if xml_list[parent_directory][xmlt] and file_type in xmlt:
                files_selected.append(fi)
    return files_selected

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

def get_proto_module_files(files_list):
    protomodule_files, module_files, other_files = [],[],[]
    for fname in files_list:
        if 'protomodule' in fname.lower(): 
            protomodule_files.append(fname)
        elif 'module' in fname.lower(): 
            module_files.append(fname)
        else:
            other_files.append(fname)
    return protomodule_files, module_files, other_files


## https://security.web.cern.ch/recommendations/en/ssh_tunneling.shtml
## https://cern.service-now.com/service-portal?id=kb_article&n=KB0008504
def scp_to_dbloader(dbl_username, fname, cern_dbname = ''):
    ## f"scp -o ProxyJump={dbl_username}@lxtunnel.cern.ch -o ControlPath=~/.ssh/scp-%r@%h:%p {fname} {dbl_username}@dbloader-hgcal:/home/dbspool/spool/hgc/{cern_dbname}"
    scp_cmd = ["scp",
                f"-o", f"ProxyJump={dbl_username}@lxtunnel.cern.ch",
                f"-o", f"ControlPath=~/.ssh/scp-%r@%h:%p",
                fname,
                f"{dbl_username}@dbloader-hgcal:/home/dbspool/spool/hgc/{cern_dbname}"]
    try:
        subprocess.run(scp_cmd, capture_output=True, text=True)
    except Exception as e:
        print(f"An error occurred for {fname}: {e}")
        # traceback.print_exc()
        
        
def main():
    default_dir = os.path.abspath(os.path.join(os.getcwd(), "../../xmls_for_dbloader_upload"))
    today = str(datetime.datetime.today().strftime('%Y-%m-%d'))
    parser = argparse.ArgumentParser(description="Script to process files in a directory.")
    parser.add_argument('-dir','--directory', type=valid_directory, default=default_dir, help="The directory to process. Default is ../../xmls_for_dbloader_upload.")
    parser.add_argument('-date', '--date', type=lambda s: str(datetime.datetime.strptime(s, '%Y-%m-%d').date()), default=today, help=f"Date for XML generated (format: YYYY-MM-DD). Default is today's date: {today}")
    parser.add_argument('-lxu', '--dbl_username', default=None, required=False, help="Username to access lxplus.")
    parser.add_argument('-cerndb', '--cern_dbase', default='dev_db', required=False, help="Name of cern db to upload to - dev_db/prod_db.")
    args = parser.parse_args()

    dbl_username = args.dbl_username
    directory_to_search = args.directory
    search_date = args.date

    print(f"Searching XML files in {directory_to_search} genetated on {search_date} ...")
    files_found_all = find_files_by_date(directory_to_search, search_date)
    files_found = get_selected_type_files(files_found_all)

    if files_found:
        print("Files found: ")
        for file in files_found: print(file)
        print('\n')
        build_files, other_files = get_build_files(files_found)
        protomodule_build_files, module_build_files, other_build_files = get_proto_module_files(build_files)
        cern_dbname = (cerndb_types[args.cern_dbase]['dbname']).lower()
        print(f"Uploading protomodule 'build' files to {cern_dbname}...")
        for fname in tqdm(protomodule_build_files):
            scp_to_dbloader(dbl_username = dbl_username, fname = fname, cern_dbname = cern_dbname)
        print(f"Uploading module 'build' files to {cern_dbname}...")
        for fname in tqdm(module_build_files):
            scp_to_dbloader(dbl_username = dbl_username, fname = fname, cern_dbname = cern_dbname)
        print(f"Uploading other files to {cern_dbname}...")
        for fname in tqdm(other_files):
            scp_to_dbloader(dbl_username = dbl_username, fname = fname, cern_dbname = cern_dbname)
    else:
        print("No files found for the given date.")

if __name__ == "__main__":
    main()


