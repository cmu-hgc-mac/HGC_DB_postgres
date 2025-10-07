import platform, os, argparse, base64, subprocess
from pathlib import Path
from scp import SCPClient
from src import process_xml_list, open_scp_connection
import numpy as np
import datetime, time, yaml, paramiko, pwinput, sys, re
from tqdm import tqdm
import traceback, datetime

xml_list = process_xml_list(get_yaml_data = True)
xml_list = {t: {list(d.keys())[0]: d[list(d.keys())[0]]  for d in xml_list[t]}  for t in xml_list.keys()}

loc = 'dbase_info'
conn_yaml_file = os.path.join(loc, 'conn.yaml')
config_data  = yaml.safe_load(open(conn_yaml_file, 'r'))
mass_upload_xmls = config_data.get('mass_upload_xmls', True)
scp_persist_minutes = config_data.get('scp_persist_minutes', 240)
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
            file_type = (file_type.split('_',3)[-1]).replace('upload.xml', 'xml') ## since sensor name has extra _
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
                f"-o", f"ControlPath=~/.ssh/ctrl_dbloader",
                fname,
                f"{dbl_username}@dbloader-hgcal:/home/dbspool/spool/hgc/{cern_dbname}"]
    try:
        subprocess.run(scp_cmd, capture_output=True, text=True)
    except Exception as e:
        print(f"An error occurred for {fname}: {e}")
        # traceback.print_exc()

class mass_upload_to_dbloader:
    def __init__(self, dbl_username, fnames, cern_dbname = '', remote_xml_dir = "~/hgc_xml_temp", verbose  = False):
        self.mass_upload_logs_fp = "export_data/mass_upload_logs"
        os.makedirs(self.mass_upload_logs_fp, exist_ok=True)
        self.temp_txt_file_name = os.path.join(self.mass_upload_logs_fp, f"terminal_out.txt" )#_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.txt")
        self.terminal_output = ""
        self.starttime = datetime.datetime.now()
        self.controlpathname = "ctrl_dbloader"
        self.dbl_username = dbl_username
        self.fnames = fnames
        self.cern_dbname = cern_dbname
        self.remote_xml_dir = remote_xml_dir
        self.verbose = verbose
        
    def make_lxplus_dir(self):
        makedir_cmd = ["ssh", f"{self.dbl_username}@dbloader-hgcal", "-o", f"ProxyJump={self.dbl_username}@lxtunnel.cern.ch", f"-o", f"ControlPath=~/.ssh/{self.controlpathname}", f"mkdir -p {self.remote_xml_dir}"]
        result = subprocess.run(makedir_cmd,     text=True)
        return result.returncode

    def scp_xml_lxplus(self):
        scp_cmd = ["scp", "-C", "-o", f"ProxyJump={self.dbl_username}@lxtunnel.cern.ch", f"-o", f"ControlPath=~/.ssh/{self.controlpathname}"] + self.fnames + [f"{self.dbl_username}@dbloader-hgcal:{self.remote_xml_dir}/"]
        print(f"SCPing files to {self.dbl_username}@dbloader-hgcal:~/hgc_xml_temp ...")
        result = subprocess.run(scp_cmd,         text=True)
        return result.returncode

    def rm_xml_lxplus(self):
        remove_xml_cmd = ["ssh", "-o", f"ProxyJump={self.dbl_username}@lxtunnel.cern.ch", f"-o", f"ControlPath=~/.ssh/{self.controlpathname}", f"{self.dbl_username}@dbloader-hgcal", f"rm {self.remote_xml_dir}/*",]
        if self.verbose: print(f"Removing files from {self.dbl_username}@dbloader-hgcal:~/hgc_xml_temp ...")
        result = subprocess.run(remove_xml_cmd,  text=True, capture_output=True)
        if "No such file or directory" in result.stderr: return 0
        return result.returncode

    def mass_upload_xml_dbl(self):
        GREEN = "\033[32m"; RESET = "\033[0m"
        print(f"Uploading to dbloader-hgcal with mass_loader ... patience, please")
        print(f"{GREEN}The mass_upload terminal output sometimes says that the uploads have failed even when they have succeeded.{RESET}")
        print(f"{GREEN}The CERN team is working on fixing it.{RESET}")
        print(f"{GREEN}Check the API and the dbloader log to see if the uploads were successful until this gets fixed.{RESET}")
        print(f"=================================================================")
        with open("export_data/mass_loader.py", "r") as f:
            mass_upload_cmd = ["ssh", "-o", f"ProxyJump={self.dbl_username}@lxtunnel.cern.ch", f"-o", f"ControlPath=~/.ssh/{self.controlpathname}", f"{self.dbl_username}@dbloader-hgcal", f"python3 - --{self.cern_dbname.lower()} {self.remote_xml_dir}/*.xml"]
            with subprocess.Popen(mass_upload_cmd, stdin=f, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as process, open(self.temp_txt_file_name, "a", encoding="utf-8") as txtfile:                        
                for line in process.stdout:
                    self.terminal_output += line   # save terminal output from mass_upload to log txt file
                    txtfile.write(line)                      # save to text file immediately
                    txtfile.flush()                          # flush to disk in real time
                    if self.verbose:
                        sys.stdout.write(line)         # print live
                        sys.stdout.flush()             # force immediate display
                    elif "INFO - Found " in line and "XML files" in line:
                        sys.stdout.write(line)         # print live
                        sys.stdout.flush()             # force immediate display
                    elif "Progress: [" in line: 
                        sys.stdout.write("\r" + line.strip())  # overwrite the same line
                        sys.stdout.flush()

                process.wait()  # wait for process to finish
                print()
                print(f"=================================================================")
                
                return process.returncode  ### 0 for success, 255 for failed
            
    def scp_logs_local(self):
        if '.csv' in self.terminal_output:
            print("----> Saving log files to export_data/mass_upload_logs <----")
            csv_outfile = f"{self.terminal_output.split('.csv')[0].split(' ')[-1]}.csv"
            log_outfile = os.path.splitext(csv_outfile)[0] + ".log"
            terminal_outfile = os.path.splitext(csv_outfile)[0] + ".txt"
            os.rename(self.temp_txt_file_name, os.path.join(self.mass_upload_logs_fp, terminal_outfile))
            print(terminal_outfile)
            scp_masslog_file = ["scp", "-o", f"ProxyJump={self.dbl_username}@lxtunnel.cern.ch", "-o", f"ControlPath=~/.ssh/{self.controlpathname}", f"{self.dbl_username}@dbloader-hgcal:~/{csv_outfile}", f"{self.dbl_username}@dbloader-hgcal:~/{log_outfile}", self.mass_upload_logs_fp]
            result = subprocess.run(scp_masslog_file,     text=True)
            file_path_log, file_path_csv = os.path.join(self.mass_upload_logs_fp, os.path.basename(log_outfile)), os.path.join(self.mass_upload_logs_fp, os.path.basename(csv_outfile))
            if os.path.isfile(file_path_csv) and os.path.isfile(file_path_log):
                rm_masslog_file = ["ssh", "-o", f"ProxyJump={self.dbl_username}@lxtunnel.cern.ch", "-o", f"ControlPath=~/.ssh/{self.controlpathname}", f"{self.dbl_username}@dbloader-hgcal", f"rm ~/{csv_outfile} ~/{log_outfile}"]
                result = subprocess.run(rm_masslog_file,     text=True)
            print("")
            return result.returncode
                    
    def run_steps(self):
        ### remove any existing XML files from that directory to prevent reuploads
        steps = [self.make_lxplus_dir, self.rm_xml_lxplus, self.scp_xml_lxplus, self.mass_upload_xml_dbl, self.scp_logs_local, self.rm_xml_lxplus]

        current_step = 0
        while current_step < len(steps):
            if open_scp_connection(dbl_username=self.dbl_username, get_scp_status=True, mass_upload_xmls=mass_upload_xmls) != 0:    ### connection is missing
                print("Reconnect to LXPLUS -- preexisting connection broken -- retry this step")
                scp_status = open_scp_connection(dbl_username=self.dbl_username, scp_persist_minutes=scp_persist_minutes, scp_force_quit=False, mass_upload_xmls=mass_upload_xmls)
                continue  ### keeps requesting credentials until connection is successful
            try:
                return_status = steps[current_step]()
                if current_step in [2, 3]:
                    current_time = datetime.datetime.now()
                    print("Time elapsed:", current_time - self.starttime)
                    self.starttime = current_time
                if return_status == 0: current_step += 1  ### if current step was successful (success = 0, fail = 255), go to next step. 
            except Exception as e:
                print(f"An error occurred at step {current_step+1}: {e}")
                scp_status = open_scp_connection(dbl_username=self.dbl_username, scp_persist_minutes=scp_persist_minutes, scp_force_quit=False, mass_upload_xmls=mass_upload_xmls)        
    
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
        print(f"Uploading {len(protomodule_build_files)} protomodule 'build' files to {cern_dbname}...")

        if protomodule_build_files:
            if mass_upload_xmls:
                mass_upload_to_dbloader(dbl_username = dbl_username, fnames=protomodule_build_files, cern_dbname = cern_dbname).run_steps()
            else:
                for fname in tqdm(protomodule_build_files):
                    scp_to_dbloader(dbl_username = dbl_username, fname = fname, cern_dbname = cern_dbname)
        
            if module_build_files or other_files:
                print("Waiting 10 seconds after protomodule upload...")
                print("")
                time.sleep(10) ### DBLoader has some latency

        print(f"Uploading {len(module_build_files)} module 'build' files to {cern_dbname}...")
        if module_build_files:
            if mass_upload_xmls:
                mass_upload_to_dbloader(dbl_username = dbl_username, fnames=module_build_files, cern_dbname = cern_dbname).run_steps()
            else:
                for fname in tqdm(module_build_files):
                    scp_to_dbloader(dbl_username = dbl_username, fname = fname, cern_dbname = cern_dbname)
        
            if other_files:
                print("Waiting 10 seconds after module upload...")
                print("")
                time.sleep(10) ## DBLoader has some latency

        print(f"Uploading {len(other_files)} other files to {cern_dbname}...")
        if other_files:
            if mass_upload_xmls:
                mass_upload_to_dbloader(dbl_username = dbl_username, fnames=other_files, cern_dbname = cern_dbname).run_steps()
            else:
                for fname in tqdm(other_files):
                    scp_to_dbloader(dbl_username = dbl_username, fname = fname, cern_dbname = cern_dbname)
    else:
        print("No files found for the given date.")

if __name__ == "__main__":
    main()


