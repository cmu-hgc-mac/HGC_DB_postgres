import platform, os, argparse, base64, subprocess
from pathlib import Path
import datetime, time, yaml, paramiko, pwinput, sys, re, math, shutil
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from export_data.src import process_xml_list, open_scp_connection, str2bool, dbloader_hostname
from export_data.check_successful_upload import check_successful_upload_seq
import asyncio
import numpy as np
from tqdm import tqdm
import traceback, datetime
from scp import SCPClient
GREEN = "\033[32m"; RED = "\033[31m"; YELLOW = "\033[33m"; RESET = "\033[0m"; 

xml_list = process_xml_list(get_yaml_data = True)
xml_list = {t: {list(d.keys())[0]: d[list(d.keys())[0]]  for d in xml_list[t]}  for t in xml_list.keys()}

loc = 'dbase_info'
conn_yaml_file = os.path.join(loc, 'conn.yaml')
config_data  = yaml.safe_load(open(conn_yaml_file, 'r'))
mass_upload_xmls = config_data.get('mass_upload_xmls', True)
scp_persist_minutes = config_data.get('scp_persist_minutes', 240)
mass_upload_method = config_data.get('mass_upload_method', 'via_paramiko')
cerndb_types = {"dev_db": {'dbtype': 'Development', 'dbname': 'INT2R'}, 
                "prod_db": {'dbtype': 'Production','dbname':'CMSR'}}

def get_selected_type_files(files_found_all):
    files_selected = []
    for fi in files_found_all:
        parent_directory = str(Path(fi).parent.name)
        file_type = str(Path(fi).name)
        
        if parent_directory == 'iv' or parent_directory == 'pedestal':
            file_type = f"module_{parent_directory}_xml" if "320M" in str(fi) else f"hxb_{parent_directory}_xml"
            parent_directory = 'testing'

        for xmlt in list(xml_list[parent_directory].keys()):
            xmlt_mod = xmlt.split('_', 1)[-1].replace("_xml", "") if parent_directory != "testing" else xmlt.replace("_xml", "")
            if xml_list[parent_directory][xmlt] and xmlt_mod in file_type:
                files_selected.append(fi)
    return list(set(files_selected))

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
            if file.lower().endswith('.xml') or file.lower().endswith('.zip'):
                file_path = os.path.join(root, file)
                file_stat = os.stat(file_path)
                mod_time = datetime.date.fromtimestamp(file_stat.st_mtime)
                create_time = datetime.date.fromtimestamp(file_stat.st_ctime)
                if mod_time == target_date or create_time == target_date:
                    matched_files.append(file_path)
    return matched_files


def run_mass_upload_seq(files_for_upload, lxp_username, cern_dbname, lxp_password, cern_auto_upload, upload_instances, mass_upload_to_dbloader):
    if files_for_upload:
        if mass_upload_xmls:
            inst = mass_upload_to_dbloader(lxp_username=lxp_username, fnames=files_for_upload, cern_dbname=cern_dbname, dbloader_hostname=dbloader_hostname, lxp_password=lxp_password, cern_auto_upload=cern_auto_upload)
            inst.run_steps()
            upload_instances.append(inst)
            return os.path.join(inst.mass_upload_logs_fp, inst.csv_outfile)
        else:
            for fname in tqdm(files_for_upload):
                scp_to_dbloader(lxp_username=lxp_username, fname=fname, cern_dbname=cern_dbname, dbloader_hostname=dbloader_hostname)


def get_files_by_type(files_list, file_type = 'build'):
    type_files = []
    other_files = []
    file_type = [file_type] if type(file_type) == str else file_type
    for ft in file_type:
        for fname in files_list:
            if ft in fname.lower(): 
                type_files.append(fname)
            else:
                other_files.append(fname)
    return type_files, other_files

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
def scp_to_dbloader(lxp_username, fname, cern_dbname = '', dbloader_hostname = 'hgcaldbloader.cern.ch'):
    ## f"scp -o ProxyJump={lxp_username}@lxplus.cern.ch -o ControlPath=~/.ssh/scp-%r@%h:%p {fname} {lxp_username}@{dbloader_hostname}:/home/dbspool/spool/hgc/{cern_dbname}"
    scp_cmd = ["scp",
                f"-o", f"ProxyJump={lxp_username}@lxplus.cern.ch",
                f"-o", f"ControlPath=~/.ssh/ctrl_dbloader",
                fname,
                f"{lxp_username}@{dbloader_hostname}:/home/dbspool/spool/hgc/{cern_dbname}"]
    try:
        subprocess.run(scp_cmd, capture_output=True, text=True)
    except Exception as e:
        print(f"An error occurred for {fname}: {e}")
        # traceback.print_exc()

##########################################################################################
################ Mass upload with SSH control master method ##############################
##########################################################################################

class mass_upload_to_dbloader_via_ssh_controlmaster:
    def __init__(self, lxp_username, fnames, cern_dbname = '', remote_xml_dir = "~/hgc_xml_temp", verbose = False, dbloader_hostname = 'dbloader-hgcal', lxp_password = None, cern_auto_upload=False):
        _project_root = Path(__file__).resolve().parent.parent  ## HGC_DB_postgres/
        self.mass_upload_logs_fp = str(_project_root / "export_data" / "mass_upload_logs")
        os.makedirs(self.mass_upload_logs_fp, exist_ok=True)
        self.temp_txt_file_name = os.path.join(self.mass_upload_logs_fp, f"terminal_out.txt" )#_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.txt")
        self.terminal_output = ""
        self.verbose = verbose
        self.starttime = datetime.datetime.now()
        self.run_on_remote_fpath = str(_project_root / "export_data" / "mass_loader_modified.py")
        
        self.cern_dbname = cern_dbname
        self.dbloader_hostname = dbloader_hostname
        self.lxp_username = lxp_username
        self.fnames = fnames
        self.remote_xml_dir = remote_xml_dir
        self.csv_outfile = f"mass_upload_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        self.files_to_retry = 0
        self.log_save_time = None
        self.times_to_retry_upload = int(5+round(math.log2(len(self.fnames)))) ### Assume 50% of files will fail with each attempt. Will need log2(number of files) attempte for uploads to go through.
        self.times_to_retry_reconnect = 5

        ### Unique to ssh controlmaster method
        self.controlpathname = "ctrl_dbloader"
        self.cern_auto_upload = cern_auto_upload  
        if open_scp_connection(lxp_username=self.lxp_username, get_scp_status=True) != 0:    ### connection is missing
            scp_status = open_scp_connection(lxp_username=self.lxp_username, scp_persist_minutes=scp_persist_minutes, scp_force_quit=False, cern_auto_upload=self.cern_auto_upload)
        
    def make_lxplus_dir(self):
        makedir_cmd = ["ssh", f"{self.lxp_username}@{self.dbloader_hostname}", "-o", f"ProxyJump={self.lxp_username}@lxplus.cern.ch", f"-o", f"ControlPath=~/.ssh/{self.controlpathname}", f"mkdir -p {self.remote_xml_dir}"]
        result = subprocess.run(makedir_cmd,     text=True)
        return result.returncode

    def rm_xml_lxplus(self):
        remove_xml_cmd = ["ssh", "-o", f"ProxyJump={self.lxp_username}@lxplus.cern.ch", f"-o", f"ControlPath=~/.ssh/{self.controlpathname}", f"{self.lxp_username}@{self.dbloader_hostname}", f"rm -f {self.remote_xml_dir}/*.xml {self.remote_xml_dir}/*.zip",]
        if self.verbose: print(f"Removing files from {self.lxp_username}@{self.dbloader_hostname}:~/hgc_xml_temp ...")
        result = subprocess.run(remove_xml_cmd,  text=True, capture_output=True)
        if "No such file or directory" in result.stderr: return 0
        return result.returncode
    
    def scp_xml_lxplus(self):
        scp_cmd = ["scp", "-C", "-o", f"ProxyJump={self.lxp_username}@lxplus.cern.ch", f"-o", f"ControlPath=~/.ssh/{self.controlpathname}"] + self.fnames + [f"{self.lxp_username}@{self.dbloader_hostname}:{self.remote_xml_dir}/"]
        print(f"SCPing files to {self.lxp_username}@{self.dbloader_hostname}:~/hgc_xml_temp ...")
        result = subprocess.run(scp_cmd,         text=True)
        return result.returncode

    def mass_upload_xml_dbl(self):
        print(f"Uploading to {self.dbloader_hostname} with mass_loader ... patience, please")
        print("="*65)
        with open(self.run_on_remote_fpath, "r") as massloadfile:
            mass_upload_cmd = ["ssh", "-o", f"ProxyJump={self.lxp_username}@lxplus.cern.ch", f"-o", f"ControlPath=~/.ssh/{self.controlpathname}", f"{self.lxp_username}@{self.dbloader_hostname}", f"python3 - --{self.cern_dbname.lower()} {self.remote_xml_dir}/*.xml {self.remote_xml_dir}/*.zip -t 15 -c 5 -s {self.csv_outfile} -d"]
            with subprocess.Popen(mass_upload_cmd, stdin=massloadfile, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as process, open(self.temp_txt_file_name, "a", encoding="utf-8") as txtfile:                        
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
                        self.files_to_retry = int(line.strip().split('Timeout:')[-1].split(')')[0])  ## Example: 2025-10-13 16:39:01,347 - INFO - Progress: [41/41] (Success: 0, Already: 26, Failed: 7, Timeout: 8)
                        sys.stdout.write("\r" + f"{YELLOW}{line.strip()}{RESET}")  # overwrite the same line
                        sys.stdout.flush()

                process.wait()  # wait for process to finish
                print()
                print("="*65)
                if '.csv' in self.terminal_output:
                    self.csv_outfile = f"{self.terminal_output.split('.csv')[0].split(' ')[-1]}.csv"
                return process.returncode  ### 0 for success, 255 for failed
            
    def check_upload_xml_dbl(self):
        print("="*65)
        _check_upload_script = str(Path(__file__).resolve().parent / "check_upload_xml_logs.py")
        with open(_check_upload_script, "r") as checkuploadfile:
            check_upload_cmd = ["ssh", "-o", f"ProxyJump={self.lxp_username}@lxplus.cern.ch", f"-o", f"ControlPath=~/.ssh/{self.controlpathname}", f"{self.lxp_username}@{self.dbloader_hostname}", f"python3 - -lfp ~/{self.csv_outfile}"]
            with subprocess.Popen(check_upload_cmd, stdin=checkuploadfile, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as process, open(self.temp_txt_file_name, "a", encoding="utf-8") as txtfile: 
                for line in process.stdout:
                    self.terminal_output += line   # save terminal output from mass_upload to log txt file
                    txtfile.write(line)                      # save to text file immediately
                    txtfile.flush()                          # flush to disk in real time
                    # if not "Success" in line or "Already existis" in line:
                    sys.stdout.write(line)         # print live
                    sys.stdout.flush()             # force immediate display
                    if 'db_failure' in line:
                        self.files_to_retry = int(line.split('-')[-1])
                process.wait()  # wait for process to finish
                print()
                print("="*65)
                return process.returncode  ### 0 for success, 255 for failed
            
    def scp_logs_local(self):
        if '.csv' in self.terminal_output:
            self.log_save_time = datetime.datetime.now()
            print("----> Saving log files to export_data/mass_upload_logs <----")
            print(self.csv_outfile)
            log_outfile = os.path.splitext(self.csv_outfile)[0] + ".log"
            terminal_outfile = os.path.splitext(self.csv_outfile)[0] + ".txt"
            local_terminal_path = os.path.join(self.mass_upload_logs_fp, terminal_outfile)
            shutil.move(self.temp_txt_file_name, local_terminal_path)
            scp_masslog_file = ["scp", "-o", f"ProxyJump={self.lxp_username}@lxplus.cern.ch", "-o", f"ControlPath=~/.ssh/{self.controlpathname}", f"{self.lxp_username}@{self.dbloader_hostname}:~/{self.csv_outfile}", self.mass_upload_logs_fp ] #f"{self.lxp_username}@{self.dbloader_hostname}:~/{log_outfile}", self.mass_upload_logs_fp]
            result = subprocess.run(scp_masslog_file,     text=True)
            local_log_path, local_csv_path = os.path.join(self.mass_upload_logs_fp, os.path.basename(log_outfile)), os.path.join(self.mass_upload_logs_fp, os.path.basename(self.csv_outfile))
            self.csv_outfile = local_csv_path  # update to full local path for downstream use
            if os.path.isfile(local_csv_path): # and os.path.isfile(local_log_path):
                rm_masslog_file = ["ssh", "-o", f"ProxyJump={self.lxp_username}@lxplus.cern.ch", "-o", f"ControlPath=~/.ssh/{self.controlpathname}", f"{self.lxp_username}@{self.dbloader_hostname}", f"rm -f ~/{self.csv_outfile} ~/mass_loader*.log"]
                result = subprocess.run(rm_masslog_file,     text=True)
            print("")
            return result.returncode
                    
    def run_steps(self):
        ### remove any existing XML files from that directory to prevent reuploads
        steps = [self.make_lxplus_dir, self.rm_xml_lxplus, self.scp_xml_lxplus, self.mass_upload_xml_dbl, self.check_upload_xml_dbl, self.scp_logs_local, self.rm_xml_lxplus]

        current_step = 0
        while (current_step < len(steps)) and (self.times_to_retry_reconnect != 0):
            if open_scp_connection(lxp_username=self.lxp_username, get_scp_status=True) != 0:    ### connection is missing
                print("Reconnect to LXPLUS -- preexisting connection broken -- retry this step")
                scp_status = open_scp_connection(lxp_username=self.lxp_username, scp_persist_minutes=scp_persist_minutes, scp_force_quit=False, cern_auto_upload=self.cern_auto_upload)
                self.times_to_retry_reconnect -= 1
                continue  ### keeps requesting credentials until connection is successful
            try:
                return_status = steps[current_step]()
                if current_step in [2, 3]:
                    current_time = datetime.datetime.now()
                    print("Time elapsed:", current_time - self.starttime)
                    self.starttime = current_time
                if current_step == 3: ## mass_upload_xml_dbl
                    if self.files_to_retry > 0 and self.times_to_retry_upload > 0 and return_status == 0:
                        print(f"Reattempting the {self.files_to_retry} timed-out file(s) -- ({self.times_to_retry_upload-1} reattempt(s) left) ")
                        self.times_to_retry_upload -= 1 ### attempt only upto 5 times
                        continue ### repeat current step
                if return_status == 0: 
                    current_step += 1  ### if current step was successful (success = 0, fail = 255), go to next step. 
                else:
                    self.times_to_retry_reconnect -= 1  ### prevent infinite loop in case of error    

            except Exception as e:
                print(f"An error occurred at step {current_step+1}: {e}")
                self.times_to_retry_reconnect -= 1  ### prevent infinite loop in case of error 
    
##########################################################################################
################################# Mass upload with Paramiko ##############################
##########################################################################################

class mass_upload_to_dbloader_via_paramiko:
    def __init__(self, lxp_username, fnames, cern_dbname = '', remote_xml_dir = "~/hgc_xml_temp", verbose = False, dbloader_hostname = 'dbloader-hgcal', lxp_password = None, cern_auto_upload=False):
        _project_root = Path(__file__).resolve().parent.parent  ## HGC_DB_postgres/
        self.mass_upload_logs_fp = str(_project_root / "export_data" / "mass_upload_logs")
        os.makedirs(self.mass_upload_logs_fp, exist_ok=True)
        self.temp_txt_file_name = os.path.join(self.mass_upload_logs_fp, f"terminal_out.txt" )
        self.terminal_output = ""
        self.verbose = verbose
        self.starttime = datetime.datetime.now()
        self.run_on_remote_fpath = str(_project_root / "export_data" / "mass_loader_modified.py")
        
        self.cern_dbname = cern_dbname
        self.dbloader_hostname = dbloader_hostname
        self.lxp_username = lxp_username
        self.fnames = fnames
        self.remote_xml_dir = remote_xml_dir
        self.csv_outfile = f"mass_upload_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        self.files_to_retry = 0  ### what is the best way to use this?
        self.log_save_time = None
        self.times_to_retry_upload = int(5+round(math.log2(len(self.fnames)))) ### Assume 50% of files will fail with each attempt. Will need log2(number of files) attempte for uploads to go through.
        self.times_to_retry_reconnect = 5

        ### Unique to paramiko method:
        self.lxp_password = lxp_password
        self.ssh_server1 = None
        self.ssh_server2 = None
        self.ssh_conn = None
        self.connect()
        time.sleep(5) ## wait for the connection to happen
        
    def connect(self):
        self.ssh_server1 = paramiko.SSHClient()
        self.ssh_server1.load_system_host_keys()
        self.ssh_server1.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            self.ssh_server1.connect(hostname='lxplus.cern.ch', username=self.lxp_username, password=self.lxp_password)
            transport = self.ssh_server1.get_transport()
            dest_addr = (self.dbloader_hostname, 22)  
            local_addr = ('127.0.0.1', 22) # localhost
            channel = transport.open_channel("direct-tcpip", dest_addr, local_addr)
            self.ssh_server2 = paramiko.SSHClient()
            self.ssh_server2.load_system_host_keys()
            self.ssh_server2.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_server2.connect(hostname=self.dbloader_hostname, username=self.lxp_username, password=self.lxp_password, sock=channel)
            self.ssh_conn = self.ssh_server2.get_transport()
            return 0  ## success
        
        except paramiko.AuthenticationException:
            print("Authentication failed, please verify your credentials.")
        except paramiko.SSHException as e:
            print(f"SSH exception occurred: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")
        return 255

    def close_scp(self):
        self.ssh_conn.close()
        self.ssh_server2.close()
        self.ssh_server1.close()
        return 0

    def make_lxplus_dir(self):
        stdin, stdout, stderr = self.ssh_server2.exec_command(f"mkdir -p {self.remote_xml_dir}")
        result = stdout.channel.recv_exit_status()
        return result

    def rm_xml_lxplus(self):
        stdin, stdout, stderr = self.ssh_server2.exec_command(f"rm -f {self.remote_xml_dir}/*.xml {self.remote_xml_dir}/*.zip")
        if self.verbose: print(f"Removing files from {self.lxp_username}@{self.dbloader_hostname}:~/hgc_xml_temp ...")
        result = stdout.channel.recv_exit_status()
        return result

    def scp_xml_lxplus(self):
        with SCPClient(self.ssh_conn) as scp:
            try:
                for fname in self.fnames[:]:  #iterate over a copy
                    scp.put(fname, f'~/hgc_xml_temp/')
                    self.fnames.remove(fname)   ### remove file from the list
                return 0   ### success if all files uploaded
            except Exception as e:
                print(f"Upload to LXPLUS failed {fname}", e)
                return 255

    def mass_upload_xml_dbl(self):
        print(f"Uploading to {self.dbloader_hostname} with mass_loader ... patience, please")
        print("="*65)
        with open(self.run_on_remote_fpath, "r") as massloadfile, open(self.temp_txt_file_name, "a", encoding="utf-8") as txtfile:
            script = massloadfile.read()
            command = f"python3 - --{self.cern_dbname.lower()} {self.remote_xml_dir}/*.xml {self.remote_xml_dir}/*.zip -t 15 -c 5 -s {self.csv_outfile} -d"
            stdin, stdout, stderr = self.ssh_server2.exec_command(command)
            stdin.write(script)
            stdin.channel.shutdown_write()  # signal EOF
            self.files_to_retry = 0

            for line in iter(stdout.readline, ""):
                self.terminal_output += line  # save terminal output from mass_upload to log txt file
                txtfile.write(line); txtfile.flush()     # save to text file immediately; flush to disk in real time
                if self.verbose:
                    sys.stdout.write(line); sys.stdout.flush()   # print live; force immediate display
                    sys.stdout.flush()      # 
                elif "INFO - Found " in line and "XML files" in line:
                    sys.stdout.write(line); sys.stdout.flush()   # print live; force immediate display
                elif "Progress: [" in line:
                    try:
                        self.files_to_retry = int(line.strip().split("Timeout:")[-1].split(")")[0])  ## Example: 2025-10-13 16:39:01,347 - INFO - Progress: [41/41] (Success: 0, Already: 26, Failed: 7, Timeout: 8)
                    except Exception:
                        pass
                    sys.stdout.write("\r" + line.strip())   # overwrite the same line
                    sys.stdout.flush()

            err_text = stderr.read().decode()  # Read stderr fully (if any additional messages)
            if err_text:
                self.terminal_output += err_text
                txtfile.write(err_text); txtfile.flush()
                sys.stderr.write(err_text); sys.stderr.flush()

            if ".csv" in self.terminal_output:
                self.csv_outfile = f"{self.terminal_output.split('.csv')[0].split(' ')[-1]}.csv"
            exit_status = stdout.channel.recv_exit_status()

        print(); print("="*65)
        return exit_status


    def check_upload_xml_dbl(self):
        print("="*65)
        with open("export_data/check_upload_xml_logs.py", "r") as checkuploadfile, \
            open(self.temp_txt_file_name, "a", encoding="utf-8") as txtfile:

            python_code = checkuploadfile.read()
            stdin, stdout, stderr = self.ssh_server2.exec_command(f"python3 - -lfp ~/{self.csv_outfile}")
            stdin.write(python_code)
            stdin.channel.shutdown_write()  # Important: signal EOF to remote Python
            self.files_to_retry = 0

            for line in iter(stdout.readline, ""):
                self.terminal_output += line
                txtfile.write(line)
                txtfile.flush()
                sys.stdout.write(line)
                sys.stdout.flush()
                if "db_failure" in line: self.files_to_retry = int(line.split("-")[-1])

            exit_status = stdout.channel.recv_exit_status()  # Wait for the command to finish and get return code
            
            print()
            print("="*65)
            return exit_status
    
    def scp_logs_local(self):
        if ".csv" in self.terminal_output:
            try:
                self.log_save_time = datetime.datetime.now()
                print("----> Saving log files to export_data/mass_upload_logs <----")
                print(self.csv_outfile)
                log_outfile = os.path.splitext(self.csv_outfile)[0] + ".log"
                terminal_outfile = os.path.splitext(self.csv_outfile)[0] + ".txt"
                local_terminal_path = os.path.join(self.mass_upload_logs_fp, terminal_outfile)
                shutil.move(self.temp_txt_file_name, local_terminal_path)
                local_log_path, local_csv_path = os.path.join(self.mass_upload_logs_fp, os.path.basename(log_outfile)), os.path.join(self.mass_upload_logs_fp, os.path.basename(self.csv_outfile))
                sftp = self.ssh_server2.open_sftp()
                sftp.get(self.csv_outfile, local_csv_path)              # Copy remote CSV to local
                # sftp.get(log_outfile, local_log_path)
            except Exception as e:
                print(e)
                return 255  # indicate failure

            if os.path.isfile(local_csv_path) and os.path.isfile(local_terminal_path):
                try:
                    sftp.remove(self.csv_outfile)  # Remove remote CSV after download
                    stdin, stdout, stderr = self.ssh_server2.exec_command(f"rm -f ~/mass_loader*.log")
                    # sftp.remove(log_outfile)  # Remove remote log after download
                except FileNotFoundError:
                    return 255
            print("")
            return 0  # success

    def run_steps(self):
        ### remove any existing XML files from that directory to prevent reuploads
        steps = [self.make_lxplus_dir, self.rm_xml_lxplus, self.scp_xml_lxplus, self.mass_upload_xml_dbl, self.check_upload_xml_dbl, self.scp_logs_local, self.rm_xml_lxplus]
        current_step = 0
        while (current_step < len(steps)) and (self.times_to_retry_reconnect != 0):
            connection_missing = (self.ssh_server2 is None) or (self.ssh_conn is None) or (not self.ssh_conn.is_active())
            if connection_missing:
                print("Reconnect to LXPLUS -- preexisting connection broken -- retry this step")
                self.connect()
                self.times_to_retry_reconnect -= 1
                continue  ### keeps requesting credentials until connection is successful
            try:
                return_status = steps[current_step]()
                if current_step in [2, 3]:
                    current_time = datetime.datetime.now()
                    print("Time elapsed:", current_time - self.starttime)
                    self.starttime = current_time
                if current_step == 3: ## mass_upload_xml_dbl
                    if self.files_to_retry > 0 and self.times_to_retry_upload > 0 and return_status == 0:
                        print(f"Reattempting the {self.files_to_retry} timed-out file(s) -- ({self.times_to_retry_upload-1} reattempt(s) left) ")
                        self.times_to_retry_upload -= 1 ### attempt only upto n times
                        continue ### repeat current step
                if return_status == 0: 
                    current_step += 1  ### if current step was successful (success = 0, fail = 255), go to next step. 
                else:
                    self.times_to_retry_reconnect -= 1  ### prevent infinite loop in case of error    

            except Exception as e:
                self.times_to_retry_reconnect -= 1  ### prevent infinite loop in case of error    
                print(f"An error occurred at step {current_step+1}: {e}")
        
        self.close_scp()

###################################################################################################
################################## Consolidate Logs ###############################################
###################################################################################################

def consolidate_mass_upload_logs(instances):
    """Combine csv and txt log files from multiple mass_upload instances into single files.

    Output filename: mass_upload_YYYYMMDD_HHMMSS_HHMMSS_... where YYYYMMDD is from the
    first instance's start time and each HHMMSS corresponds to each instance's start time.
    """
    if not instances:
        return

    logs_fp = instances[0].mass_upload_logs_fp
    time_parts = [(inst.log_save_time or inst.starttime).strftime("%H%M%S") for inst in instances]
    date_part  = (instances[0].log_save_time or instances[0].starttime).strftime("%Y%m%d")
    base_name  = f"mass_upload_{date_part}_" + "_".join(time_parts)

    combined_csv_path = os.path.join(logs_fp, base_name + ".csv")
    combined_txt_path = os.path.join(logs_fp, base_name + ".txt")

    # --- Combine CSVs (keep header from first file, skip headers in rest) ---
    header_written = False
    with open(combined_csv_path, "w", encoding="utf-8") as out_csv:
        for inst in instances:
            csv_name = os.path.basename(inst.csv_outfile)
            local_csv = os.path.join(logs_fp, csv_name)
            if not os.path.isfile(local_csv):
                continue
            with open(local_csv, "r", encoding="utf-8") as in_csv:
                for i, line in enumerate(in_csv):
                    if i == 0:  # header row
                        if not header_written:
                            out_csv.write(line)
                            header_written = True
                    else:
                        out_csv.write(line)

    # --- Combine TXTs (append sequentially with a separator) ---
    with open(combined_txt_path, "w", encoding="utf-8") as out_txt:
        for idx, inst in enumerate(instances):
            csv_stem = os.path.splitext(os.path.basename(inst.csv_outfile))[0]
            local_txt = os.path.join(logs_fp, csv_stem + ".txt")
            out_txt.write(f"{'='*65}\n")
            out_txt.write(f"=== Upload {idx+1} of {len(instances)}: {csv_stem} ===\n")
            out_txt.write(f"{'='*65}\n")
            if os.path.isfile(local_txt):
                with open(local_txt, "r", encoding="utf-8") as in_txt:
                    out_txt.write(in_txt.read())
            else:
                out_txt.write(f"(log file not found: {local_txt})\n")
            out_txt.write("\n")

    # --- Remove original per-instance files ---
    for inst in instances:
        csv_name = os.path.basename(inst.csv_outfile)
        local_csv = os.path.join(logs_fp, csv_name)
        csv_stem = os.path.splitext(csv_name)[0]
        local_txt = os.path.join(logs_fp, csv_stem + ".txt")
        for f in [local_csv, local_txt]:
            if os.path.isfile(f):
                os.remove(f)

    print(f"----> Consolidated logs saved:")
    print(f"      {combined_csv_path}")
    print(f"      {combined_txt_path}")


###################################################################################################
################################## Main Function ##################################################
###################################################################################################

def main():
    GENERATED_XMLS_DIR = str(Path(__file__).resolve().parent / "xmls_for_upload")
    today = str(datetime.datetime.today().strftime('%Y-%m-%d'))
    parser = argparse.ArgumentParser(description="Script to process files in a directory.")
    parser.add_argument('-dir','--directory', type=valid_directory, default=GENERATED_XMLS_DIR, help=f"The directory to process. Default is {GENERATED_XMLS_DIR}.")
    parser.add_argument('-date', '--date', type=lambda s: str(datetime.datetime.strptime(s, '%Y-%m-%d').date()), default=today, help=f"Date for XML generated (format: YYYY-MM-DD). Default is today's date: {today}")
    parser.add_argument('-lxu', '--lxp_username', default=None, required=False, help="Username to access lxplus.")
    parser.add_argument('-cerndb', '--cern_dbase', default='dev_db', required=False, help="Name of cern db to upload to - dev_db/prod_db.")
    parser.add_argument('-autoupload', '--cern_auto_upload', default='False', required=False, help="True if the upload is automated via a service account")
    parser.add_argument('-dbp', '--dbpassword', default=None, required=False, help="Password to access database.")
    parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
    parser.add_argument('-delx', '--del_xml', default='True', required=False, help="Delete XMLs after upload.")
    args = parser.parse_args()

    lxp_username = args.lxp_username
    directory_to_search = args.directory
    search_date = args.date
    cern_auto_upload = str2bool(args.cern_auto_upload)
    dbpassword = args.dbpassword or pwinput.pwinput(prompt='Enter database shipper password: ', mask='*')
    encryption_key = args.encrypt_key
    clean_success_xml = str2bool(args.del_xml)
    cern_dbname = (cerndb_types[args.cern_dbase]['dbname']).lower() ## 'int2r' or 'cmsr'
    lxp_password = None  ## default
    mass_upload_to_dbloader = mass_upload_to_dbloader_via_ssh_controlmaster  ## default for user guided

    if cern_auto_upload:
        from task_scheduler.scheduler_helper import get_lxplus_username_password
        lxp_username, lxp_password = get_lxplus_username_password()
        mass_upload_methods = {"via_ssh_controlmaster": mass_upload_to_dbloader_via_ssh_controlmaster,
                            "via_paramiko": mass_upload_to_dbloader_via_paramiko,}
        mass_upload_to_dbloader = mass_upload_methods[mass_upload_method]

    print(f"Searching XML files in {directory_to_search} genetated on {search_date} ...")
    files_found_all = find_files_by_date(directory_to_search, search_date)
    files_found = get_selected_type_files(files_found_all)

    if files_found:
        print("Files found: ")
        for file in files_found:
            print(file)
        print('\n')
        build_files, other_files = get_files_by_type(files_found, file_type='build')
        cond_files,  other_files = get_files_by_type(other_files, file_type=['cond', 'wirebond', 'assembly', 'inspect'])
        protomodule_build_files, module_build_files, other_build_files = get_proto_module_files(build_files)

        upload_instances = []
        upload_kwargs = dict(lxp_username=lxp_username, cern_dbname=cern_dbname, lxp_password=lxp_password, cern_auto_upload=cern_auto_upload, upload_instances=upload_instances, mass_upload_to_dbloader=mass_upload_to_dbloader)
        upload_file_types = [protomodule_build_files, module_build_files, cond_files, other_files]
        upload_file_type_names = ['protomodule build', 'module build', 'cond', 'other']
        wait_after = [10, 10, 5, 0]  ## seconds to wait after each batch before the next (DBLoader latency)

        for files, name, wait in zip(upload_file_types, upload_file_type_names, wait_after):
            try:
                print(f"Uploading {len(files)} {name} files to {cern_dbname}...")
                csv_outfile = run_mass_upload_seq(files, **upload_kwargs)
                if csv_outfile and dbpassword:
                    asyncio.run(check_successful_upload_seq(dbpassword=dbpassword, db_type=cern_dbname, encryption_key=encryption_key, consolidated_csv=csv_outfile, clean_success_xml=clean_success_xml))
                remaining = [f for f in upload_file_types[upload_file_types.index(files)+1:] if f]
                if files and remaining and wait:
                    print(f"Waiting {wait} seconds after {name} upload...")
                    print("")
                    time.sleep(wait)
            except Exception as e:
                print('Error', e)

        if len(upload_instances) >= 1:
            consolidate_mass_upload_logs(upload_instances)

        try:
            if cern_auto_upload and open_scp_connection(lxp_username=lxp_username, get_scp_status=True) == 0:
                scp_status = open_scp_connection(lxp_username=lxp_username, scp_force_quit=True)
        except Exception as e:
            print("I don't know what is going on!!!", e)
    else:
        print("No files found for the given date.")

if __name__ == "__main__":
    main()


