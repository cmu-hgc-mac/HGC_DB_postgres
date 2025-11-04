import subprocess, os, glob, platform, webbrowser, traceback

mac_dict_lxplus = { 'CMU' : {'host': 'cmsmac04.phys.cmu.edu',   'database':'hgcdb',      'forwardport': '15432'}, 
                    'UCSB': {'host': 'gut.physics.ucsb.edu',    'database':'hgcdb',     'forwardport': '15433'}, 
                    'TIFR': {'host': 'lxhgcdb02.tifr.res.in',   'database':'hgcdb',     'password': 'hgcal', 'forwardport': '15434'},
                    'IHEP': {'host': 'hgcal.ihep.ac.cn',        'database':'postgres', 'forwardport': '15435'},
                    'NTU' : {'host': 'hep11.phys.ntu.edu.tw',   'database':'hgcdb',     'forwardport': '15436'}, }

def open_ssh_connection(dbl_username = None, scp_persist_minutes = 240, scp_force_quit = False, get_scp_status = False):
    controlpathname = "ctrlpath_lxtunnel_postgres"
    test_cmd = ["ssh", 
                "-o", f"ControlPath=~/.ssh/{controlpathname}",
                "-O", "check",     # <-- ask the master process if it’s alive
                f"{dbl_username}@{controlpathname}"]
    if get_scp_status:
        result = subprocess.run(test_cmd, capture_output=True, text=True)
        return result.returncode

    if scp_force_quit:
        quit_cmd = ["ssh", "-O", "exit",
                    "-o", f"ControlPath=~/.ssh/{controlpathname}", f"{dbl_username}@{controlpathname}"]
        subprocess.run(quit_cmd, check=True)
        result = subprocess.run(test_cmd, capture_output=True, text=True)
        if result.returncode != 255: ## or result.returncode == 0:
            print("Failed to close ControlMaster Process. Do it manually.")
            print(f"`ssh -O exit -o ControlPath=~/.ssh/{controlpathname} {dbl_username}@{controlpathname}`")
        else:
            print("ControlMaster process closed.")
        return result.returncode

    result = subprocess.run(test_cmd, capture_output=True, text=True)    
    if result.returncode != 0 and dbl_username:
        ### Process is not alive but residual files exist that need to be deletes
        pattern = os.path.expanduser(f"~/.ssh/{controlpathname}") 
        controlfiles =  glob.glob(pattern)
        if len(controlfiles) > 0:
            try:
                for cf in controlfiles:
                    os.remove(cf)
                    print(f"Removed existing control file: {cf}")
            except:
                print(f"Failed to remove control files: {controlfiles}")
        try:
            # print(f"Running on {platform.system()}")
            if platform.system() == "Windows":
                print("")
                print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                print("SSH ControlMaster unavailabele for Windows.")
                print("Install Windows Subsystem for Linux (WSL) and reclone this repository in a Linux space.")
                print("https://learn.microsoft.com/en-us/windows/wsl/install")
                print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                print("")
                webbrowser.open(f"https://learn.microsoft.com/en-us/windows/wsl/install")
               
            else: ## platform.system() == "Linux" or platform.system() == "Darwin" 
                print("")
                print("****************************************")
                print("******* LXPLUS LOGIN CREDENTIALS *******")
                print("****************************************")
                print("")

                scp_timeout_cond = scp_persist_minutes if scp_persist_minutes == 'yes' else f"{scp_persist_minutes}m"    
                ### opens to only dbloader-hgcal via lxplus
                ssh_cmd = ["ssh", "-MNf",
                    "-o", "ControlMaster=yes",
                    "-o", f"ControlPath=~/.ssh/{controlpathname}",    
                    "-o", f"ControlPersist={scp_timeout_cond}",]
                
                for mac in mac_dict_lxplus.keys():
                    ssh_cmd.append("-L")
                    ssh_cmd.append(f"{mac_dict_lxplus[mac]['forwardport']}:{mac_dict_lxplus[mac]['host']}:5432")

                ssh_cmd.append(f"{dbl_username}@lxtunnel.cern.ch")                
                subprocess.run(ssh_cmd, check=True)

                print("** SSH ControlMaster session started. **")
                print("****************************************")
                print("")
                print("************* PLEASE NOTE **************")
                print(f"ControlMaster process will be alive for {scp_persist_minutes} minutes.")
                print(f"To change this, define 'scp_persist_minutes: 240' in dbase_info/conn.yaml.")
                print(f"To allow password-free SCP to your LXPLUS for {scp_persist_minutes} minutes...")
                print(f"define 'scp_force_quit: False' in dbase_info/conn.yaml.")
                print(f"To force quit this open connection manually, run below command in your terminal:")
                print(f"`ssh -O exit -o ControlPath=~/.ssh/{controlpathname} {dbl_username}@{controlpathname}`")
                print("****************************************")
                print("")


        except Exception as e:
            print(f"Failed to create control file.")
            traceback.print_exc()
    
    result = subprocess.run(test_cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print("ControlMaster process alive.")
    else:
        print("ControlMaster process failed.")
    ## ssh -O exit -o ControlPath=~/.ssh/scp-{dbl_username}@{controlpathname} {dbl_username}@{controlpathname} ## To kill process
    # ssh -O exit -o ControlPath=~/.ssh/ctrl_lxplus_dbloader simurthy@ctrl_lxplus_dbloader
    return result.returncode


mac_dict = mac_dict_lxplus

for mac in mac_dict.keys():
    mac_dict[mac]['host'] = 'localhost' ## replace with localhost
    mac_dict[mac]['port'] = mac_dict[mac].pop('forwardport')
    
## conn = await asyncpg.connect(user='viewer', **mac_dict[macid]) 
