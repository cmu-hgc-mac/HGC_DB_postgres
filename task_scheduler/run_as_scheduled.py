import subprocess
import os, sys
from cryptography.fernet import Fernet
os.chdir(project_root)

dbshipper_pass = 'aaaa'
encryption_key = 'bbbb'


sensor_get_stat = True
basplate_get_stat = True
hexaboard_get_stat = True
mmts_inventory_get_stat = True
import_data_cmd = [sys.executable,
                    "import_data/get_parts_from_hgcapi.py", 
                    "-p", dbshipper_pass, 
                    "-k", encryption_key, 
                    "-getbp", str(basplate_get_stat), 
                    "-gethxb", str(hexaboard_get_stat), 
                    "-getmmtsinv", str(mmts_inventory_get_stat), 
                    "-getsen", str(sensor_get_stat)]

subprocess.run(import_data_cmd)


startdate
enddate
export_data_cmd = [sys.executable, 
                   "export_data/export_pipeline.py", 
                   "-dbp", dbshipper_pass, 
                   "-lxu", lxp_username, 
                   "-k", encryption_key, 
                   "-gen", str(True), 
                   "-uplp", str(True), 
                   "-delx", str(True), 
                   "-datestart", str(startdate_var.get()), 
                   "-dateend", str(enddate_var.get())]