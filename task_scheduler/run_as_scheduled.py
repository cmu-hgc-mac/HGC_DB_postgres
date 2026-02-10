import subprocess, os, sys, yaml, base64
import keyring as kr
from cryptography.fernet import Fernet
config_fname = os.path.join(os.path.join(os.getcwd(), 'task_scheduler'), 'schedule_config.yaml')
config_data  = yaml.safe_load(open(config_fname, 'r'))
os.chdir(config_data['HGC_DB_postgres_path'])
from datetime import datetime, timedelta

today = datetime.today().date()
today_str = today.strftime('%Y-%m-%d')
yesterday = today - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d')

encryption_key = Fernet.generate_key()
cipher_suite = Fernet(encryption_key) ## Generate or load a key. 
lxp_username = config_data['CERN_service_account_username']
# kr.get_password('LXPLUS', config_data['CERN_service_account_username'])

dbshipper_pass = base64.urlsafe_b64encode( cipher_suite.encrypt((kr.get_password('POSTGRES', config_data['postgres_username'])).encode()) ).decode()  ## Encrypt password and then convert to base64

if config_data['import_from_HGCAPI']:
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


if config_data['upload_to_CMSR']:
    export_data_cmd = [sys.executable, 
                    "export_data/export_pipeline.py", 
                    "-dbp", dbshipper_pass, 
                    "-lxu", lxp_username, 
                    "-k", encryption_key, 
                    "-gen", str(True), 
                    "-uplp", str(True), 
                    "-delx", str(True), 
                    "-datestart", yesterday_str, 
                    "-dateend", today_str]