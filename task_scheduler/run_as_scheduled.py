import subprocess, os, sys, yaml, base64
from cryptography.fernet import Fernet
config_fname = os.path.join(os.path.join(os.getcwd(), 'task_scheduler'), 'schedule_config.yaml')
config_data  = yaml.safe_load(open(config_fname, 'r'))
os.chdir(config_data['HGC_DB_postgres_path'])
from datetime import datetime, timedelta

today = datetime.today().date()
today_str = today.strftime('%Y-%m-%d')
yesterday = today - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d')

with open(config_data['encrypt_path'], "rb") as key_file:
    encryption_key = key_file.read()

cipher_suite = Fernet(encryption_key)

with open(config_data['postgres_shipper_pass_path'], "rb") as f:
    encrypted_password_postgres = f.read()

dbshipper_pass = cipher_suite.decrypt(encrypted_password_postgres).decode()

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

    lxp_username = config_data['CERN_service_account_username']
    # with open(config_data['CERN_service_account_pass_path'], "rb") as f:
    #     encrypted_password_lxplus = f.read()
    
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