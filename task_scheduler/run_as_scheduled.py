import subprocess, os, sys, yaml, base64, pexpect
from cryptography.fernet import Fernet
from datetime import datetime, timedelta
config_fname = os.path.join(os.path.join(os.getcwd(), 'task_scheduler'), 'schedule_config.yaml')
sched_config  = yaml.safe_load(open(config_fname, 'r'))
os.chdir(sched_config['HGC_DB_postgres_path'])
from export_data.src import open_scp_connection
from task_scheduler.scheduler_helper import JobIndicator
conn_yaml_file = os.path.join('dbase_info', 'conn.yaml')
config_data  = yaml.safe_load(open(conn_yaml_file, 'r'))
scp_persist_minutes = config_data.get('scp_persist_minutes', 240)
scp_force_quit = config_data.get('scp_force_quit', True)

today = datetime.today().date()
today_str = today.strftime('%Y-%m-%d')
yesterday = today - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d')

with open(sched_config['encrypt_path'], "rb") as key_file:
    encryption_key = key_file.read()

cipher_suite = Fernet(encryption_key)

with open(sched_config['postgres_shipper_pass_path'], "rb") as f:
    encrypted_password_postgres = f.read()

dbshipper_pass = base64.urlsafe_b64encode( encrypted_password_postgres ).decode() 

if sched_config['import_from_HGCAPI']:
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


if sched_config['upload_to_CMSR']:
    restore_seq = subprocess.run(["git", "restore", "export_data/list_of_xmls.yaml" ], capture_output=True, text=True)
    lxp_username = sched_config['CERN_service_account_username']

    with JobIndicator("/tmp/my_cron_job.running"):
        scp_status = open_scp_connection(dbl_username=lxp_username, scp_persist_minutes=scp_persist_minutes)
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
        scp_status = open_scp_connection(dbl_username=lxp_username, scp_persist_minutes=scp_persist_minutes, scp_force_quit=True)