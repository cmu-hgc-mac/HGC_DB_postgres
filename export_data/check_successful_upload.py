import paramiko
import getpass
import datetime
import yaml
import subprocess
import os
import argparse
import pytz
from pathlib import Path

oracle_error_conversion = {
    "(DbLoader.java:274)": 'Dataset already exists'
}

transition_date = '2025-02-28' ## time when CERN DB changed a format of log filename
transition_dt = datetime.datetime.strptime(transition_date, "%Y-%m-%d")

def get_institution_abbr():
    """Read institution_abbr from conn.yaml"""
    yaml_path = Path("dbase_info/conn.yaml")
    with open(yaml_path, "r") as f:
        config = yaml.safe_load(f)
    return config.get("institution_abbr")

def close_ssh_tunnel(dbl_username: str):
    """Close SSH tunnel with ControlPath."""
    control_path = os.path.expanduser(f"~/.ssh/scp-{dbl_username}@dbloader-hgcal:22")
    cmd = [
        "ssh", "-O", "exit",
        "-o", f"ControlPath={control_path}",
        f"{dbl_username}@dbloader-hgcal"
    ]
    subprocess.run(cmd, check=True)

## resolve the datetime format discrepancy in CERN DB log file
# def check_transition(dt: datetime.datetime) -> str:
#     return "before" if dt < transition_dt else "after"

def check_logs(username, cerndb):
    host = "dbloader-hgcal"
    log_dir = f"/home/dbspool/logs/hgc/{cerndb}"

    # --- Timezone conversion (local → Swiss time) ---
    local_tz = datetime.datetime.now().astimezone().tzinfo
    swiss_tz = pytz.timezone("Europe/Zurich")

    # current Swiss time
    now_swiss = datetime.datetime.now(local_tz).astimezone(swiss_tz)

    # Time window (±30 min)
    lower_dt = (now_swiss - datetime.timedelta(minutes=40)).replace(second=0, microsecond=0)
    upper_dt = (now_swiss + datetime.timedelta(minutes=10)).replace(second=0, microsecond=0)

    # Get institution abbreviation from conn.yaml
    location = get_institution_abbr()
    if not location or not username:
        raise ValueError("Missing location or username")

    control_path = os.path.expanduser(f"~/.ssh/scp-{username}@{host}:22")

    try:
        # Step 1: Test SSH tunnel
        test_cmd = f'ssh -o ControlPath={control_path} {username}@{host} "echo SSH_OK"'
        test = subprocess.run(test_cmd, capture_output=True, text=True, shell=True)
        if test.returncode != 0 or "SSH_OK" not in test.stdout:
            print("❌ SSH tunnel not working.")
            print(f"stderr: {test.stderr.strip()}")
            return

        # Step 2 + 3: Find candidate logs and check last line
        # list_cmd = (
        #     f'ssh -o ControlPath={control_path} {username}@{host} '
        #     f'find {log_dir} -type f -name "*_{location}_*.xml" '
        #     f'-newermt "{today} {lower}:00" ! -newermt "{today} {upper}:00"'
        # )

        # list_cmd = [
        #     "ssh", "-o", f"ControlPath={control_path}",
        #     f"{username}@{host}",
        #     f"ls -lrt {log_dir}/*_{location}_*.xml | tail -n 500"
        # ]
        # print(list_cmd)
        # listing = subprocess.run(list_cmd, capture_output=True, text=True, shell=True)

        # remote_cmd = f'ls -lrt {log_dir}/*_{location}_*.xml | tail -n 500'
        # list_cmd = [
        #     "ssh", "-o", "ControlMaster=no", "-o", f"ControlPath={control_path}",
        #     f"{username}@{host}",
        #     "bash", "-c", f"'{remote_cmd}'"
        # ]

        remote_cmd = (
            f'find {log_dir}/*_{location}_*.xml -maxdepth 1 -name "*_{location}_*.xml" -type f '
            f'-printf "%TY-%Tm-%Td %TH:%TM:%TS %p\n" | sort | tail -n 500'
        )
        # remote_cmd = f'ls -lrt {log_dir}/*_{location}_*.xml | tail -n 100'
        list_cmd = [
            "ssh",
            "-o", "ControlMaster=auto",
            "-o", f"ControlPath={control_path}",
            "-o", "ControlPersist=no",
            f"{username}@{host}",
            remote_cmd
        ]
        listing = subprocess.run(list_cmd, capture_output=True, text=True)
        log_lines = listing.stdout.strip().split('\n')
        
        ######################################
        # Still failing to grab the latest log files. IT is likely due to ssh catching issue. 
        ######################################

        if listing.returncode != 0:
            print("❌ SSH worked, but failed to list log files.")
            print(f"stderr: {listing.stderr.strip()}")
            return

        log_files = []
        for line in listing.stdout.strip().splitlines():
            parts = line.split()
            print(parts)
            date = parts[0]   # e.g. "2024-11-21"
            time = parts[1][:5]  # take only HH:MM → "15:56"
            filename = parts[-1]
            
            dt_string = f"{date} {time}" # e.g. '2024-11-21' 
            filename = parts[-1]
        
            log_dt = datetime.datetime.strptime(dt_string, "%Y-%m-%d %H:%M")

            # print(filename.split('/')[-1], check_transition(log_dt), type(log_dt), type(lower_dt), type(upper_dt), lower_dt <= log_dt <= upper_dt)
            print(f'{log_dt}, {lower_dt}, {upper_dt}')

            if lower_dt <= log_dt <= upper_dt:
                log_files.append(filename)

        if not log_files:
            print("⚠️ No log files found for this location.")
            return
        
        for filename in log_files:
            xml_filename = os.path.basename(filename)

            tail_cmd = (
                f'ssh -o ControlPath={control_path} {username}@{host} '
                f'"tail -n 1 {filename}"'
            )
            tail = subprocess.run(tail_cmd, capture_output=True, text=True, shell=True)

            if tail.returncode != 0:
                print(f"❌ Failed to read file {xml_filename}")
                print(f"stderr: {tail.stderr.strip()}")
                continue

            last_line = tail.stdout.strip()

            if "commit transaction" in last_line:
                print(f"{xml_filename.split('/')[-1]} ---- ✅ commit transaction")
            else:
                error = oracle_error_conversion[xml_filename]
                print(f"❌ {xml_filename.split('/')[-1]} ---- {error}")

    finally:
        print("Closing SSH tunnel...")
        close_ssh_tunnel(dbl_username=username)

def main():
    parser = argparse.ArgumentParser(description="Script to process files in a directory.")
    parser.add_argument('-lxu', '--dbl_username', default=None, required=False, help="Username to access lxplus.")
    parser.add_argument('-cerndb', '--cern_dbase', default='dev_db', required=False, help="Name of cern db to upload to - dev_db/prod_db.")
    args = parser.parse_args()

    dbl_username = args.dbl_username
    cerndb = args.cern_dbase

    check_logs(username=dbl_username, cerndb=cerndb)

if __name__ == "__main__":
    main()
