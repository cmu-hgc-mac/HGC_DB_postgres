import paramiko
import getpass
import datetime
import yaml
import subprocess
import time
import os
import argparse
from pathlib import Path

def get_institution_abbr():
    """Read institution_abbr from conn.yaml"""
    yaml_path = Path("../dbase_info/conn.yaml")
    with open(yaml_path, "r") as f:
        config = yaml.safe_load(f)
    return config.get("institution_abbr")

def open_ssh_tunnel(dbl_username: str):
    """Open SSH tunnel with ControlMaster and ControlPath."""
    control_path = os.path.expanduser(f"~/.ssh/scp-{dbl_username}@dbloader-hgcal:22")

    cmd = [
        "ssh", "-MNf",
        "-o", "ControlMaster=yes",
        "-o", f"ControlPath={control_path}",
        "-o", "ControlPersist=2h",
        "-o", f"ProxyJump={dbl_username}@lxtunnel.cern.ch",
        f"{dbl_username}@dbloader-hgcal"
    ]
    subprocess.run(cmd, check=True)
    return control_path

def close_ssh_tunnel(dbl_username: str):
    """Close SSH tunnel with ControlPath."""
    control_path = os.path.expanduser(f"~/.ssh/scp-{dbl_username}@dbloader-hgcal:22")
    cmd = [
        "ssh", "-O", "exit",
        "-o", f"ControlPath={control_path}",
        f"{dbl_username}@dbloader-hgcal"
    ]
    subprocess.run(cmd, check=True)

def check_logs(username, cerndb):
    host = "dbloader-hgcal"
    log_dir = f"/home/dbspool/logs/hgc/{cerndb}"

    # Time window (±30 min from now)
    now = datetime.datetime.now()
    lower = (now - datetime.timedelta(minutes=30)).strftime("%H:%M")
    upper = (now + datetime.timedelta(minutes=10)).strftime("%H:%M")
    today = now.strftime("%b %d")  # e.g. "Sep 05"

    # Get institution abbreviation from conn.yaml
    location = get_institution_abbr()
    if not location or not username:
        raise ValueError("Missing location or username")

    try:
        control_path = open_ssh_tunnel(dbl_username=username)
        time.sleep(15)## pause for 15 seconds

        # Filter log files by date + time range + location
        # The filename contains _{location}_ so we grep for it
        cmd_list = (
            f"ssh -o ControlPath={control_path} {username}@{host} "
            f"\" ls -l {log_dir}/*_{location}_*.log | "
            f"awk '$6\" \"$7 == \\\"{today}\\\" {{split($8,t,\":\"); "
            f"if(t[1]$2>=substr(\\\"{lower}\\\",1,5) && t[1]$2<=substr(\\\"{upper}\\\",1,5)) print $9}}'\""
        )

        result = subprocess.run(cmd_list, capture_output=True, text=True, shell=True, check=True)
        log_files = result.stdout.strip().splitlines()

        if not log_files:
            print("⚠️ No log files found in the given time window for this location.")

        else:
            # Check last lines
            for f in log_files:
                cmd_tail = f"ssh -o ControlPath={control_path} {username}@{host} tail -n 1 {f}"
                result_tail = subprocess.run(cmd_tail, capture_output=True, text=True, shell=True, check=True)
                last_line = result_tail.stdout.strip()

                if "commit transaction" not in last_line:
                    print(f"{f} ---- ❌ {last_line}")

    finally:
        print('Closing SSH tunnel...')
        close_ssh_tunnel(dbl_username=username)


def main():
    parser = argparse.ArgumentParser(description="Script to process files in a directory.")
    parser.add_argument('-lxu', '--dbl_username', default=None, required=False, help="Username to access lxplus.")
    parser.add_argument('-dbp', '--dbpassword', default=None, required=False, help="Password to access database.")
    parser.add_argument('-cerndb', '--cern_dbase', default='dev_db', required=False, help="Name of cern db to upload to - dev_db/prod_db.")
    args = parser.parse_args()

    dbl_username = args.dbl_username
    db_password = args.dbpassword
    cerndb = args.cern_dbase

    check_logs(username=dbl_username, cerndb=cerndb)

if __name__ == "__main__":
    main()
