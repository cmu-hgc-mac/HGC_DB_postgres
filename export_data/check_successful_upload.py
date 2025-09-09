import paramiko
import getpass
import datetime
import yaml
import subprocess
import time
import os
import argparse
import pytz
from pathlib import Path

oracle_error_conversion = {
    "(DbLoader.java:274)": 'Dataset already exists'
}

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

def check_logs(username, cerndb):
    host = "dbloader-hgcal"
    log_dir = f"/home/dbspool/logs/hgc/{cerndb}"

    # --- Timezone conversion (local → Swiss time) ---
    local_tz = datetime.datetime.now().astimezone().tzinfo
    swiss_tz = pytz.timezone("Europe/Zurich")

    # current Swiss time
    now_swiss = datetime.datetime.now(local_tz).astimezone(swiss_tz)

    # Time window (±30 min)
    lower = (now_swiss - datetime.timedelta(minutes=40)).strftime("%H:%M")
    upper = (now_swiss + datetime.timedelta(minutes=10)).strftime("%H:%M")
    today = now_swiss.strftime("%Y-%m-%d")  # '2025-09-09'

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

        list_cmd = (
            f'ssh -o ControlPath={control_path} {username}@{host} '
            f'ls -lrt {log_dir}/*_{location}_*.xml | tail -n 500'
        )

        listing = subprocess.run(list_cmd, capture_output=True, text=True, shell=True)

        if listing.returncode != 0:
            print("❌ SSH worked, but failed to list log files.")
            print(f"stderr: {listing.stderr.strip()}")
            return

        print(listing)
        log_files = []
        for line in listing.stdout.strip().splitlines():
            parts = line.split()
            if len(parts) < 9:
                continue
            date = f"{parts[5]} {parts[6]}"  # e.g. "Sep 09"
            time = parts[7]  # e.g. "22:05"
            filename = parts[-1]

            # Keep only today’s logs in time window
            if date == today and lower <= time <= upper:
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

    print('Waiting for 10 seconds for logs to be ready...')
    time.sleep(10)
    check_logs(username=dbl_username, cerndb=cerndb)

if __name__ == "__main__":
    main()
