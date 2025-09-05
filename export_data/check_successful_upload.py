import paramiko
import getpass
import datetime
import yaml
from pathlib import Path

def get_institution_abbr():
    """Read institution_abbr from conn.yaml"""
    yaml_path = Path("../dbase_info/conn.yaml")
    with open(yaml_path, "r") as f:
        config = yaml.safe_load(f)
    return config.get("institution_abbr")

def check_logs(username, password, cerndb):
    host = "dbloader-hgcal"
    log_dir = f"/home/dbspool/logs/hgc/{cerndb}"

    # SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username=username, password=password)

    # Time window (±30 min from now)
    now = datetime.datetime.now()
    lower = (now - datetime.timedelta(minutes=30)).strftime("%H:%M")
    upper = (now + datetime.timedelta(minutes=30)).strftime("%H:%M")
    today = now.strftime("%b %d")  # e.g. "Sep 05"

    # Get institution abbreviation from conn.yaml
    location = get_institution_abbr()
    if not location:
        print("⚠️ Could not find institution_abbr in conn.yaml")
        return

    print(f"Checking logs for location: {location}")

    # Filter log files by date + time range + location
    # The filename contains _{location}_ so we grep for it
    cmd_list = (
        f"ls -l {log_dir}/*_{location}_*.log | "
        f"awk '$6\" \"$7 == \"{today}\" "
        f"{{split($8,t,\":\"); "
        f"if(t[1]$2>=substr(\"{lower}\",1,5) && t[1]$2<=substr(\"{upper}\",1,5)) print $9}}'"
    )

    stdin, stdout, stderr = ssh.exec_command(cmd_list)
    files = stdout.read().decode().splitlines()

    if not files:
        print("⚠️ No log files found in the given time window for this location.")
        ssh.close()
        return

    # Check last lines
    for f in files:
        cmd_tail = f"tail -n 1 {f}"
        stdin, stdout, stderr = ssh.exec_command(cmd_tail)
        last_line = stdout.read().decode().strip()

        if "commit transaction" not in last_line:
            print(f"{f} ❌ {last_line}")

    ssh.close()


if __name__ == "__main__":
    username = input("Enter SSH username: ")
    password = getpass.getpass("Enter SSH password: ")
    cerndb = input("Enter cerndb folder name (e.g. cerndb01): ")

    check_logs(username, password, cerndb)
