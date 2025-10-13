import xml.etree.ElementTree as ET
import csv, argparse, time
from pathlib import Path
from typing import Tuple
import re

GREEN = "\033[32m"; RED = "\033[31m"; YELLOW = "\033[33m"; RESET = "\033[0m"; 
remote_xml_dir = f"~/hgc_xml_temp"
status_tracker = {'dbloader_failure': 0, 'xml_issues': 0 , 'dbloader_success' : 0}

def remove_file(file_path: Path):
    if file_path.exists():
        file_path.unlink()

def get_upload_log_filepaths(csv_file_path):
    log_paths, upload_paths = [], []
    with open(csv_file_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            if row:  # ensure non-empty line
                log_paths.append(row[-1])  # last column
                upload_paths.append(row[-2])  # second last column
    return log_paths, upload_paths

def analyze_log_status(log_path: str, upload_path: str, status_tracker: dict) -> Tuple[str, str]:
    """
    Analyze the log file and return a tuple: (status, last_line).
    Status is determined by the log content based on key phrases.
    """
    xmlfilename = Path(upload_path).name
    try:
        if not Path(log_path).exists():
            print(f"{RED}Log Missing: {xmlfilename}:{RESET} Log file not found")
            status_tracker['dbloader_failure'] +=1
            return (f"Log Missing", "Log file not found")

        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()

        if not lines:
            print(f"{RED}Log Empty: {xmlfilename}:{RESET} Log file is empty")
            status_tracker['dbloader_failure'] +=1
            return ("Log Empty", "Log file is empty")

        last_line = lines[-1].strip()
        log_text = " ".join(lines)
        missing_failed_lastLine_pattern = re.compile(r"\.\.\.\s*\d+\s+more\b")

        # ---- Decision logic ----
        if "commit transaction" in last_line.lower():
            print(f"{GREEN}Success: {xmlfilename}:{RESET} {last_line}")
            status_tracker['dbloader_success'] +=1
            remove_file(Path(remote_xml_dir, xmlfilename))
            return ("Success", last_line)

        if "dbloader.java:274" in log_text.lower():
            if "dataset already exists" in log_text.lower():
                print(f"{YELLOW}Already exsists: {xmlfilename}:{RESET} {last_line}")
                status_tracker['dbloader_success'] +=1
                remove_file(Path(remote_xml_dir, xmlfilename))
                return ("Already Exists", last_line)
            else:
                print(f"{RED}XML Parse Error: {xmlfilename}: {RESET} {last_line}")
                status_tracker['xml_issues'] +=1
                return ("XML Parse Error", last_line)

        if missing_failed_lastLine_pattern.search(last_line.lower()):
            print(f"{RED}Missing/Wrong Variable: {xmlfilename}:{RESET} {last_line}")
            status_tracker['xml_issues'] +=1
            return ("Missing/Wrong Variable", last_line)

        print(f"{RED}Error: {xmlfilename}: {RESET} {last_line}")
        status_tracker['dbloader_failure'] +=1
        return ("Error", last_line) # Default

    except Exception as e:
        print(f"{RED}Error Reading Log: {xmlfilename}{RESET}")
        status_tracker['dbloader_failure'] +=1
        return (f"Error Reading Log: {e}", "")
    
def main():
    parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
    parser.add_argument('-lfp', '--logfilepath', default=None, required=True, help="CSV log file from mass_upload containing upload log paths")
    args = parser.parse_args()

    csv_file_path = args.logfilepath
    log_paths, upload_paths = get_upload_log_filepaths(csv_file_path)
    print(f"{YELLOW}Checking files from {Path(log_paths[0]).parent}{RESET} ...")
    time.sleep(5) ### wait for log files to get saved
    for log_path, upload_path in zip(log_paths, upload_paths):
        result = analyze_log_status(log_path, upload_path)
    print('XML CHECK STATUS')
    for k in status_tracker.keys():
        print(k, '-', status_tracker[k])

if __name__ == "__main__":
    main()
   
