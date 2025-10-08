import xml.etree.ElementTree as ET
import csv, argparse, time
from pathlib import Path
from typing import Tuple
import re

GREEN = "\033[32m"; RED = "\033[31m"; YELLOW = "\033[33m"; RESET = "\033[0m"; 
        
def get_upload_log_filepaths(csv_file_path):
    log_paths = []
    with open(csv_file_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            if row:  # ensure non-empty line
                log_paths.append(row[-1])  # last column
    return log_paths

def analyze_log_status(log_path: str) -> Tuple[str, str]:
    """
    Analyze the log file and return a tuple: (status, last_line).
    Status is determined by the log content based on key phrases.
    """
    logfilename = Path(log_path).name
    try:
        if not Path(log_path).exists():
            print(f"{RED}Log Missing: {logfilename}:{RESET} Log file not found")
            return (f"Log Missing", "Log file not found")

        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()

        if not lines:
            print(f"{RED}Log Empty: {logfilename}:{RESET} Log file is empty")
            return ("Log Empty", "Log file is empty")

        last_line = lines[-1].strip()

        log_text = " ".join(lines)
        missing_failed_lastLine_pattern = re.compile(r"\.\.\.\s*\d+\s+more\b")

        # ---- Decision logic ----
        if "commit transaction" in last_line.lower():
            print(f"{GREEN}Success: {logfilename}:{RESET} {last_line}.")
            return ("Success", last_line)

        if "dbloader.java:274" in log_text.lower():
            if "dataset already exists" in log_text.lower():
                print(f"{YELLOW}Already exsists: {logfilename}:{RESET} {last_line}.")
                return ("Already Exists", last_line)
            else:
                return ("XML Parse Error", last_line)

        if missing_failed_lastLine_pattern.search(last_line.lower()):
            print(f"{RED}Missing/Wrong Variable: {logfilename}:{RESET} {last_line}.")
            return ("Missing/Wrong Variable", last_line)

        print(f"{RED}Error: {logfilename}: {RESET} {last_line}.")
        return ("Error", last_line) # Default

    except Exception as e:
        return (f"Error Reading Log: {e}", "")
    
def main():
    parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
    parser.add_argument('-lfp', '--logfilepath', default=None, required=True, help="CSV log file from mass_upload containing upload log paths")
    args = parser.parse_args()

    csv_file_path = args.logfilepath
    log_paths = get_upload_log_filepaths(csv_file_path)

    print(f"{YELLOW}Checking files from {Path(log_paths[0]).parent}{RESET} ...")
    time.sleep(5) ### wait for log files to get saved
    for log_path in log_paths:
        result = analyze_log_status(log_path)

if __name__ == "__main__":
    main()
   