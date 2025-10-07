import xml.etree.ElementTree as ET
import csv, argparse

def get_upload_log_filepaths(csv_file_path):
    log_paths = []
    with open(csv_file_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            if row:  # ensure non-empty line
                log_paths.append(row[-1])  # last column
    return log_paths


def search_xml_file(xml_file_path, keywords_to_find = ['error', 'exception']):
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
    except ET.ParseError as e:
        print("Invalid XML file:", e)
        exit()

    found = False

    for elem in root.iter():
        text = (elem.text or "").lower()
        tag = (elem.tag or "").lower()
        if "error" in text or "exception" in text or "error" in tag or "exception" in tag:
            print(f"Found in element: <{elem.tag}> => {elem.text}")
            found = True

    if not found:
        print("No mentions of 'error' or 'exception' found.")

def main():
    parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
    parser.add_argument('-lfp', '--logfilepath', default=None, required=True, help="CSV log file from mass_upload containing upload log paths")
    args = parser.parse_args()

    csv_file_path = args.logfilepath
    log_paths = get_upload_log_filepaths(csv_file_path)
    for log_path in log_paths:
        result = search_xml_file(log_paths)
   