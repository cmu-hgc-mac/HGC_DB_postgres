import os
import time
import yaml
import re
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path


# Paths
XMLS_DIR = "export_data/xmls_for_upload"
YAML_FILE = "export_data/table_to_xml_var.yaml"

# Load YAML file
with open(YAML_FILE, "r") as f:
    yaml_data = yaml.safe_load(f)

# Mapping of part names (directory) to YAML categories
PART_TO_YAML_CATEGORIES = {
    "sensor": ["sensor_inspection"],
    "module": ["module_cure_cond", "module_assembly", "module_build", "wirebond", "module_inspection", "module_grading"],
    "protomodule": ["proto_cure_cond", "proto_assembly", "proto_build", "module_inspection"],
    "hexaboard": ["hxb_inspection", "hxb_build"],
    "baseplate": ["bp_inspection", "bp_build"]
    }

# Function to get XML files, with an optional time limit
def get_xml_files(base_dir, time_limit=None):
    xml_files = []
    current_time = time.time()

    for root, _, files in os.walk(base_dir):
        for file in files:
            file_path = os.path.join(root, file)
            within_time = time_limit is None or (current_time - os.path.getmtime(file_path)) <= time_limit
            if not within_time:
                continue
            if file.endswith(".xml"):
                xml_files.append(file_path)
            elif file.endswith(".zip"):
                try:
                    with zipfile.ZipFile(file_path, 'r') as zf:
                        for member in zf.namelist():
                            if member.endswith(".xml"):
                                xml_files.append(os.path.join(file_path, member))
                except zipfile.BadZipFile:
                    print(f"Could not open zip: {file_path}")

    return xml_files

# Function to extract all tags and their values from an XML file
def extract_xml_tags_and_values(xml_file):
    try:
        # Handle virtual paths like "path/to/archive.zip/member.xml"
        zip_idx = xml_file.find('.zip' + os.sep)
        if zip_idx != -1:
            zip_path = xml_file[:zip_idx + 4]
            member = xml_file[zip_idx + 5:]
            with zipfile.ZipFile(zip_path, 'r') as zf:
                with zf.open(member) as f:
                    tree = ET.parse(f)
        else:
            tree = ET.parse(xml_file)
        root = tree.getroot()
        tags_with_values = {}
        for elem in root.iter():
            text_value = elem.text.strip() if elem.text and elem.text.strip() else None
            tags_with_values[elem.tag] = text_value
        return tags_with_values
    except ET.ParseError:
        print(f"Error parsing {xml_file}")
        return {}

# Function to determine the YAML categories based on directory name
def get_yaml_categories(xml_file_path):
    """Determine the correct YAML sections based on the directory name."""
    parts = Path(xml_file_path).parts
    # For zip members, skip the .zip component to get the actual subdirectory
    non_zip_parts = [p for p in parts if not p.endswith('.zip')]
    part_name = non_zip_parts[-2]  # Get directory name (e.g., 'sensor')
    if part_name == 'testing':
        return "testing"
    else:
        xml_type = xml_file_path.split('_')[-2] ## e.g. cond, build, assembly, wirebond, grading
        _yaml_cat = PART_TO_YAML_CATEGORIES[part_name]
        yaml_cat = [item for item in _yaml_cat if item.endswith(xml_type)][0]
        return yaml_cat

# Function to find missing or empty XML tags
def find_missing_or_empty_tags(expected_tags, xml_data):
    missing_or_empty = {}
    placeholder_pattern = re.compile(r"\{\{\s*(\w+)\s*\}\}")

    for tag, (dbase_table, dbase_col, nullable) in expected_tags.items():
        value = xml_data.get(tag, None)
        if value is None or value == "" or placeholder_pattern.match(str(value)):
            missing_or_empty[tag] = (dbase_table, dbase_col, nullable)
    return missing_or_empty

# Function to get expected tags from a list of YAML categories
def get_expected_tags(category):
    expected_tags = {}
    for item in yaml_data[category]:
        if "xml_tag" in item:
            expected_tags[item["xml_tag"]] = (item.get("dbase_table"), item.get("dbase_col"), item.get("nullable"))
    return expected_tags

# Main execution
def find_missing_var_xml(time_limit=90):
    '''
    time_limit - Set to None to get all XML files, or specify a time limit in seconds
    '''
    xml_files = get_xml_files(XMLS_DIR, time_limit=time_limit)

    if not xml_files:
        print("No XML files found.")

    # group_key -> {'tags': {tag: (table, col)}, 'files': [filenames]}
    groups = {}

    for xml_file in xml_files:
        if xml_file.split('/')[2] != 'testing':
            xml_data = extract_xml_tags_and_values(xml_file)
            yaml_category = get_yaml_categories(xml_file)

            if yaml_category:
                expected_tags_map = get_expected_tags(yaml_category)
                missing_tags = find_missing_or_empty_tags(expected_tags_map, xml_data)

                # drop nullable tags
                non_nullable = {tag: (tbl, col) for tag, (tbl, col, nullable) in missing_tags.items() if not nullable}

                if non_nullable:
                    key = tuple(sorted(non_nullable.keys()))
                    if key not in groups:
                        groups[key] = {'tags': non_nullable, 'files': []}
                    groups[key]['files'].append(xml_file.split('/')[-1])

            else:
                print(f"\nNo matching YAML categories found for {xml_file}. Skipping.")

    for key, info in groups.items():
        print(f"\n===== MISSING OR EMPTY TAGS =====")
        for tag, (dbase_table, dbase_col) in info['tags'].items():
            print(f" - {tag}:  table={dbase_table}, col={dbase_col}")
        print(f"  Affects {len(info['files'])} file(s):")
        for fname in info['files']:
            print(f"    • {fname}")
        print("=" * 36)