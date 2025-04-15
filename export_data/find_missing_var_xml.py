import os
import time
import yaml
import re
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
    "sensor": ["sensor_cond"],
    "module": ["module_cond", "module_assembly", "module_build", "wirebond"],
    "protomodule": ["proto_cond", "proto_assembly", "proto_build"],
    "hexaboard": ["hxb_cond", "hxb_build"],
    "baseplate": ["bp_cond", "bp_build"]
    }

# Function to get XML files, with an optional time limit
def get_xml_files(base_dir, time_limit=None):
    xml_files = []
    current_time = time.time()

    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".xml"):
                file_path = os.path.join(root, file)
                if time_limit is None or (current_time - os.path.getmtime(file_path)) <= time_limit:
                    xml_files.append(file_path)
    
    return xml_files

# Function to extract all tags and their values from an XML file
def extract_xml_tags_and_values(xml_file):
    try:
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
    part_name = Path(xml_file_path).parts[-2]  # Get directory name (e.g., 'sensor')
    if part_name == 'testing':
        return "testing"
    else:
        xml_type = xml_file_path.split('_')[-2] ## e.g. cond, build, assembly, wirebond
        _yaml_cat = PART_TO_YAML_CATEGORIES[part_name]
        yaml_cat = [item for item in _yaml_cat if item.endswith(xml_type)][0]
        return yaml_cat

# Function to find missing or empty XML tags
def find_missing_or_empty_tags(expected_tags, xml_data):
    missing_or_empty = {}
    placeholder_pattern = re.compile(r"\{\{\s*(\w+)\s*\}\}")

    for tag, (dbase_table, dbase_col) in expected_tags.items():
        value = xml_data.get(tag, None)
        if value is None or value == "" or placeholder_pattern.match(str(value)):
            missing_or_empty[tag] = (dbase_table, dbase_col)
    return missing_or_empty

# Function to get expected tags from a list of YAML categories
def get_expected_tags(category):
    expected_tags = {}
    for item in yaml_data[category]:
        if "xml_tag" in item:
            expected_tags[item["xml_tag"]] = (item.get("dbase_table"), item.get("dbase_col"))
    return expected_tags

# Main execution
def find_missing_var_xml(time_limit=90):
    '''
    time_limit - Set to None to get all XML files, or specify a time limit in seconds
    '''
    xml_files = get_xml_files(XMLS_DIR, time_limit=time_limit)

    if not xml_files:
        print("No XML files found.")

    for xml_file in xml_files:
        if xml_file.split('/')[2] != 'testing':
            xml_data = extract_xml_tags_and_values(xml_file)
            yaml_category = get_yaml_categories(xml_file)

            if yaml_category:
                expected_tags_map = get_expected_tags(yaml_category)
                missing_tags = find_missing_or_empty_tags(expected_tags_map, xml_data)

                if missing_tags:

                    print(f"\n===== MISSING OR EMPTY TAGS FOUND for {xml_file.split('/')[-1]}! =====")
                    print(f"  Referencing YAML categories: {yaml_category}")
                    print("------------------------------------------------------------")

                    for tag, (dbase_table, dbase_col) in missing_tags.items():
                        print(f" - {tag}:\n   → dbase_table: {dbase_table}\n   → dbase_col: {dbase_col}")
                    print("============================================================")

            else:
                print(f"\nNo matching YAML categories found for {xml_file}. Skipping.")