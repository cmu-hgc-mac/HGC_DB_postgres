import yaml
import os

# Define the path to the YAML file that contains the 'variable'
config_dir = os.path.dirname(os.path.abspath(__file__))
yaml_file_path = os.path.join(config_dir, '..', 'dbase_info', 'conn.yaml')
resource_file_path = os.path.join(config_dir, '..', 'export_data', 'resource.yaml')

# Initialize the global variable
LOCATION = None
INSTITUTION = None

# Function to load and define the global variable
def initialize_global_variable():
    global LOCATION, INSTITUTION
    with open(yaml_file_path, 'r') as file:
        data = yaml.safe_load(file)
        LOCATION = data['institution_abbr']  # Retrieve 'variable' from file_1.yaml
    with open(resource_file_path, 'r') as file:
        data = yaml.safe_load(file)
        INSTITUTION = data['institution_fullname'].get(LOCATION)

# Call this function to load the variable when the module is imported
initialize_global_variable()
