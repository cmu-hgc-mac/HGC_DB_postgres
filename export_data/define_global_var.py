import yaml
import os

# Define the path to the YAML file that contains the 'variable'
config_dir = os.path.dirname(os.path.abspath(__file__))
yaml_file_path = os.path.join(config_dir, '..', 'dbase_info', 'conn.yaml')

# Initialize the global variable
LOCATION = None

# Function to load and define the global variable
def initialize_global_variable():
    global LOCATION
    with open(yaml_file_path, 'r') as file:
        data = yaml.safe_load(file)
        LOCATION = data['institution_abbr']  # Retrieve 'variable' from file_1.yaml

# Call this function to load the variable when the module is imported
initialize_global_variable()
