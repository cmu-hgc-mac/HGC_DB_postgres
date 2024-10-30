'''
1. generate xmls 
2. save them into a tentative directory ('generated_xmls'/)
3. scp to central db 
4. check the log in central db 
5. if sucess, delete the generated xmls
'''

import os, sys, argparse, pwinput, datetime
import subprocess
import shutil

XML_GENERATOR_DIR = 'export/generate_xmls_utils'## directory for py scripts to generate xmls
GENERATED_XMLS_DIR = 'export/xmls_for_upload'##  directory to store the generated xmls. Feel free to change it. 

# Ensure the generated XML directory exists
os.makedirs(GENERATED_XMLS_DIR, exist_ok=True)
def run_script(script_path, dbpassword, output_dir=GENERATED_XMLS_DIR):
    """Run a Python script as a subprocess."""
    # process = subprocess.run([sys.executable, script_path])
    try:
        process = subprocess.run([sys.executable, script_path, dbpassword, output_dir], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while running the script: {e}")

def generate_xmls(dbpassword):
    """Recursively loop through specific subdirectories under generate_xmls directory and run all Python scripts."""
    tasks = []
    # Specific subdirectories to process
    subdirs = ['baseplate', 'hexaboard', 'module', 'protomodule', 'sensor', 'testing']
    scripts_to_run = []

    for subdir in subdirs:
        subdir_path = os.path.join(XML_GENERATOR_DIR, subdir)
        
        if os.path.exists(subdir_path):
            for file in os.listdir(subdir_path):
                
                ## We only upload build_upload.xml for all parts EXCEPT protomodule and modules. 
                if (subdir_path.split('/')[-1] in ['protomodule', 'module']) and (file.endswith('.py')):
                    script_path = os.path.join(subdir_path, file)
                    scripts_to_run.append(script_path)
                elif subdir_path.split('/')[-1] not in ['protomodule', 'module']:
                    if file.endswith('build_xml.py') == False:
                        script_path = os.path.join(subdir_path, file)
                        scripts_to_run.append(script_path)
    
    # Run all the scripts asynchronously
    total_scripts = len(scripts_to_run)
    completed_scripts = 0
    for script_path in scripts_to_run:
        run_script(script_path, dbpassword)
        completed_scripts += 1
        print('-'*10)
        print(f'Executed -- {script_path}.')
        print(f"Progress: {completed_scripts}/{total_scripts} scripts completed")
        print('-'*10)

def scp_files(lxplus_username, lxplus_password, directory, search_date):
    """Call the scp script to transfer files."""
    try:
        scp_command = ['python3', 
                       'export/dbloader_scp_xml.py', 
                       lxplus_username, 
                       lxplus_password, 
                       directory,
                       search_date]
        process = subprocess.run(scp_command, check=True)

    except Exception as e:
        print(f"Error during SCP: {e}")
        return False

def clean_generated_xmls():
    """Delete all files in the generated XMLs directory after successful SCP."""
    try:
        shutil.rmtree(GENERATED_XMLS_DIR)
        print(f"Deleted all files in {GENERATED_XMLS_DIR}.")
    except Exception as e:
        print(f"Error while deleting files: {e}")

def valid_directory(path):
    if os.path.isdir(path):
        return path
    else:
        raise argparse.ArgumentTypeError(f"Invalid directory: {path}")
    
def main():
    # default_dir = os.path.abspath(os.path.join(os.getcwd(), "../../xmls_for_dbloader_upload"))
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    # Step 0: Get arguments
    parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
    parser.add_argument('-dbp', '--dbpassword', default=None, required=False, help="Password to access database.")
    parser.add_argument('-lxu', '--dbl_username', default=None, required=False, help="Username to access lxplus.")
    parser.add_argument('-lxp', '--dbl_password', default=None, required=False, help="Password to access lxplus.")
    parser.add_argument('-dir','--directory', type=valid_directory, default=GENERATED_XMLS_DIR, help="The directory to process. Default is ../../xmls_for_dbloader_upload.")
    parser.add_argument('-date', '--date', type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d').date(), default=today, help=f"Date for XML generated (format: YYYY-MM-DD). Default is today's date: {today}")
    args = parser.parse_args()

    dbpassword = str(args.dbpassword).replace(" ", "")
    lxplus_username = str(args.dbl_username).replace(" ", "")
    lxplus_password = str(args.dbl_password).replace(" ", "")
    directory_to_search = str(args.directory).replace(" ", "")
    search_date = str(args.date).replace(" ", "")

    if dbpassword is None:
        dbpassword = pwinput.pwinput(prompt='Enter database shipper password: ', mask='*')
    if lxplus_username is None:
        lxplus_username = pwinput.pwinput(prompt='Enter lxplus username: ', mask='*')
    if lxplus_password is None:
        lxplus_password = pwinput.pwinput(prompt='Enter lxplus password: ', mask='*')

    # Step 1: Generate XML files
    generate_xmls(dbpassword)

    # Step 2: SCP files to central DB
    if scp_files(lxplus_username, lxplus_password, directory_to_search, search_date):
        # Step 3: Delete generated XMLs on success
        clean_generated_xmls()

if __name__ == '__main__':
    main()
