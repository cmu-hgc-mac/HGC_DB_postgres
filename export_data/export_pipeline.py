'''
1. generate xmls 
2. save them into a tentative directory ('generated_xmls'/)
3. scp to central db 
4. check the log in central db 
5. if sucess, delete the generated xmls
'''

import os, sys, argparse, base64, subprocess, traceback, asyncio
import shutil, pwinput, datetime, yaml
from cryptography.fernet import Fernet
from src import process_xml_list
from find_missing_var_xml import find_missing_var_xml
from check_successful_upload import check_upload, get_api_data, get_part_id_fromXML

XML_GENERATOR_DIR = 'export_data/generate_xmls_utils'## directory for py scripts to generate xmls
GENERATED_XMLS_DIR = 'export_data/xmls_for_upload'##  directory to store the generated xmls. Feel free to change it. 

# Ensure the generated XML directory exists
os.makedirs(GENERATED_XMLS_DIR, exist_ok=True)

def str2bool(boolstr):
    dictstr = {'True': True, 'False': False}
    return dictstr[boolstr]

def run_script(script_path, dbpassword, date_start, date_end, output_dir=GENERATED_XMLS_DIR, encryption_key = None, partsnamelist = None):
    """Run a Python script as a subprocess."""
    # process = subprocess.run([sys.executable, script_path])
    command = [
        sys.executable, script_path,
        '-dbp', dbpassword,
        '-dir', output_dir,
        '-k', encryption_key,
        '-datestart', date_start,
        '-dateend', date_end]
    
    if partsnamelist:
        command += ['-pn'] + partsnamelist

    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        traceback.print_exc()
        print(f"Error occurred while running the script: {e}")

def generate_xmls(dbpassword, date_start, date_end, encryption_key = None, partsnamelist = None):
    """Recursively loop through specific subdirectories under generate_xmls directory and run all Python scripts."""
    tasks = []
    # Specific subdirectories to process
    xml_list = process_xml_list(get_yaml_data = True)
    subdirs = list(xml_list.keys())    # subdirs = ['baseplate', 'hexaboard', 'module', 'protomodule', 'sensor', 'testing']
    scripts_to_run = []

    for subdir in subdirs:
        subdir_path = os.path.join(XML_GENERATOR_DIR, subdir)
        
        if os.path.exists(subdir_path):
            # for file in os.listdir(subdir_path):
            for filetype in list(xml_list[subdir]):
                file_suff = list(filetype.keys())[0]
                file = f"generate_{file_suff}.py"
                if filetype[file_suff] and os.path.exists(os.path.join(subdir_path, file)):                
                    ## We only upload build_upload.xml for all parts EXCEPT protomodule and modules.     
                    if subdir_path.split('/')[-1] not in ['protomodule', 'module'] and (file.endswith('build_xml.py') == True):
                        print(f'subdir_path to skip -- {subdir_path}')
                        continue; 
                    script_path = os.path.join(subdir_path, file)
                    scripts_to_run.append(script_path)
    #Run all the scripts asynchronously
    total_scripts = len(scripts_to_run)
    completed_scripts = 0
    for script_path in scripts_to_run:
        run_script(script_path = script_path, dbpassword = dbpassword, encryption_key = encryption_key, date_start=date_start, date_end=date_end, partsnamelist=partsnamelist)
        completed_scripts += 1
        print('-'*10)
        print(f'Executed -- {script_path}.')
        print(f"Progress: {completed_scripts}/{total_scripts} XML file types generated.")
        print('-'*10); print('')

def scp_files(lxplus_username, lxplus_password, directory, search_date, encryption_key = None, cerndb = 'dev_db'):
    """Call the scp script to transfer files."""
    try:
        scp_command = ['python3', 
                       'export_data/dbloader_scp_xml.py', 
                       '-lxu', lxplus_username, 
                       '-lxp', lxplus_password, 
                       '-dir', directory,
                       '-date', str(search_date),
                       '-cerndb', cerndb,
                       '-k', encryption_key]
    
        process = subprocess.run(scp_command, check=True)
        return True

    except Exception as e:
        traceback.print_exc()
        print(f"Error during SCP: {e}")
        return False

def clean_generated_xmls():
    """Delete all files in the generated XMLs directory after successful SCP."""
    try:
        shutil.rmtree(GENERATED_XMLS_DIR)
        print(f"Deleted all files in {GENERATED_XMLS_DIR}.")
    except Exception as e:
        traceback.print_exc()
        print(f"Error while deleting files: {e}")

def valid_directory(path):
    if os.path.isdir(path):
        return path
    else:
        raise argparse.ArgumentTypeError(f"Invalid directory: {path}")
    
async def main():
    # default_dir = os.path.abspath(os.path.join(os.getcwd(), "../../xmls_for_dbloader_upload"))
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    # Step 0: Get arguments
    parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
    parser.add_argument('-dbp', '--dbpassword', default=None, required=False, help="Password to access database.")
    parser.add_argument('-lxu', '--dbl_username', default=None, required=False, help="Username to access lxplus.")
    parser.add_argument('-lxp', '--dbl_password', default=None, required=False, help="Password to access lxplus.")
    parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
    parser.add_argument('-dir','--directory', type=valid_directory, default=GENERATED_XMLS_DIR, help="The directory to process. Default is ../../xmls_for_dbloader_upload.")
    # parser.add_argument('-date', '--date', type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d').date(), default=today, help=f"Date for XML generated (format: YYYY-MM-DD). Default is today's date: {today}")
    parser.add_argument('-datestart', '--date_start', type=lambda s: str(datetime.datetime.strptime(s, '%Y-%m-%d').date()), default=str(today), help=f"Date for XML generated (format: YYYY-MM-DD). Default is today's date: {today}")
    parser.add_argument('-dateend', '--date_end', type=lambda s: str(datetime.datetime.strptime(s, '%Y-%m-%d').date()), default=str(today), help=f"Date for XML generated (format: YYYY-MM-DD). Default is today's date: {today}")
    parser.add_argument('-gen', '--generate_stat', default='True', required=False, help="Generate XMLs.")
    parser.add_argument('-upld', '--upload_dev_stat', default='True', required=False, help="Upload to dev DBLoader without generate.")
    parser.add_argument('-uplp', '--upload_prod_stat', default='True', required=False, help="Upload to prod DBLoader without generate.")
    parser.add_argument('-delx', '--del_xml', default='False', required=False, help="Delete XMLs after upload.")
    parser.add_argument("-pn", '--partnameslist', nargs="+", help="Space-separated list", required=False)

    args = parser.parse_args()

    dbpassword = args.dbpassword or pwinput.pwinput(prompt='Enter database shipper password: ', mask='*')
    lxplus_username = args.dbl_username or pwinput.pwinput(prompt='Enter lxplus username: ', mask='*')
    lxplus_password = args.dbl_password or pwinput.pwinput(prompt='Enter lxplus password: ', mask='*')
    directory_to_search = args.directory
    date_start = args.date_start
    date_end = args.date_end
    encryption_key = args.encrypt_key
    upload_dev_stat = str2bool(args.upload_dev_stat)
    upload_prod_stat = str2bool(args.upload_prod_stat)
    partsnamelist = args.partnameslist

    inst_code  = (yaml.safe_load(open(os.path.join('dbase_info', 'conn.yaml'), 'r'))).get('institution_abbr')
    if len(inst_code) == 0:
        print("Check institution abbreviation in conn.py"); exit()

    ## Step 1: Generate XML files
    if str2bool(args.generate_stat):
        generate_xmls(dbpassword = dbpassword, encryption_key = encryption_key, date_start=date_start, date_end=date_end, partsnamelist=partsnamelist)
        find_missing_var_xml(time_limit=90)
    ## Step 2: SCP files to central DB

    db_list = []
    if upload_prod_stat:
        db_list.append('prod_db')
        db_type = 'cmsr'
    if upload_dev_stat:
        db_list.append('dev_db')
        db_type = 'int2r'
    
    if upload_dev_stat or upload_prod_stat:
        for cerndb in db_list:
            ret = True and scp_files(lxplus_username = lxplus_username, lxplus_password = lxplus_password, directory = directory_to_search, search_date = today, encryption_key = encryption_key, cerndb = cerndb)
        if ret:
            await check_upload(db_type)
            # Step 3: Delete generated XMLs on success
        if ret and str2bool(args.del_xml):
            clean_generated_xmls()
if __name__ == '__main__':
    asyncio.run(main())
