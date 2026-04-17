'''
1. generate xmls 
2. save them into a tentative directory ('generated_xmls'/)
3. scp to central db 
4. check the log in central db 
5. if sucess, delete the generated xmls
'''

import os, sys, argparse, base64, subprocess, traceback, asyncio
import shutil, pwinput, datetime, yaml, time
from cryptography.fernet import Fernet
from src import process_xml_list, str2bool
from find_missing_var_xml import find_missing_var_xml
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..')))


XML_GENERATOR_DIR = 'export_data/generate_xmls_utils'## directory for py scripts to generate xmls
GENERATED_XMLS_DIR = 'export_data/xmls_for_upload'##  directory to store the generated xmls. Feel free to change it. 

# Ensure the generated XML directory exists
os.makedirs(GENERATED_XMLS_DIR, exist_ok=True)

def run_script(script_path, dbpassword, date_start, date_end, lxplus_username, output_dir=GENERATED_XMLS_DIR, encryption_key=None, partsnamelist=None, cerndb=None, skip_uploaded=True):
    """Run a Python script as a subprocess."""
    # process = subprocess.run([sys.executable, script_path])
    command = [
        sys.executable, script_path,
        '-dbp', dbpassword,
        '-dir', output_dir,
        '-k', encryption_key,
        '-datestart', date_start,
        '-dateend', date_end,
        '-lxu', lxplus_username]
    if partsnamelist:
        command += ['-pn'] + partsnamelist
    if cerndb:
        command += ['-cerndb', cerndb]
    command += ['-skup', str(skip_uploaded)]

    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        traceback.print_exc()
        print(f"Error occurred while running the script: {e}")

def generate_xmls(dbpassword, date_start, date_end, lxplus_username, encryption_key=None, partsnamelist=None, cern_auto_upload=None, cerndb='prod_db', skip_uploaded=True):
    """Recursively loop through specific subdirectories under generate_xmls directory and run all Python scripts."""
    tasks = []
    # Specific subdirectories to process
    xml_list = process_xml_list(get_yaml_data = True, cern_auto_upload=cern_auto_upload)
    subdirs = list(xml_list.keys())    # subdirs = ['baseplate', 'hexaboard', 'module', 'protomodule', 'sensor', 'testing']
    scripts_to_run = []
    building_module, building_proto = False, False ## initialize 

    for subdir in subdirs:
        subdir_path = os.path.join(XML_GENERATOR_DIR, subdir)
        
        if os.path.exists(subdir_path):
            # for file in os.listdir(subdir_path):
            for filetype in list(xml_list[subdir]):
                file_suff = list(filetype.keys())[0]
                file = f"generate_{file_suff}.py"
                if filetype[file_suff] and os.path.exists(os.path.join(subdir_path, file)):                
                    if 'module_build' in file: building_module = True
                    if 'proto_build'  in file:  building_proto = True
                    ## We only upload build_upload.xml for all parts EXCEPT protomodule and modules.    
                    if subdir_path.split('/')[-1] not in ['protomodule', 'module'] and (file.endswith('build_xml.py') == True):
                        print(f'subdir_path to skip -- {subdir_path}')
                        continue; 
                    script_path = os.path.join(subdir_path, file)
                    scripts_to_run.append(script_path)
    
    if partsnamelist and building_module and building_proto:  ### add protomodule serial number if both module and protomodule are being built for a given module
        proto_parts_list = [part.replace('320M', '320P') for part in partsnamelist if '320M' in part] 
        partsnamelist.extend(proto_parts_list)
    
    #Run all the scripts asynchronously
    total_scripts = len(scripts_to_run)
    completed_scripts = 0
    for script_path in scripts_to_run:
        is_build = 'module_build' in script_path or 'proto_build' in script_path
        run_script(script_path=script_path, dbpassword=dbpassword, encryption_key=encryption_key, date_start=date_start, date_end=date_end, lxplus_username=lxplus_username, partsnamelist=partsnamelist, cerndb=cerndb if is_build else None, skip_uploaded=skip_uploaded)
        completed_scripts += 1
        print('-'*10)
        print(f'Executed -- {script_path}.')
        print(f"Progress: {completed_scripts}/{total_scripts} XML file types generated.")
        print('-'*10); print('')

def scp_files(lxplus_username, directory, search_date, cerndb = 'dev_db', cern_auto_upload = False, dbpassword = None, encryption_key = None, del_xml = 'True'):
    """Call the scp script to transfer files."""
    consolidated_csv = None
    try:
        scp_command = [sys.executable,
                       'export_data/dbloader_scp_xml.py',
                       '-lxu', lxplus_username,
                       '-dir', directory,
                       '-date', str(search_date),
                       '-autoupload', str(cern_auto_upload),
                       '-cerndb', cerndb,
                       '-dbp', dbpassword,
                       '-k',   encryption_key,
                       '-delx', str(del_xml)]

        process = subprocess.run(scp_command, check=True, capture_output=True, text=True)
        sys.stdout.write(process.stdout)
        sys.stdout.flush()

        lines = process.stdout.splitlines()
        for i, line in enumerate(lines):
            if '----> Consolidated logs saved:' in line:
                for subsequent_line in lines[i+1:]:
                    stripped = subsequent_line.strip()
                    if stripped.endswith('.csv'):
                        consolidated_csv = stripped
                        break
                break

        return True, consolidated_csv

    except subprocess.CalledProcessError as e:
        sys.stdout.write(e.stdout or '')
        sys.stdout.flush()
        sys.stderr.write(e.stderr or '')
        sys.stderr.flush()
        print(f"Error during SCP (exit code {e.returncode}): {e}")
        return False, consolidated_csv
    except Exception as e:
        traceback.print_exc()
        print(f"Error during SCP: {e}")
        return False, consolidated_csv

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
    parser.add_argument('-k', '--encrypt_key', default=None, required=False, help="The encryption key")
    parser.add_argument('-dir','--directory', type=valid_directory, default=GENERATED_XMLS_DIR, help=f"The directory to process. Default is {GENERATED_XMLS_DIR}.")
    # parser.add_argument('-date', '--date', type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d').date(), default=today, help=f"Date for XML generated (format: YYYY-MM-DD). Default is today's date: {today}")
    parser.add_argument('-datestart', '--date_start', type=lambda s: str(datetime.datetime.strptime(s, '%Y-%m-%d').date()), default=str(today), help=f"Date for XML generated (format: YYYY-MM-DD). Default is today's date: {today}")
    parser.add_argument('-dateend', '--date_end', type=lambda s: str(datetime.datetime.strptime(s, '%Y-%m-%d').date()), default=str(today), help=f"Date for XML generated (format: YYYY-MM-DD). Default is today's date: {today}")
    parser.add_argument('-gen', '--generate_stat', default='True', required=False, help="Generate XMLs.")
    parser.add_argument('-upld', '--upload_dev_stat', default='False', required=False, help="Upload to dev DBLoader without generate.")
    parser.add_argument('-uplp', '--upload_prod_stat', default='True', required=False, help="Upload to prod DBLoader without generate.")
    parser.add_argument('-delx', '--del_xml', default='False', required=False, help="Delete XMLs after upload.")
    parser.add_argument("-pn", '--partnameslist', nargs="+", help="Space-separated list", required=False)
    parser.add_argument('-autoupload', '--cern_auto_upload', default='False', required=False, help="True if the upload is automated via a service account")
    parser.add_argument('-skup', '--skip_uploaded', default='True', required=False, help="Skip parts already uploaded to CERN DB")

    args = parser.parse_args()

    dbpassword = args.dbpassword or pwinput.pwinput(prompt='Enter database shipper password: ', mask='*')
    lxplus_username = args.dbl_username or pwinput.pwinput(prompt='Enter lxplus username: ', mask='*')
    # lxplus_password = args.dbl_password or pwinput.pwinput(prompt='Enter lxplus password: ', mask='*')
    directory_to_search = args.directory
    date_start = args.date_start
    date_end = args.date_end
    encryption_key = args.encrypt_key
    upload_dev_stat = str2bool(args.upload_dev_stat)
    upload_prod_stat = str2bool(args.upload_prod_stat)
    partsnamelist = args.partnameslist
    skip_uploaded = str2bool(args.skip_uploaded)


    inst_code  = (yaml.safe_load(open(os.path.join('dbase_info', 'conn.yaml'), 'r'))).get('institution_abbr')
    if len(inst_code) == 0:
        print("Check institution abbreviation in conn.py"); exit()

        ## Step 1: Generate XML files
    cerndb = 'dev_db' if upload_dev_stat else 'prod_db'
    if str2bool(args.generate_stat):
        generate_xmls(dbpassword=dbpassword, encryption_key=encryption_key, date_start=date_start, date_end=date_end, lxplus_username=lxplus_username, partsnamelist=partsnamelist, cern_auto_upload=str2bool(args.cern_auto_upload), cerndb=cerndb, skip_uploaded=skip_uploaded)
        find_missing_var_xml(time_limit=90)
        print("Waiting 3 seconds ...")
        time.sleep(3) ### XMLs take a few seconds to get saved
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
            scp_success, consolidated_csv = scp_files(lxplus_username=lxplus_username, directory=directory_to_search, search_date=today, cerndb=cerndb, cern_auto_upload=str2bool(args.cern_auto_upload), dbpassword=dbpassword, encryption_key=encryption_key, del_xml=args.del_xml)
        
        if scp_success and upload_prod_stat and consolidated_csv:
            command = [sys.executable, "export_data/check_successful_upload.py", "--consolidated_csv",  consolidated_csv , "--dbpassword", dbpassword, "--encrypt_key", encryption_key or "",  "-uplp", "True", "-delx", args.del_xml]
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            sys.stdout.write(result.stdout)
            sys.stdout.flush()
            if result.stderr:
                print("check_successful_upload.py errors:\n", result.stderr)

if __name__ == '__main__':
    asyncio.run(main())

