'''
1. generate xmls 
2. save them into a tentative directory ('generated_xmls'/)
3. scp to central db 
4. check the log in central db 
5. if sucess, delete the generated xmls
'''

import os, sys
import subprocess
import shutil

XML_GENERATOR_DIR = 'export/generate_xmls_utils'## directory for py scripts to generate xmls
GENERATED_XMLS_DIR = 'export/xmls_for_upload'##  directory to store the generated xmls. Feel free to change it. 

# Ensure the generated XML directory exists
os.makedirs(GENERATED_XMLS_DIR, exist_ok=True)
def run_script(script_path, output_dir=GENERATED_XMLS_DIR):
    """Run a Python script as a subprocess."""
    # process = subprocess.run([sys.executable, script_path])
    process = subprocess.run([sys.executable, script_path, output_dir], check=True)


def generate_xmls():
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
                # if file.endswith('.py'):
                #     script_path = os.path.join(subdir_path, file)
                #     scripts_to_run.append(script_path)
    
    # Run all the scripts asynchronously
    total_scripts = len(scripts_to_run)
    completed_scripts = 0
    for script_path in scripts_to_run:
        run_script(script_path)
        completed_scripts += 1
        print('-'*10)
        print(f'Executed -- {script_path}.')
        print(f"Progress: {completed_scripts}/{total_scripts} scripts completed")
        print('-'*10)
        break

def scp_files():
    """Call the scp script to transfer files."""
    try:
        scp_command = ['python3', 'export/dbloader_scp_xml.py', '--dir', GENERATED_XMLS_DIR]
        process = subprocess.run([sys.executable,'export/dbloader_scp_xml.py', '--dir', GENERATED_XMLS_DIR])

        # Continuously capture and print the output
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                print(output.strip())  # Print the output from the subprocess in real-time

        # Check for errors
        stderr_output = process.stderr.read()
        if stderr_output:
            print(f"Error from dbloader_scp_xml.py: {stderr_output}")
            return False

        return process.returncode == 0
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

def main():
    # Step 1: Generate XML files
    generate_xmls()

    # Step 2: SCP files to central DB
    if scp_files():
        # Step 3: Delete generated XMLs on success
        clean_generated_xmls()

if __name__ == '__main__':
    main()
