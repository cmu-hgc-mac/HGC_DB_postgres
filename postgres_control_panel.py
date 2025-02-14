import os, yaml, base64, sys, threading, atexit, signal
from pathlib import Path
from cryptography.fernet import Fernet
import subprocess, webbrowser, zipfile, urllib.request, traceback
import tkinter
from tkinter import Tk, Button, Checkbutton, Label, messagebox, Frame, Toplevel, Entry, IntVar, StringVar, BooleanVar, Text, LabelFrame, Radiobutton, filedialog, OptionMenu
from tkinter import END, DISABLED, Label as Label
from datetime import datetime

def run_git_pull_seq():
    result = subprocess.run(["git", "pull"], capture_output=True, text=True)
    if result.returncode == 0:
        print("Git pull successful ..."); print(result.stdout)
    else:
        print("Git pull failed ..."); print(result.stderr); exit()

try:
    run_git_pull_seq()
except:
    print("There is a git conflict but continue.")

from export_data.src import process_xml_list, update_yaml_with_checkboxes
process_xml_list()

encryption_key = Fernet.generate_key()
cipher_suite = Fernet(encryption_key) ## Generate or load a key. 
adminer_process_button_face = " Search/Edit Data  "
loc = 'dbase_info'
conn_yaml_file = os.path.join(loc, 'conn.yaml')
config_data  = yaml.safe_load(open(conn_yaml_file, 'r'))
dbase_name = config_data.get('dbname')
db_hostname = config_data.get('db_hostname')
cern_dbase = config_data.get('cern_db')
php_port = config_data.get('php_port', '8083')
php_url = f"http://127.0.0.1:{php_port}/adminer-pgsql.php?pgsql={db_hostname}&username=viewer&db={dbase_name}"

def get_pid_result():
    try:
        return subprocess.run(["lsof", "-ti", f":{php_port}"], capture_output=True, text=True)
    except:
        return subprocess.run(["cmd", "/c", f'netstat -ano | findstr :{php_port}'], capture_output=True, text=True) ### for Windows

def bind_button_keys(button):
    button.bind("<Return>", lambda event: button.invoke())  # Bind Enter key

# Synchronous functions for button actions
def donothing(): False

def upload_action():
    show_message("Currently under development...")

def show_message_textbox(message):
    window = Toplevel()
    window.title("Configuration Details")
    from tkinter import scrolledtext
    text_area = scrolledtext.ScrolledText(window, wrap=tkinter.WORD, width=80, height=20)
    text_area.pack(padx=10, pady=10, fill=tkinter.BOTH, expand=True)
    text_area.insert(END, message)
    text_area.config(state=DISABLED)

def check_config_action():  
    message = f'Database configuration variables in "{os.path.join("HGC_DB_postgres",conn_yaml_file)}".\n\n'
    message += "\n".join(f"{key}: {value}" for key, value in config_data.items())
    show_message_textbox(message)

# import time
# def print_action():
#     time.sleep(1)  # Simulate a time-consuming task
#     show_message("Printing...")

def show_message(message):
    messagebox.showinfo("Action", message)

# Function to exit the application
def exit_application():
    process_xml_list()
    root.quit()  # Exit the application

# Load image
def load_image(image_path):
    if os.path.exists(image_path):
        from PIL import Image, ImageTk  # Import here to avoid error if Pillow is not installed
        image = Image.open(image_path)
        return ImageTk.PhotoImage(image)
    else:
        print(f"Logo not found: {image_path}")
        return None

def create_database():
    input_window = Toplevel(root)
    input_window.title("Input Required")
    # Field 1: Database Name
    Label(input_window, text="Set initial: VIEWER password (only read):").pack(pady=5)
    viewer_var = StringVar()
    viewer_var_entry = Entry(input_window, textvariable=viewer_var, width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    viewer_var_entry.pack(pady=5)
    # Field 2: Username
    Label(input_window, text="Set initial: USER password (write access):").pack(pady=5)
    user_var = StringVar()
    user_var_entry = Entry(input_window, textvariable=user_var, width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    user_var_entry.pack(pady=5)
    # Field 3: Password (hidden input)
    Label(input_window, text="**Enter postgres password:** (for modifications)").pack(pady=5)
    password_var = StringVar()
    password_entry = Entry(input_window, textvariable=password_var, show='*', width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    password_entry.pack(pady=5)

    def submit_create():
        viewer_pass = viewer_var.get()
        user_pass = user_var.get()
        db_pass = base64.urlsafe_b64encode( cipher_suite.encrypt((password_var.get()).encode()) ).decode() if password_var.get().strip() else ""  ## Encrypt password and then convert to base64
        if db_pass.strip():
            input_window.destroy()  # Close the input window
            # Run the subprocess command
            subprocess.run([sys.executable, "create/create_database.py", "-p", db_pass, "-up", user_pass, "-vp", viewer_pass, "-k", encryption_key])
            subprocess.run([sys.executable, "create/create_tables.py", "-p", db_pass, "-k", encryption_key])
            subprocess.run([sys.executable, "modify/modify_table.py", "-p", db_pass, "-k", encryption_key])
            show_message(f"Check terminal for PostgreSQL database tables. Refresh pgAdmin4.")
        else:
            if messagebox.askyesno("Input Error", "Do you want to cancel? \nDatabase password cannot be empty."):
                input_window.destroy()  

    submit_create_button = Button(input_window, text="Submit", command=submit_create)
    submit_create_button.pack(pady=10)
    bind_button_keys(submit_create_button)

def verify_shipin():
    input_window = Toplevel(root)
    input_window.title("Verify received components")
    Label(input_window, text="Enter local db USER password:").pack(pady=5)
    shipper_var = StringVar()
    shipper_var_entry = Entry(input_window, textvariable=shipper_var, show='*', width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    shipper_var_entry.pack(pady=5)

    today_date = datetime.now()
    Label(input_window, text="Shipment verification date (approx.)").pack(pady=5)
    shipindate_var = StringVar(master=input_window, value=today_date.strftime("%Y-%m-%d"))
    shipindate_var_entry = Entry(input_window, textvariable=shipindate_var, width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    shipindate_var_entry.pack(pady=5)
    
    def activate_geom():
        if selected_component.get() != 'sensor':
            geom_dropdown.config(state="disabled")
        else:
            geom_dropdown.config(state="normal")

    Label(input_window, text="Type of component:").pack(pady=5)
    selected_component = StringVar(value="baseplate")
    radio_bp = Radiobutton(input_window, text="baseplate", variable=selected_component, value="baseplate", command=activate_geom)
    radio_bp.pack(anchor="w", pady=2, padx=80)
    radio_hxb = Radiobutton(input_window, text="hexaboard", variable=selected_component, value="hexaboard", command=activate_geom)
    radio_hxb.pack(anchor="w", pady=2, padx=80)
    radio_sen = Radiobutton(input_window, text="sensor -- select geometry", variable=selected_component, value="sensor", command=activate_geom)
    radio_sen.pack(anchor="w", pady=2, padx=80)
    densgeomframe = Frame(input_window)
    densgeomframe.pack(pady=5)

    geom_options = ["Full", "Top", "Bottom", "Left", "Right", "Five"]
    selected_geom = StringVar(value=geom_options[0])
    geom_dropdown = OptionMenu(densgeomframe, selected_geom, *geom_options)
    geom_dropdown.pack(side="left", padx=10, pady=2)
    geom_dropdown.config(state="disabled")
    
    Label(input_window, text="Please physically verify the reception of each component at your MAC.", fg="red",wraplength=270).pack(pady=5)

    def enter_part_barcodes():
        entries = []
        abspath = os.path.dirname(os.path.abspath(__file__))
        temptextfile = str(os.path.join(abspath, "shipping","temporary_part_entries.txt"))
        dbshipper_pass = base64.urlsafe_b64encode( cipher_suite.encrypt( (shipper_var.get()).encode()) ).decode() if shipper_var.get().strip() else "" ## Encrypt password and then convert to base64
        if dbshipper_pass.strip() and shipindate_var.get().strip() and selected_component.get():
            popup1 = Toplevel(); popup1.title("Enter Barcode of Parts")
            def verify_components():
                popup1.destroy() 
                subprocess.run([sys.executable, "shipping/verify_received_components.py", "-p", dbshipper_pass, "-k", encryption_key, "-pt", str(selected_component.get()), "-fp", str(temptextfile), "-dv", str(shipindate_var.get()), "-geom" , str(selected_geom.get())])

            def save_entries():
                with open("shipping/temporary_part_entries.txt", "w") as file:
                    for entry in entries:
                        text = entry.get().strip()
                        if text: file.write(text + "\n")
                verify_components()

            for i in range(10):
                listlabel = Label(popup1, text=f"{i + 1}:")
                listlabel.grid(row=i, column=0, padx=10, pady=0, sticky="w")
                entry = Entry(popup1, width=30)
                entry.grid(row=i, column=1, padx=10, pady=0)
                entries.append(entry)

            submit_button = Button(popup1, text="Submit to DB", command=save_entries)
            submit_button.grid(row=10, column=0, columnspan=2, pady=10)
        else:
            if messagebox.askyesno("Input Error", "Do you want to cancel?\nDatabase password, part type and date cannot be empty."):
                input_window.destroy()  

    def upload_file_with_part():
        dbshipper_pass = base64.urlsafe_b64encode( cipher_suite.encrypt( (shipper_var.get()).encode()) ).decode() if shipper_var.get().strip() else "" ## Encrypt password and then convert to base64
        if dbshipper_pass.strip() and shipindate_var.get().strip() and selected_component.get():
            popup2 = Toplevel()
            popup2.title("Upload text/csv file with component names")
            file_entry = None
            
            def browse_file():
                file_path = filedialog.askopenfilename(title="Select a File")
                if file_path:
                    file_entry.delete(0, 'end')  # Clear the current entry
                    file_entry.insert(0, file_path)

            browse_button = Button(popup2, text="Browse", command=browse_file)
            browse_button.pack(pady=10)
            file_entry = Entry(popup2, width=50, bd=2)
            file_entry.pack(pady=10)

            def verify_components():
                if file_entry.get().strip():
                    subprocess.run([sys.executable, "shipping/verify_received_components.py", "-p", dbshipper_pass, "-k", encryption_key, "-pt", str(selected_component.get()), "-fp", str(file_entry.get()), "-dv", str(shipindate_var.get()), "-geom" , str(selected_geom.get())])
                    popup2.destroy()  

            submit_fileparts_button = Button(popup2, text="Submit to DB", command=verify_components)
            submit_fileparts_button.pack(pady=10)
            bind_button_keys(submit_fileparts_button)
        else:
            if messagebox.askyesno("Input Error", "Do you want to cancel?\nDatabase password, part type and date cannot be empty."):
                input_window.destroy()  

    enter_verify_button = Button(input_window, text="Enter barcodes of (up to 10) individual parts", command=enter_part_barcodes)
    enter_verify_button.pack(pady=10, padx=0)
    bind_button_keys(enter_verify_button)
    # enter_verify_button.config(state='disabled')
    Label(input_window, text="Or").pack(pady=5)
    upload_verfile_button = Button(input_window, text="Upload text/csv file with part barcodes", command=upload_file_with_part)
    upload_verfile_button.pack(pady=10)
    bind_button_keys(upload_verfile_button)
    

def import_data():
    # run_git_pull_seq()
    input_window = Toplevel(root)
    input_window.title("Input Required")

    Label(input_window, text="Enter local db USER password:").pack(pady=5)
    shipper_var = StringVar()
    shipper_var_entry = Entry(input_window, textvariable=shipper_var, show='*', width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    shipper_var_entry.pack(pady=5)

    # Label(input_window, text="Enter lxplus username:").pack(pady=5)
    # lxuser_var = StringVar()
    # lxuser_var_entry = Entry(input_window, textvariable=lxuser_var, width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    # lxuser_var_entry.pack(pady=5)

    # Label(input_window, text="Enter lxplus password:").pack(pady=5)
    # lxpassword_var = StringVar()
    # lxpassword_entry = Entry(input_window, textvariable=lxpassword_var, show='*', width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    # lxpassword_entry.pack(pady=5)

    download_dev_var = BooleanVar(value=False)
    download_dev_var_entry = Checkbutton(input_window, text="download from INT2R (DEV-DB)", variable=download_dev_var)
    download_dev_var_entry.pack(pady=5)
    download_prod_var = BooleanVar(value=True)
    download_prod_var_entry = Checkbutton(input_window, text="download from CMSR (PROD-DB)", variable=download_prod_var)
    download_prod_var_entry.pack(pady=2)

    def submit_import():
        dbshipper_pass = base64.urlsafe_b64encode( cipher_suite.encrypt( (shipper_var.get()).encode()) ).decode() if shipper_var.get().strip() else "" ## Encrypt password and then convert to base64
        # lxuser_pass = lxuser_var.get()
        # lxpassword_pass = lxpassword_var.get()
        download_dev_stat = download_dev_var.get()
        download_prod_stat = download_prod_var.get()

        if not download_prod_stat and not download_dev_stat:
            if messagebox.askyesno("Input Error", "Do you want to cancel?\nSelect a source database."):
                input_window.destroy()  
        else:
            if dbshipper_pass.strip(): # and lxuser_pass.strip() and lxpassword_pass.strip():
                input_window.destroy()  
                subprocess.run([sys.executable, "import_data/get_parts_from_hgcapi.py", "-p", dbshipper_pass, "-k", encryption_key, "-downld", str(download_dev_stat), "-downlp", str(download_prod_stat)])
                # subprocess.run([sys.executable, "housekeeping/update_tables_data.py", "-p", dbshipper_pass, "-k", encryption_key])
                # subprocess.run([sys.executable, "housekeeping/update_foreign_key.py", "-p", dbshipper_pass, "-k", encryption_key])
                show_message(f"Data imported from HGCAPI. Refresh pgAdmin4.")
            else:
                if messagebox.askyesno("Input Error", "Do you want to cancel?\nDatabase password cannot be empty."):
                    input_window.destroy()  

    submit_import_button = Button(input_window, text="Submit", command=submit_import)
    submit_import_button.pack(pady=10)
    bind_button_keys(submit_import_button)

def export_data():
    # run_git_pull_seq()
    input_window = Toplevel(root)
    input_window.title("Input Required")
    Label(input_window, text="**Enter local DB USER password:**").pack(pady=5)
    shipper_var = StringVar()
    shipper_var_entry = Entry(input_window, textvariable=shipper_var, show='*', width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    shipper_var_entry.pack(pady=5)
    Label(input_window, text="**Enter LXPLUS username:**").pack(pady=5)
    lxuser_var = StringVar()
    lxuser_var_entry = Entry(input_window, textvariable=lxuser_var, width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    lxuser_var_entry.pack(pady=5)
    Label(input_window, text="**Enter LXPLUS password:**").pack(pady=5)
    lxpassword_var = StringVar()
    lxpassword_entry = Entry(input_window, textvariable=lxpassword_var, show='*', width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    lxpassword_entry.pack(pady=5)

    today_date = datetime.now()
    Label(input_window, text="Start date").pack(pady=5)
    startdate_var = StringVar(master=input_window, value=today_date.strftime("%Y-%m-%d"))
    startdate_var_entry = Entry(input_window, textvariable=startdate_var, width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    startdate_var_entry.pack(pady=5)
    Label(input_window, text="End date").pack(pady=5)
    enddate_var = StringVar(master=input_window, value=today_date.strftime("%Y-%m-%d"))
    enddate_var_entry = Entry(input_window, textvariable=enddate_var, width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    enddate_var_entry.pack(pady=5)

    generate_var = BooleanVar(value=True)
    generate_var_entry = Checkbutton(input_window, text="Generate XML files", variable=generate_var)
    generate_var_entry.pack(pady=5)
    upload_dev_var = BooleanVar(value=True)
    upload_dev_var_entry = Checkbutton(input_window, text="Upload to INT2R (DEV-DB)", variable=upload_dev_var)
    upload_dev_var_entry.pack(pady=5)
    upload_prod_var = BooleanVar(value=False)
    upload_prod_var_entry = Checkbutton(input_window, text="Upload to CMSR (PROD-DB)", variable=upload_prod_var)
    upload_prod_var_entry.pack(pady=2)
    deleteXML_var = BooleanVar(value=False)
    deleteXML_var_entry = Checkbutton(input_window, text="Delete XMLs after upload", variable=deleteXML_var)
    deleteXML_var_entry.pack(pady=5)
    
    def select_specific():
        popup = Toplevel()
        popup.title("Select XMLs")
        
        xml_list = process_xml_list(get_yaml_data = True)
        checkbox_vars = {}
        
        def create_checkboxes(xml_list, parent):
            if isinstance(xml_list, dict):
                for key, value in xml_list.items():
                    frame = LabelFrame(parent, text=key, padx=5, pady=5)
                    frame.pack(fill="x", expand=True, padx=10, pady=5)
                    checkbox_vars[key] = create_checkboxes(value, frame)
            elif isinstance(xml_list, list):
                vars_list = []
                for item in xml_list:
                    for key, value in item.items():
                        var = IntVar(value=1 if value else 0)
                        checkbox = Checkbutton(parent, text=key, variable=var)
                        checkbox.pack(anchor="w", padx=10, pady=2)
                        vars_list.append({key: var})
                return vars_list
            return {}

        create_checkboxes(xml_list, popup)
        toggle_state = {"all_selected": True} 

        def toggle_all():
            toggle_state["all_selected"] = not toggle_state["all_selected"]
            new_state = 1 if toggle_state["all_selected"] else 0

            def apply_toggle(data):
                if isinstance(data, list):
                    for item in data:
                        for _, var in item.items():
                            var.set(new_state)
                elif isinstance(data, dict):
                    for value in data.values():
                        apply_toggle(value)

            apply_toggle(checkbox_vars)

        toggle_all_button = Button(popup, text="(De)Select All", command=toggle_all)
        toggle_all_button.pack(pady=10)

        def submit_selection():
            updated_data = update_yaml_with_checkboxes(xml_list = xml_list, checkbox_vars=checkbox_vars)
            process_xml_list(updated_data)
            popup.destroy()
        
        submit_select_button = Button(popup, text="Submit", command=submit_selection)
        submit_select_button.pack(pady=10)
        
    def submit_export():
        lxp_username = lxuser_var.get()
        dbshipper_pass = base64.urlsafe_b64encode( cipher_suite.encrypt( (shipper_var.get()).encode()) ).decode() if shipper_var.get().strip() else "" ## Encrypt password and then convert to base64
        lxp_password = base64.urlsafe_b64encode( cipher_suite.encrypt( (lxpassword_var.get()).encode()) ).decode() if lxpassword_var.get().strip() else "" ## Encrypt password and then convert to base64
        generate_stat = generate_var.get()
        upload_dev_stat = upload_dev_var.get()
        upload_prod_stat = upload_prod_var.get()
        deleteXML_stat = deleteXML_var.get()

        if not upload_dev_stat and not upload_prod_stat:
            lxp_username, lxp_password = 'na', 'na'

        if dbshipper_pass.strip() and lxp_username.strip() and lxp_password.strip():
            input_window.destroy()  
            # subprocess.run([sys.executable, "housekeeping/update_tables_data.py", "-p", dbshipper_pass, "-k", encryption_key])
            # subprocess.run([sys.executable, "housekeeping/update_foreign_key.py", "-p", dbshipper_pass, "-k", encryption_key])
            subprocess.run([sys.executable, "export_data/export_pipeline.py", "-dbp", dbshipper_pass, "-lxu", lxp_username, "-lxp", lxp_password, "-k", encryption_key, "-gen", str(generate_stat), "-upld", str(upload_dev_stat), "-uplp", str(upload_prod_stat), "-delx", str(deleteXML_stat), "-datestart", str(startdate_var.get()), "-dateend", str(enddate_var.get())])
            show_message(f"Check terminal for upload status. Refresh pgAdmin4.")
        else:
            if messagebox.askyesno("Input Error", "Do you want to cancel?\nDatabase password cannot be empty."):
                input_window.destroy()  
        

    select_specific_button = Button(input_window, text="Select type of XMLs", command=select_specific)
    select_specific_button.pack(pady=10)
    bind_button_keys(select_specific_button)

    submit_export_button = Button(input_window, text="Submit", command=submit_export)
    submit_export_button.pack(pady=10)
    bind_button_keys(submit_export_button)

def refresh_data():
    # run_git_pull_seq()
    input_window = Toplevel(root)
    input_window.title("Input Required")
    Label(input_window, text="Enter local db USER password:").pack(pady=5)
    shipper_var = StringVar()
    shipper_var_entry = Entry(input_window, textvariable=shipper_var, show='*', width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    shipper_var_entry.pack(pady=5)

    def submit_refresh():
        dbshipper_pass = base64.urlsafe_b64encode( cipher_suite.encrypt( (shipper_var.get()).encode()) ).decode() if shipper_var.get().strip() else ""  ## Encrypt password and then convert to base64
    
        if dbshipper_pass.strip():
            input_window.destroy()  
            subprocess.run([sys.executable, "housekeeping/update_tables_data.py", "-p", dbshipper_pass, "-k", encryption_key])
            subprocess.run([sys.executable, "housekeeping/update_foreign_key.py", "-p", dbshipper_pass, "-k", encryption_key])
            print("******** Database refreshed ********")
            show_message(f"Check terminal and refresh pgAdmin4.")
        else:
            if messagebox.askyesno("Input Error", "Do you want to cancel?\nDatabase password cannot be empty."):
                input_window.destroy()  

    submit_refresh_button = Button(input_window, text="Submit", command=submit_refresh)
    submit_refresh_button.pack(pady=10)
    bind_button_keys(submit_refresh_button)

def open_adminerevo():   ### lsof -i :8083; kill <pid>
    def close_adminer_process():
        try:
            pids = get_pid_result().stdout.strip().split("\n")
            for pid in pids:
                if pid.isdigit():
                    os.kill(int(pid), signal.SIGTERM)
            print("Closed Adminer process.")
        except Exception as e:
            print(f"Error: {e}")

    if 'search' in button_search_data.config('text')[-1].lower():
        adminer_php_file = 'adminer-pgsql.php'
        if not os.path.exists(adminer_php_file):
            try:
                url = "https://download.adminerevo.org/latest/adminer/adminer-pgsql.zip"
                adminer_zip_file = adminer_php_file.replace('.php','.zip')
                urllib.request.urlretrieve(url, adminer_zip_file)
                with zipfile.ZipFile(adminer_zip_file, 'r') as zip_ref:
                    zip_ref.extractall() 
                if os.path.exists(adminer_zip_file): os.remove(adminer_zip_file)
            except Exception as e:
                print(e)
 
        try:
            adminer_process = subprocess.Popen(["php", "-S", f"127.0.0.1:{php_port}", "-t", "."], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL, start_new_session=True)
            webbrowser.open(php_url)
            button_search_data.config(text="Stop AdminerEvo", fg="red")
            print('AdminerEvo opened in browser...')
            print(php_url)
        except Exception as e:
            traceback.print_exc()
            print('\n*** PHP Installation Instructions at', 'https://github.com/cmu-hgc-mac/HGC_DB_postgres/blob/main/documentation/php_installation.md ***')
            webbrowser.open(f"https://github.com/cmu-hgc-mac/HGC_DB_postgres/blob/main/documentation/php_installation.md")
    else:
        close_adminer_process()
        button_search_data.config(text=adminer_process_button_face, fg="black")    
    


# Create a helper function to handle button clicks
def handle_button_click(action):
    threading.Thread(target=action).start()

# Initialize the application
root = Tk()
root.title("Local DB Control Panel - CMS HGC MAC")
root.geometry("400x550")

# Load logo image
image_path = "documentation/images/logo_small_75.png"  # Update with your image path
logo_image = load_image(image_path)

button_width, button_height = 25, 3
small_button_width, small_button_height = 22, 1

# Create a frame for the layout using grid
frame = Frame(root)
frame.grid(row=0, column=0, padx=10, pady=10, sticky='nsew')
for row in range(3):  # Assuming you have 3 rows
    frame.grid_rowconfigure(row, weight=1)

# Configure grid layout to expand in all directions
root.grid_rowconfigure(0, weight=1)
root.grid_columnconfigure(0, weight=1)

# Add logo or fallback label
if logo_image:
    logo_label = Label(frame, image=logo_image, anchor="w")
    logo_label.grid(row=0, column=0, padx=10, rowspan=2)
else:
    logo_label = Label(frame, text="Carnegie Mellon University")
    logo_label.grid(row=0, column=0, padx=10, rowspan=2)

# Add buttons with grid layout
button_create = Button(frame, text="Create/Modify DBase Tables", command=create_database, width=small_button_width, height=small_button_height)
button_create.grid(row=0, column=1, pady=5)

button_check_config = Button(frame, text="Check Config", command=check_config_action, width=small_button_width, height=small_button_height)
button_check_config.grid(row=1, column=1, pady=5)

# spacer = Frame(frame, height=10)  # Spacer with height (for vertical spacing)
# spacer.grid(row=2, column=1, pady=10)

button_shipin = Button(frame, text="Verify received shipment ", command=verify_shipin, width=button_width, height=button_height)
button_shipin.grid(row=3, column=1, pady=5, sticky='ew')

button_download = Button(frame, text="    Import Parts Data      ", command=import_data, width=button_width, height=button_height)
button_download.grid(row=4, column=1, pady=5, sticky='ew')

button_upload_xml = Button(frame, text=" Upload XMLs to DBLoader ", command=export_data, width=button_width, height=button_height)
button_upload_xml.grid(row=5, column=1, pady=5, sticky='ew')
# button_upload_xml.config(state='disabled')

button_shipout = Button(frame, text="   Outgoing shipment     ", command=refresh_data, width=button_width, height=button_height)
button_shipout.grid(row=6, column=1, pady=5, sticky='ew')
button_shipout.config(state="disabled")

button_refresh_db = Button(frame, text=" Refresh local database     ", command=refresh_data, width=button_width, height=button_height)  
button_refresh_db.grid(row=7, column=1, pady=5, sticky='ew')

button_search_data = Button(frame, text=adminer_process_button_face, command=open_adminerevo, width=button_width, height=button_height) 
button_search_data.grid(row=8, column=1, pady=5, sticky='ew')

for pid in get_pid_result().stdout.strip().split("\n"):
    if pid.isdigit():
        button_search_data.config(text="Stop AdminerEvo", fg="red")
    else:
        button_search_data.config(text=adminer_process_button_face, fg="black")
        
# Configure grid to ensure all rows expand with window resize
for i in range(10):
    frame.grid_rowconfigure(i, weight=1)

# Documentation link at the bottom
def open_documentation():
    webbrowser.open("https://github.com/cmu-hgc-mac/HGC_DB_postgres?tab=readme-ov-file#hgc_db_postgres")

doc_label = Label(root, text="Documentation", fg="blue", cursor="hand2")
doc_label.grid(row=1, column=0, sticky="s")
doc_label.bind("<Button-1>", lambda e: open_documentation())

root.protocol("WM_DELETE_WINDOW", exit_application)  # Bind the close event to exit cleanly
root.mainloop() # Show the window and start the application
