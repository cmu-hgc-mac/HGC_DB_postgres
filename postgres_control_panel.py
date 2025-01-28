import sys
import threading
import time
import os, yaml, base64
from cryptography.fernet import Fernet
import subprocess, webbrowser
import tkinter
from tkinter import Tk, Button, Checkbutton, Label, messagebox, Frame, Toplevel, Entry, IntVar, StringVar, BooleanVar, Text, LabelFrame
from tkinter import END, DISABLED, Label as TLabel
from datetime import datetime
from export_data.src import process_xml_list, update_yaml_with_checkboxes

encryption_key = Fernet.generate_key()
cipher_suite = Fernet(encryption_key) ## Generate or load a key. 

loc = 'dbase_info'
conn_yaml_file = os.path.join(loc, 'conn.yaml')
config_data  = yaml.safe_load(open(conn_yaml_file, 'r'))
dbase_name = config_data.get('dbname')
cern_dbase = config_data.get('cern_db')

def run_git_pull_seq():
    result = subprocess.run(["git", "pull"], capture_output=True, text=True)
    if result.returncode == 0:
        print("Git pull successful ..."); print(result.stdout)
    else:
        print("Git pull failed ..."); print(result.stderr); exit()

run_git_pull_seq()

def bind_button_keys(button):
    button.bind("<Return>", lambda event: button.invoke())  # Bind Enter key

# Synchronous functions for button actions
def import_action():
    show_message("Currently under development...")

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

def print_action():
    time.sleep(1)  # Simulate a time-consuming task
    show_message("Printing...")

def show_message(message):
    messagebox.showinfo("Action", message)

# Function to exit the application
def exit_application():
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
    TLabel(input_window, text="Set initial: VIEWER password (only read):").pack(pady=5)
    viewer_var = StringVar()
    viewer_var_entry = Entry(input_window, textvariable=viewer_var, width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    viewer_var_entry.pack(pady=5)
    # Field 2: Username
    TLabel(input_window, text="Set initial: USER password (write access):").pack(pady=5)
    user_var = StringVar()
    user_var_entry = Entry(input_window, textvariable=user_var, width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    user_var_entry.pack(pady=5)
    # Field 3: Password (hidden input)
    TLabel(input_window, text="**Enter postgres password:**").pack(pady=5)
    password_var = StringVar()
    password_entry = Entry(input_window, textvariable=password_var, show='*', width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    password_entry.pack(pady=5)

    def submit_create():
        viewer_pass = viewer_var.get()
        user_pass = user_var.get()
        db_pass = base64.urlsafe_b64encode( cipher_suite.encrypt((password_var.get()).encode()) ).decode() ## Encrypt password and then convert to base64
        if db_pass.strip():
            input_window.destroy()  # Close the input window
            # Run the subprocess command
            subprocess.run([sys.executable, "create/create_database.py", "-p", db_pass, "-up", user_pass, "-vp", viewer_pass, "-k", encryption_key])
            subprocess.run([sys.executable, "create/create_tables.py", "-p", db_pass, "-k", encryption_key])
            show_message(f"PostgreSQL database '{dbase_name}' tables created.")
        else:
            if messagebox.askyesno("Input Error", "Do you want to cancel? \nDatabase password cannot be empty."):
                input_window.destroy()  

    submit_create_button = Button(input_window, text="Submit", command=submit_create)
    submit_create_button.pack(pady=10)
    bind_button_keys(submit_create_button)

def modify_tables():
    # run_git_pull_seq()
    input_window = Toplevel(root)
    input_window.title("Input Required")
    TLabel(input_window, text="**Enter postgres password:**").pack(pady=10)
    password_var = StringVar()
    entry = Entry(input_window, textvariable=password_var, show='*', width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    entry.pack(pady=10)

    def submit_modify():
        db_pass = base64.urlsafe_b64encode( cipher_suite.encrypt( (password_var.get()).encode()) ).decode() ## Encrypt password and then convert to base64
        if db_pass.strip():
            input_window.destroy()  # Close the input window
            # Run the subprocess command
            subprocess.run([sys.executable, "modify/modify_table.py", "-p", db_pass, "-k", encryption_key])
            subprocess.run([sys.executable, "create/create_tables.py", "-p", db_pass, "-k", encryption_key])
            show_message(f"Check terminal for tables modified. Refresh pgAdmin4.")
        else:
            if messagebox.askyesno("Input Error", "Do you want to cancel?\nDatabase password cannot be empty."):
                input_window.destroy()  

    submit_modify_button = Button(input_window, text="Submit", command=submit_modify)
    submit_modify_button.pack(pady=10)
    bind_button_keys(submit_modify_button)

def import_data():
    # run_git_pull_seq()
    input_window = Toplevel(root)
    input_window.title("Input Required")

    TLabel(input_window, text="Enter local db USER password:").pack(pady=5)
    shipper_var = StringVar()
    shipper_var_entry = Entry(input_window, textvariable=shipper_var, show='*', width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    shipper_var_entry.pack(pady=5)

    # TLabel(input_window, text="Enter lxplus username:").pack(pady=5)
    # lxuser_var = StringVar()
    # lxuser_var_entry = Entry(input_window, textvariable=lxuser_var, width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    # lxuser_var_entry.pack(pady=5)

    # TLabel(input_window, text="Enter lxplus password:").pack(pady=5)
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
        dbshipper_pass = base64.urlsafe_b64encode( cipher_suite.encrypt( (shipper_var.get()).encode()) ).decode() ## Encrypt password and then convert to base64
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
    TLabel(input_window, text="**Enter local DB USER password:**").pack(pady=5)
    shipper_var = StringVar()
    shipper_var_entry = Entry(input_window, textvariable=shipper_var, show='*', width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    shipper_var_entry.pack(pady=5)
    TLabel(input_window, text="**Enter LXPLUS username:**").pack(pady=5)
    lxuser_var = StringVar()
    lxuser_var_entry = Entry(input_window, textvariable=lxuser_var, width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    lxuser_var_entry.pack(pady=5)
    TLabel(input_window, text="**Enter LXPLUS password:**").pack(pady=5)
    lxpassword_var = StringVar()
    lxpassword_entry = Entry(input_window, textvariable=lxpassword_var, show='*', width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    lxpassword_entry.pack(pady=5)

    today_date = datetime.now()
    TLabel(input_window, text="Start date").pack(pady=5)
    startdate_var = StringVar(master=input_window, value=today_date.strftime("%Y-%m-%d"))
    startdate_var_entry = Entry(input_window, textvariable=startdate_var, width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    startdate_var_entry.pack(pady=5)
    TLabel(input_window, text="End date").pack(pady=5)
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
        dbshipper_pass = base64.urlsafe_b64encode( cipher_suite.encrypt( (shipper_var.get()).encode()) ).decode() ## Encrypt password and then convert to base64
        lxp_password = base64.urlsafe_b64encode( cipher_suite.encrypt( (lxpassword_var.get()).encode()) ).decode() ## Encrypt password and then convert to base64
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
    TLabel(input_window, text="Enter local db USER password:").pack(pady=5)
    shipper_var = StringVar()
    shipper_var_entry = Entry(input_window, textvariable=shipper_var, show='*', width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    shipper_var_entry.pack(pady=5)

    def submit_refresh():
        dbshipper_pass = base64.urlsafe_b64encode( cipher_suite.encrypt( (shipper_var.get()).encode()) ).decode() ## Encrypt password and then convert to base64
    
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

# Create a helper function to handle button clicks
def handle_button_click(action):
    threading.Thread(target=action).start()

# Initialize the application
root = Tk()
root.title("Local DB Dashboard - CMS HGC MAC")
root.geometry("400x550")

# Load logo image
image_path = "documentation/images/logo_small_75.png"  # Update with your image path
logo_image = load_image(image_path)

# Create a frame for the layout
frame = Frame(root)
frame.pack(pady=10, fill='both', expand=True)

# Add logo or fallback label
if logo_image:
    logo_label = Label(frame, image=logo_image, anchor="w")
    logo_label.pack(side="top", anchor="w", padx=10)
else:
    logo_label = Label(frame, text="Carnegie Mellon University")
    logo_label.pack(side="top", anchor="w", padx=10)

button_width, button_height = 22, 3
small_button_width, small_button_height = 15, 1

# Create buttons with large size
button_create = Button(frame, text="Create DBase Tables", command=create_database, width = small_button_width, height = small_button_height)
button_create.pack(pady=5)
bind_button_keys(button_create)

button_modify = Button(frame, text="Modify Existing Tables", command=modify_tables, width = small_button_width, height = small_button_height)
button_modify.pack(pady=5)
bind_button_keys(button_modify)

button_check_config = Button(frame, text="Check Config", command=check_config_action, width = small_button_width, height = small_button_height)
button_check_config.pack(pady=5)
bind_button_keys(button_check_config)

spacer = Frame(frame, height=20)  # Spacer with height (for vertical spacing)
spacer.pack()

button_shipin = Button(frame, text="Verify received shipment 📦 ⬇️", command=refresh_data, width = button_width, height = button_height)
button_shipin.pack(pady=5)
bind_button_keys(button_shipin)
button_shipin.config(state="disabled")

# button_download = Button(frame, text="Import Parts Data", command=lambda: handle_button_click(import_action), width = button_width, height = button_height)
button_download = Button(frame, text="   Import Parts Data     📁 ⬇️", command=import_data, width = button_width, height = button_height)
button_download.pack(pady=5)
bind_button_keys(button_download)

button_upload = Button(frame, text=" Upload XMLs to DBLoader 📁 ⬆️", command=export_data, width = button_width, height = button_height)
button_upload.pack(pady=5)
bind_button_keys(button_upload)

button_shipout = Button(frame, text="   Outgoing shipment     📦 ⬆️", command=refresh_data, width = button_width, height = button_height)
button_shipout.pack(pady=5)
button_shipout.config(state="disabled")
bind_button_keys(button_shipout)

button_upload = Button(frame, text=" Refresh local database     🔄", command=refresh_data, width = button_width, height = button_height)  #🔃 
button_upload.pack(pady=5)
bind_button_keys(button_upload)


# Documentation link at the bottom
def open_documentation():
    webbrowser.open("https://github.com/cmu-hgc-mac/")  


doc_label = Label(root, text="Documentation", fg="blue", cursor="hand2")
doc_label.pack(pady=5)
# doc_label.pack(side='bottom', pady=5)
doc_label.bind("<Button-1>", lambda e: open_documentation())  # Bind click event to the label

# Bind the close event to exit cleanly
root.protocol("WM_DELETE_WINDOW", exit_application)

# Show the window and start the application
root.mainloop()
