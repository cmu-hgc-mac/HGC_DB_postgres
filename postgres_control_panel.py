import sys
import threading
import time
import os, yaml
import subprocess, webbrowser
import tkinter
from tkinter import Tk, Button, Label, messagebox, Frame, Toplevel, Entry, StringVar, Text, END, DISABLED, Label as TLabel

loc = 'dbase_info'
conn_yaml_file = os.path.join(loc, 'conn.yaml')
config_data  = yaml.safe_load(open(conn_yaml_file, 'r'))
dbase_name = config_data.get('dbname')
cern_dbase = config_data.get('cern_db')

def bind_button_keys(button):
    button.bind("<Return>", lambda event: button.invoke())  # Bind Enter key
    button.bind("<space>", lambda event: button.invoke())   # Bind Space key

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
    TLabel(input_window, text="Set initial: viewer password (only read):").pack(pady=5)
    viewer_var = StringVar()
    viewer_var_entry = Entry(input_window, textvariable=viewer_var, width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    viewer_var_entry.pack(pady=5)

    # Field 2: Username
    TLabel(input_window, text="Set initial: user password (write access):").pack(pady=5)
    user_var = StringVar()
    user_var_entry = Entry(input_window, textvariable=user_var, width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    user_var_entry.pack(pady=5)

    # Field 3: Password (hidden input)
    TLabel(input_window, text="**Enter master password:**").pack(pady=5)
    password_var = StringVar()
    password_entry = Entry(input_window, textvariable=password_var, show='*', width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    password_entry.pack(pady=5)

    def submit_create():
        viewer_pass = viewer_var .get()
        user_pass = user_var.get()
        db_pass = password_var.get()
        if db_pass.strip():
            input_window.destroy()  # Close the input window
            # Run the subprocess command
            subprocess.run([sys.executable, "create/create_database.py", "-p", db_pass, "-up", user_pass, "-vp", viewer_pass])
            subprocess.run([sys.executable, "create/create_tables.py", "-p", db_pass])
            show_message(f"PostgreSQL database '{dbase_name}' tables created.")
        else:
            if messagebox.askyesno("Input Error", "Do you want to cancel? \nDatabase password cannot be empty."):
                input_window.destroy()  

    submit_create_button = Button(input_window, text="Submit", command=submit_create)
    submit_create_button.pack(pady=10)
    bind_button_keys(submit_create_button)

def modify_tables():
    input_window = Toplevel(root)
    input_window.title("Input Required")

    TLabel(input_window, text="Enter master password:").pack(pady=10)
    password_var = StringVar()
    entry = Entry(input_window, textvariable=password_var, show='*', width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
    entry.pack(pady=10)

    def submit_modify():
        db_pass = password_var.get()
        if db_pass.strip():
            input_window.destroy()  # Close the input window
            # Run the subprocess command
            subprocess.run([sys.executable, "modify/modify_table.py", "-p", db_pass])
            show_message(f"PostgreSQL tables modified. Refresh pgAdmin4.")
        else:
            if messagebox.askyesno("Input Error", "Do you want to cancel?\nDatabase password cannot be empty."):
                input_window.destroy()  

    submit_modify_button = Button(input_window, text="Submit", command=submit_modify)
    submit_modify_button.pack(pady=10)
    bind_button_keys(submit_modify_button)

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
    logo_label = Label(frame, image=logo_image)
    logo_label.pack()
else:
    logo_label = Label(frame, text="Carnegie Mellon University")
    logo_label.pack()

button_width, button_height = 20, 3

def bind_button_keys(button):
    button.bind("<Return>", lambda event: button.invoke())  # Bind Enter key
    button.bind("<space>", lambda event: button.invoke())   # Bind Space key

# Create buttons with large size
button_create = Button(frame, text="Create DBase Tables", command=create_database, width = button_width, height = button_height)
button_create.pack(pady=5)
bind_button_keys(button_create)

button_modify = Button(frame, text="Modify Existing Tables", command=modify_tables, width = button_width, height = button_height)
button_modify.pack(pady=5)
bind_button_keys(button_modify)

button_check_config = Button(frame, text="Check Config", command=check_config_action, width = button_width, height = button_height)
button_check_config.pack(pady=5)
bind_button_keys(button_check_config)

button_download = Button(frame, text="Import Parts Data", command=lambda: handle_button_click(import_action), width = button_width, height = button_height)
button_download.pack(pady=5)
bind_button_keys(button_download)

button_upload = Button(frame, text="Upload XMLs to DBLoader", command=lambda: handle_button_click(upload_action), width = button_width, height = button_height)
button_upload.pack(pady=5)
bind_button_keys(button_upload)

# Documentation link at the bottom
def open_documentation():
    webbrowser.open("https://github.com/cmu-hgc-mac/")  

cerndb_types = {"dev_db": {'dbtype': 'Development', 'dbname': 'INT2R'}, "prod_db": {'dbtype': 'Production','dbname':'CMSR'}}
dbtype_label = Label(root, text=f'Writing to CERN {cerndb_types[cern_dbase]["dbtype"]} Database: {cerndb_types[cern_dbase]["dbname"]}', fg="black")
dbtype_label.pack(pady=2)

doc_label = Label(root, text="Documentation", fg="blue", cursor="hand2")
doc_label.pack(pady=5)
# doc_label.pack(side='bottom', pady=5)
doc_label.bind("<Button-1>", lambda e: open_documentation())  # Bind click event to the label


# Bind the close event to exit cleanly
root.protocol("WM_DELETE_WINDOW", exit_application)

# Show the window and start the application
root.mainloop()
