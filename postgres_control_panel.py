import sys
import threading
import time
import os, yaml
import subprocess, webbrowser
from tkinter import Tk, Button, Label, messagebox, Frame, Toplevel, Entry, StringVar, Text, END, DISABLED, Label as TLabel

loc = 'dbase_info'
conn_yaml_file = os.path.join(loc, 'conn.yaml')
dbase_name  = yaml.safe_load(open(conn_yaml_file, 'r')).get('dbname')

# Synchronous functions for button actions
def refresh_action():
    show_message("Refreshing...")

def upload_action():
    show_message("Uploading...")

def check_config_action():  
    with open(conn_yaml_file, 'r') as file:
        config_data = yaml.safe_load(file)
    message = 'Database configuration variables in "HGC_DB_postgres/dbase_info/conn.yaml".\n\n'
    message += "\n".join(f"{key}: {value}" for key, value in config_data.items())
    show_message(message)

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
    viewer_var_entry = Entry(input_window, textvariable=viewer_var, width=30)
    viewer_var_entry.pack(pady=5)

    # Field 2: Username
    TLabel(input_window, text="Set initial: user password (write access):").pack(pady=5)
    user_var = StringVar()
    user_var_entry = Entry(input_window, textvariable=user_var, width=30)
    user_var_entry.pack(pady=5)

    # Field 3: Password (hidden input)
    TLabel(input_window, text="**Enter master password:**").pack(pady=5)
    password_var = StringVar()
    password_entry = Entry(input_window, textvariable=password_var, show='*', width=30)
    password_entry.pack(pady=5)

    def submit():
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
            if messagebox.askyesno("Input Error", "Database password cannot be empty. Do you want to cancel?"):
                input_window.destroy()  

    submit_button = Button(input_window, text="Submit", command=submit)
    submit_button.pack(pady=10)

def modify_tables():
    input_window = Toplevel(root)
    input_window.title("Input Required")

    TLabel(input_window, text="Enter master password:").pack(pady=10)
    password_var = StringVar()
    entry = Entry(input_window, textvariable=password_var, show='*', width=30)
    entry.pack(pady=10)

    def submit():
        db_pass = password_var.get()
        if db_pass.strip():
            input_window.destroy()  # Close the input window
            # Run the subprocess command
            subprocess.run([sys.executable, "modify/modify_table.py", "-p", db_pass])
            show_message(f"PostgreSQL tables modified. Refresh pgAdmin4.")
        else:
            if messagebox.askyesno("Input Error", "Database password cannot be empty. Do you want to cancel?"):
                input_window.destroy()  

    submit_button = Button(input_window, text="Submit", command=submit)
    submit_button.pack(pady=10)


# Create a helper function to handle button clicks
def handle_button_click(action):
    threading.Thread(target=action).start()

# Initialize the application
root = Tk()
root.title("Local DB Dashboard - CMS HGC MAC")
root.geometry("300x400")

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

# Create buttons with larger size

button_create = Button(frame, text="Create DBase Tables", command=create_database, width=15, height=2)
button_create.pack(pady=5)

button_create = Button(frame, text="Modify Tables", command=modify_tables, width=15, height=2)
button_create.pack(pady=5)

button_check_config = Button(frame, text="Check Config", command=lambda: handle_button_click(check_config_action), width=15, height=2)
button_check_config.pack(pady=5)

button_refresh = Button(frame, text="Refresh Local", command=lambda: handle_button_click(refresh_action), width=15, height=2)
button_refresh.pack(pady=5)

button_upload = Button(frame, text="Upload XMLs to DBLoader", command=lambda: handle_button_click(upload_action), width=15, height=2)
button_upload.pack(pady=5)



# button_print = Button(frame, text="Print", command=lambda: handle_button_click(print_action), width=15, height=2)
# button_print.pack(pady=5)

# Documentation link at the bottom
def open_documentation():
    webbrowser.open("https://github.com/cmu-hgc-mac/")  # Replace with your actual documentation URL

doc_label = Label(root, text="Documentation", fg="blue", cursor="hand2")
doc_label.pack(side='bottom', pady=5)
doc_label.bind("<Button-1>", lambda e: open_documentation())  # Bind click event to the label


# Bind the close event to exit cleanly
root.protocol("WM_DELETE_WINDOW", exit_application)

# Show the window and start the application
root.mainloop()
