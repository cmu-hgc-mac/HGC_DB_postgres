import tkinter as tk
from tkinter import messagebox
from PIL import Image, ImageTk
import os

# Functions for each button
def refresh_action():
    messagebox.showinfo("Action", "Refreshing...")

def create_action():
    messagebox.showinfo("Action", "Creating...")

def upload_action():
    messagebox.showinfo("Action", "Uploading...")

def check_config_action():
    messagebox.showinfo("Action", "Checking Config...")

def print_action():
    messagebox.showinfo("Action", "Printing...")

# Create the main window
root = tk.Tk()
root.title("Local DB Dashboard - CMS HGC MAC")

# Ensure window size is appropriate for displaying the content
root.geometry("300x400")

# Load image without resizing
def load_image(image_path):
    if os.path.exists(image_path):
        img = Image.open(image_path)
        return ImageTk.PhotoImage(img)
    else:
        print(f"Logo not found: {image_path}")
        return None

# Path to the image file
image_path = "logo_small_75.png"  # Replace with the correct path to your image file

# Load and display the image in the top-left corner
logo = load_image(image_path)

if logo:
    logo_label = tk.Label(root, image=logo)
    logo_label.grid(row=0, column=0, padx=10, pady=10, sticky="nw")
else:
    # Show a default message if the image is not found
    logo_label = tk.Label(root, text="Carnegie Mellon University")
    logo_label.grid(row=0, column=0, padx=10, pady=10, sticky="nw")

# Create buttons and place them in the grid layout
button_refresh = tk.Button(root, text="Refresh", width=15, command=refresh_action)
button_refresh.grid(row=1, column=0, padx=10, pady=5, sticky="w")

button_create = tk.Button(root, text="Create", width=15, command=create_action)
button_create.grid(row=2, column=0, padx=10, pady=5, sticky="w")

button_upload = tk.Button(root, text="Upload", width=15, command=upload_action)
button_upload.grid(row=3, column=0, padx=10, pady=5, sticky="w")

button_check_config = tk.Button(root, text="Check Config", width=15, command=check_config_action)
button_check_config.grid(row=4, column=0, padx=10, pady=5, sticky="w")

button_print = tk.Button(root, text="Print", width=15, command=print_action)
button_print.grid(row=5, column=0, padx=10, pady=5, sticky="w")

# Start the GUI event loop
root.mainloop()
