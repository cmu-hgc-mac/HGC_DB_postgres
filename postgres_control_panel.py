import tkinter as tk
from tkinter import messagebox
from PIL import Image, ImageTk
import os
import asyncio
import threading

# Functions for each button
async def refresh_action():
    await asyncio.sleep(1)  # Simulate a long-running operation
    messagebox.showinfo("Action", "Refreshing...")

async def create_action():
    await asyncio.sleep(1)  # Simulate a long-running operation
    messagebox.showinfo("Action", "Creating...")

async def upload_action():
    await asyncio.sleep(1)  # Simulate a long-running operation
    messagebox.showinfo("Action", "Uploading...")

async def check_config_action():
    await asyncio.sleep(1)  # Simulate a long-running operation
    messagebox.showinfo("Action", "Checking Config...")

async def print_action():
    await asyncio.sleep(1)  # Simulate a long-running operation
    messagebox.showinfo("Action", "Printing...")

# Function to run the asynchronous function in a thread
def run_async(coroutine):
    asyncio.run_coroutine_threadsafe(coroutine(), asyncio.get_event_loop())

# Create the main window
root = tk.Tk()
root.title("Local DB Dashboard - CMS HGC MAC")

# Hide the default window close button
root.overrideredirect(True)  # Remove title bar

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
image_path = "documentation/images/logo_small_75.png"  # Replace with the correct path to your image file

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
button_refresh = tk.Button(root, text="Refresh", width=15, command=lambda: run_async(refresh_action))
button_refresh.grid(row=1, column=0, padx=10, pady=5, sticky="w")

button_create = tk.Button(root, text="Create", width=15, command=lambda: run_async(create_action))
button_create.grid(row=2, column=0, padx=10, pady=5, sticky="w")

button_upload = tk.Button(root, text="Upload", width=15, command=lambda: run_async(upload_action))
button_upload.grid(row=3, column=0, padx=10, pady=5, sticky="w")

button_check_config = tk.Button(root, text="Check Config", width=15, command=lambda: run_async(check_config_action))
button_check_config.grid(row=4, column=0, padx=10, pady=5, sticky="w")

button_print = tk.Button(root, text="Print", width=15, command=lambda: run_async(print_action))
button_print.grid(row=5, column=0, padx=10, pady=5, sticky="w")

# Add an Exit button to terminate the program
button_exit = tk.Button(root, text="Exit", width=15, command=root.quit)
button_exit.grid(row=6, column=0, padx=10, pady=5, sticky="w")

# Start the event loop for asyncio in a separate thread
def start_asyncio_loop(stop_event):
    asyncio.set_event_loop(asyncio.new_event_loop())
    while not stop_event.is_set():
        asyncio.get_event_loop().run_forever()

# Create a stop event for the asyncio loop
stop_event = threading.Event()

# Start the asyncio event loop in a separate thread
asyncio_thread = threading.Thread(target=start_asyncio_loop, args=(stop_event,))
asyncio_thread.start()

# Function to handle window closing
def on_closing():
    stop_event.set()  # Signal the asyncio loop to stop
    root.destroy()  # Close the Tkinter window

# Bind the window closing event to the on_closing function
root.protocol("WM_DELETE_WINDOW", on_closing)

# Start the GUI event loop
root.mainloop()

# Wait for the asyncio thread to finish
asyncio_thread.join()

print("Application has been closed successfully.")
