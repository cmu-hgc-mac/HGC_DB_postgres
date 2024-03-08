import tkinter as tk
from tkinter import filedialog, ttk
import subprocess, os, asyncio
from io import BytesIO
from PIL import Image, ImageTk
from postgres_tools.upload_inspect import request_PostgreSQL

def select_files():
    file_paths = filedialog.askopenfilenames()
    if file_paths:
        file_paths_text.config(state=tk.NORMAL)
        file_paths_text.delete(1.0, tk.END)
        file_paths_text.insert(tk.END, f'Selected file paths:\n{", ".join(file_paths)}')
        file_paths_text.config(state=tk.DISABLED)
        file_paths_scrollbar.config(command=file_paths_text.yview)
        file_paths_text.config(yscrollcommand=file_paths_scrollbar.set)
        root.file_paths = file_paths
    else:
        file_paths_text.config(state=tk.NORMAL)
        file_paths_text.delete(1.0, tk.END)
        file_paths_text.insert(tk.END, 'No files selected')
        file_paths_text.config(state=tk.DISABLED)

def process_selected_files():
    try:
        file_paths = getattr(root, 'file_paths', None)
        if file_paths:
            output_text.config(state=tk.NORMAL)
            output_text.delete(1.0, tk.END)
            for file_path in file_paths:
                output_text.insert(tk.END, f'Processing file: {file_path}\n')
                call_script_with_plotting(file_path)
            output_text.config(state=tk.DISABLED)
            output_scrollbar.config(command=output_text.yview)
            output_text.config(yscrollcommand=output_scrollbar.set)
        else:
            output_text.config(state=tk.NORMAL)
            output_text.delete(1.0, tk.END)
            output_text.insert(tk.END, 'No files selected for processing')
            output_text.config(state=tk.DISABLED)
    except Exception as e:
        output_text.config(state=tk.NORMAL)
        output_text.delete(1.0, tk.END)
        output_text.insert(tk.END, f'Error running the process: {str(e)}')
        output_text.config(state=tk.DISABLED)

def call_script_with_plotting(file_path):
    script_path = 'process_im.py'
    command = ['python', script_path, file_path]

    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f'Error running subprocess: {e}')


def update_image_list(file_paths):
    image_list.delete(0, tk.END)
    for file_path in file_paths:
        image_list.insert(tk.END, os.path.basename(file_path))  # Display only the file name

def display_selected_image(event):
    selection = event.widget.curselection()
    if selection:
        index = selection[0]
        file_path = image_list.get(index)
        print(file_path)
        im = asyncio.run(request_PostgreSQL('baseplate_plot', file_path))[0]['hexplot']
        #image = Image.open(file_path)
        image = Image.open(BytesIO(im))
        aspect_ratio = image.width / image.height
        new_width = 600  # Set the width of the image label
        new_height = int(new_width / aspect_ratio)
        image = image.resize((new_width, new_height))
        photo = ImageTk.PhotoImage(image)
        image_label.config(image=photo)
        image_label.image = photo  # Keep a reference to avoid garbage collection
    else:
        image_label.config(image=None)

def refresh_listbox():
    global pe
    re = asyncio.run(request_PostgreSQL('baseplate_name'))
    pe = [r['bp_name'] for r in re]
    update_image_list(pe)


##################################################################
        
# Create the main window
root = tk.Tk()
root.title('File Selection and Processing GUI')
root.geometry('800x800')  # Adjusted window size
root.configure(bg='gray')

# Create and configure notebook
notebook = ttk.Notebook(root)

###################################################################

# Create and configure first tab
tab1 = ttk.Frame(notebook)
notebook.add(tab1, text='View Plots')

# Create and configure listbox for image selection
image_list_frame = tk.Frame(tab1)
image_list_scrollbar = tk.Scrollbar(image_list_frame)
image_list = tk.Listbox(image_list_frame, yscrollcommand=image_list_scrollbar.set, selectmode=tk.SINGLE, height=5, width=30) # Adjust the height and width of the listbox
image_list_scrollbar.config(command=image_list.yview)
image_list_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
image_list.pack(pady=10, side=tk.LEFT, fill=tk.BOTH, expand=True)  # Adjust side and padding of the listbox
image_list_frame.pack(pady=10, fill=tk.BOTH, expand=True)

# image_list.bind('<<ListboxSelect>>', display_selected_image)

# pe = ['a.png','b.png','c.png','a.png','b.png','c.png','a.png','b.png','c.png','a.png','b.png','c.png','a.png','b.png','c.png','a.png','b.png','c.png','a.png','b.png','c.png','a.png','b.png','c.png']
re = asyncio.run(request_PostgreSQL('baseplate_name'))
pe = [r['bp_name'] for r in re]

for entry in pe:
    image_list.insert(tk.END, entry)

image_list.bind('<<ListboxSelect>>', display_selected_image)

image_display_frame = tk.Frame(tab1)
image_label = tk.Label(image_display_frame)
image_label.pack(pady=10)  # Adjust padding of the image label
image_display_frame.pack(pady=10)

refresh_button = tk.Button(tab1, text="Refresh List", command=refresh_listbox)
refresh_button.pack()

##########################################################################

# Create and configure second tab
tab2 = ttk.Frame(notebook)
notebook.add(tab2, text='Upload Files')

select_button = tk.Button(tab2, text='Select Files', command=select_files)
process_button = tk.Button(tab2, text='Process Selected Files', command=process_selected_files)

file_paths_scrollbar = tk.Scrollbar(tab2)
file_paths_text = tk.Text(tab2, height=15, width=90, wrap=tk.WORD, state=tk.DISABLED, yscrollcommand=file_paths_scrollbar.set)
file_paths_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
file_paths_text.pack(pady=10)

select_button.pack(pady=10)
process_button.pack(pady=10)

output_scrollbar = tk.Scrollbar(tab2)
output_text = tk.Text(tab2, height=15, width=90, wrap=tk.WORD, state=tk.DISABLED, yscrollcommand=output_scrollbar.set)
output_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
output_text.pack(pady=10)

show_file_path = str(os.path.realpath(__file__)).split('file_selector.py')[0]
info_show = tk.Label(tab2, text = f'Output saved here: '+ show_file_path)
info_show.pack()

notebook.pack(expand=True, fill='both')

# Start the GUI event loop
root.mainloop()
