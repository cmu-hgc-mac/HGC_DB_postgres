import tkinter as tk
from tkinter import filedialog, ttk
import subprocess, os, asyncio
from io import BytesIO
from PIL import Image, ImageTk
from postgres_tools.upload_inspect import request_PostgreSQL, comptable

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
                output_text.insert(tk.END, f'Processed file: {file_path}\n')
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
    if '.xls' in file_path.lower():
        command = ['python', script_path, file_path]

    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f'Error running subprocess: {e}')


def update_image_list(file_paths, image_list):
    image_list.delete(0, tk.END)
    for file_path in file_paths:
        image_list.insert(tk.END, os.path.basename(file_path))  # Display only the file name
    return image_list

def display_selected_image(event):
    selected_subtab = nested_notebook.tab(nested_notebook.select(), "text")
    selection = event.widget.curselection()
    if selection:
        file_path = event.widget.get(selection[0])
        im = asyncio.run(request_PostgreSQL(selected_subtab, file_path))
        #image = Image.open(file_path)
        if im != []:
            image = Image.open(BytesIO(im[0]['hexplot']))
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
    global subtab_label
    for s in range(len(subtab_label)):
        re = asyncio.run(request_PostgreSQL(subtab_label[s]))
        pe = [r[f"{comptable[subtab_label[s]]['prefix']}_name"] for r in re]
        image_lists[s] = update_image_list(pe, image_lists[s])

# def submit_comment(event):
    # comment = comment_entry.get()
    # if comment:
    #     print(comment)
    #     selected_subtab = nested_notebook.tab(nested_notebook.select(), "text")
    #     selection = event.widget.curselection()
    #     if selection:
    #         selected_subtab = nested_notebook.tab(nested_notebook.select(), "text")
    #         list_item = image_list.get(selection[0])
    #         print(f"Subtab: {selected_subtab}, List item: {list_item}, Comment: {comment}")
    #     else:
    #         print("Please select an item from the list.")

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
notebook.pack(expand=True, fill="both")

# Create nested notebook with subtabs
nested_notebook = ttk.Notebook(tab1)
nested_notebook.pack(expand=True, fill="both")

subtabs = []
image_lists = []
subtab_label = ['baseplate','hexaboard','protomodule','module']
for s in range(len(subtab_label)):  ## bp, hxp, pml, ml
    subtabs.append(ttk.Frame(nested_notebook))
    nested_notebook.add(subtabs[s], text=subtab_label[s])

# Create and configure listbox for image selection
for s in range(len(subtab_label)):
    image_list_frame = tk.Frame(subtabs[s])
    image_list_scrollbar = tk.Scrollbar(image_list_frame)
    image_list = tk.Listbox(image_list_frame, yscrollcommand=image_list_scrollbar.set, selectmode=tk.SINGLE, height=12, width=30) # Adjust the height and width of the listbox
    image_list_scrollbar.config(command=image_list.yview)
    image_list_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    image_list.pack(pady=10, side=tk.LEFT, fill=tk.BOTH, expand=True)  # Adjust side and padding of the listbox
    image_list_frame.pack(pady=10, fill=tk.BOTH, expand=True)
    image_list.bind('<<ListboxSelect>>', display_selected_image)
    image_lists.append(image_list)


    # pe = ['a.png','b.png','c.png','a.png']
refresh_listbox()

    # Create and configure comment box
# comment_frame = tk.Frame(tab1)
# comment_label = tk.Label(comment_frame, text="Comment:")
# comment_label.pack(side=tk.LEFT, padx=(10, 5))
# comment_entry = tk.Entry(comment_frame, width=50)  # Adjust width of the comment entry
# comment_entry.pack(side=tk.LEFT, padx=(0, 5))
# submit_button = tk.Button(comment_frame, text="Submit Comment", command=submit_comment())
# submit_button.pack(side=tk.LEFT)
# comment_frame.pack(pady=5, fill=tk.X)

image_display_frame = tk.Frame(tab1)
image_label = tk.Label(image_display_frame)
image_label.pack(pady=10)  # Adjust padding of the image label
image_display_frame.pack(pady=10)

refresh_button = tk.Button(tab1, text="Refresh", command=refresh_listbox)
refresh_button.pack()


### Include the watching portion
#info_show = tk.Label(tab1, text = f'Watching directory: C:/Users/Admin/.../OGP_results')
#info_show.pack()



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
