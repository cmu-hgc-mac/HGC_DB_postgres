import tkinter as tk
from tkinter import filedialog
import subprocess

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

# Create the main window
root = tk.Tk()
root.title('File Selection and Processing GUI')
root.geometry('800x800')  # Adjusted window size
root.configure(bg='gray')

# Create and configure widgets
select_button = tk.Button(root, text='Select Files', command=select_files)
process_button = tk.Button(root, text='Process Selected Files', command=process_selected_files)

select_button.pack(pady=10)

process_button.pack(pady=10)

file_paths_scrollbar = tk.Scrollbar(root)
file_paths_text = tk.Text(root, height=15, width=90, wrap=tk.WORD, state=tk.DISABLED, yscrollcommand=file_paths_scrollbar.set)
file_paths_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
file_paths_text.pack(pady=10)

output_scrollbar = tk.Scrollbar(root)
output_text = tk.Text(root, height=15, width=90, wrap=tk.WORD, state=tk.DISABLED, yscrollcommand=output_scrollbar.set)
output_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
output_text.pack(pady=10)
import os
show_file_path = str(os.path.realpath(__file__)).split('file_selector.py')[0]
info_show = tk.Label(root, text = f'Output saved here: '+ show_file_path)
info_show.pack()
# Pack widgets into the window



# Start the GUI event loop
root.mainloop()
