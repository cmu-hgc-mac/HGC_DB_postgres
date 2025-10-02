import asyncpg, asyncio, os, yaml, base64, csv
from cryptography.fernet import Fernet
from natsort import natsorted
from datetime import datetime
import tkinter
from tkinter import END, DISABLED, Label as Label
from tkinter import Tk, Button, Checkbutton, Label, messagebox, Frame, Toplevel, Entry, IntVar, StringVar, BooleanVar, Text, LabelFrame, Radiobutton, filedialog, OptionMenu

loc = 'dbase_info'
conn_yaml_file = os.path.join(loc, 'conn.yaml')
db_params = {
    'database': yaml.safe_load(open(conn_yaml_file, 'r')).get('dbname'),
    'user': 'shipper',
    'host': yaml.safe_load(open(conn_yaml_file, 'r')).get('db_hostname'),
    'port': yaml.safe_load(open(conn_yaml_file, 'r')).get('port'),
}

def update_packed_timestamp_sync(encrypt_key, password, module_names, timestamp, savetofile = False):
    if savetofile:
        fileout_name = f"""shipping/packed_{timestamp.strftime('%Y%m%d_%H%M%S')}_modules_{len(module_names)}.txt"""
        os.makedirs('shipping', exist_ok=True)
        with open(fileout_name, "w", newline="") as file:
            writer = csv.writer(file)
            for module in natsorted(module_names):
                writer.writerow([module])
        print('Module names saved to', fileout_name)
    asyncio.run(_update_packed_timestamp(encrypt_key = encrypt_key, password = password, module_names = module_names, timestamp = timestamp))

async def _update_packed_timestamp(encrypt_key, password, module_names, timestamp, db_params = db_params):
    cipher_suite = Fernet((encrypt_key))
    dbpassword = cipher_suite.decrypt( base64.urlsafe_b64decode(password)).decode() ## Decode base64 to get encrypted string and then decrypt
    db_params.update({'password': dbpassword})
    query = """UPDATE module_info SET packed_datetime = $1 WHERE module_name = ANY($2)"""
    try:
        conn = await asyncpg.connect(**db_params)
        await conn.execute(query, timestamp, module_names)
        print(f"Updated packed_timestamp for {len(module_names)} modules.")
    except Exception as e:
        print(f"Error updating packed_timestamp: {e}")


def update_shipped_timestamp_sync(encrypt_key, password, module_names, timestamp):
    fileout_name = asyncio.run(_update_shipped_timestamp(encrypt_key = encrypt_key, password = password, module_names = module_names, timestamp = timestamp))
    return fileout_name

async def _update_shipped_timestamp(encrypt_key, password, module_names, timestamp, db_params = db_params):
    cipher_suite = Fernet((encrypt_key))
    dbpassword = cipher_suite.decrypt( base64.urlsafe_b64decode(password)).decode() ## Decode base64 to get encrypted string and then decrypt
    db_params.update({'password': dbpassword})
    query_fetch = """SELECT DISTINCT packed_datetime FROM module_info WHERE module_name = ANY($1); """
    query_update = """UPDATE module_info SET shipped_datetime = $1 WHERE packed_datetime = ANY($2) RETURNING module_name; """
    try:
        conn = await asyncpg.connect(**db_params)
        rows = await conn.fetch(query_fetch, module_names)
        packed_datetimes = [row['packed_datetime'] for row in rows]
        mod_names_out = await conn.fetch(query_update, timestamp, packed_datetimes)
        shipped_modules = [row['module_name'] for row in mod_names_out]
        print(f"Updated shipped_timestamp for {len(shipped_modules)} modules.")
        fileout_name = f"""shipping/shipmentout_{timestamp.strftime('%Y%m%d_%H%M%S')}_modules_{len(shipped_modules)}.csv"""
        os.makedirs('shipping', exist_ok=True)
        with open(fileout_name, "w", newline="") as file:
            writer = csv.writer(file)
            for module in natsorted(shipped_modules):
                writer.writerow([module])
        return fileout_name
    except Exception as e:
        print(f"Error updating shipped_timestamp: {e}")
        return None
    
class enter_part_barcodes_box(tkinter.Toplevel):
    def __init__(self, parent, encryption_key, dbshipper_pass, upload_file_with_part_out, max_mod_per_box, entries = []):
        super().__init__(parent)

        self.title("Enter barcode of parts packed in this module container")
            
        datetime_now = datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        datetime_now_label = Label(self, text=f"Now:", justify="right", anchor='e')
        datetime_now_label.grid(row=0, column=2, columnspan=1, pady=10)

        datetime_now_var = StringVar(master=self, value=datetime_now)
        datetime_now_entry = Entry(self, textvariable=datetime_now_var, width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
        datetime_now_entry.grid(row=0, column=3, columnspan=1, pady=10)

        label = Label(self, wraplength=600 ,fg = "red", text=f"Modules must be present in postgres `module_info` table to record shipments.")
        label.grid(row=1, column=1, columnspan=4, pady=10)
        upload_from_file_button = Button(self, text="Upload parts from file (optional)", command=upload_file_with_part_out)
        upload_from_file_button.grid(row=0, column=1, columnspan=1, pady=10)

        num_entries, cols = int(max_mod_per_box), 2
        for i in range(num_entries):
            row, col = 3 + i // cols, i % cols
            listlabel = Label(self, text=f"{i + 1}:")
            listlabel.grid(row=row, column=col * 2, padx=10, pady=2, sticky="w")
            entry = Entry(self, width=30)
            entry.grid(row=row, column=col * 2 + 1, padx=10, pady=2)
            entries.append(entry)

        export_var = IntVar()
        export_var.set(1)
        export_checkbox = Checkbutton(self, text='Export to file (shipping/packed...csv)', variable=export_var)
        export_checkbox.grid(row=3+(num_entries//2), column=1, columnspan=1, pady=10)

        def update_db_packed():
            module_update_pack = [entry.get() for entry in entries if entry.get().strip() != ""]
            dialog = popup_pack_in_crate(self, )
            dialog.transient(self); dialog.attributes("-topmost", True); dialog.focus_force()
            self.wait_window(dialog)  # Wait until dialog is closed
            self.destroy()
            if len(module_update_pack) > 0 :
                if len(datetime_now_var.get().strip()) == 0: datetime_now_var.set(datetime_now)
                datetime_now_obj = datetime.strptime(datetime_now_var.get().strip(), "%Y-%m-%d %H:%M:%S")
                update_packed_timestamp_sync(encrypt_key=encryption_key, password=dbshipper_pass.strip(), module_names=module_update_pack, timestamp=datetime_now_obj, savetofile=bool(export_var.get()))
                

        submit_button = Button(self, text="Record to DB", command=update_db_packed)
        submit_button.grid(row=3+(num_entries//2), column=2, columnspan=2, pady=10)


class popup_pack_in_crate(tkinter.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.result = None
        self.title("Add this to a crate?")
        self.geometry("320x200")
        self.grab_set()  # Make modal
        self.boxes_in_crate, self.modules_in_crate = asyncio.run(self.get_open_crate())
        if self.boxes_in_crate:
            message = f"Would you like to add this box to the open crate with {self.boxes_in_crate} box{'es' if self.boxes_in_crate > 1 else ''} ({self.modules_in_crate} module{'s' if self.modules_in_crate > 1 else ''})?"
        else:
            message = f"Would you like to add this box to an empty crate?"
        label = tkinter.Label(self, text=message, wraplength=300)
        label.pack(pady=10)
        btn_frame = tkinter.Frame(self)
        btn_frame.pack(pady=5)
        add_btn = tkinter.Button(btn_frame, text="Add to Crate", width=12, height = 3, command=lambda: self._set_result("add"), wraplength=100)
        add_btn.pack(side="left", padx=5)
        skip_btn = tkinter.Button(btn_frame, text="This is a standalone shipment", width=12, height = 3, command=lambda: self._set_result("skip"), wraplength=100)
        skip_btn.pack(side="left", padx=5)
        cancel_btn = tkinter.Button(btn_frame, text="Cancel", width=12, command=lambda: self._set_result("cancel"))
        cancel_btn.pack(side="left", padx=5)
        self.protocol("WM_DELETE_WINDOW", lambda: self._set_result("cancel"))

    async def get_open_crate(self):
        boxes_in_crate, modules_in_crate = 10, 300
        return boxes_in_crate, modules_in_crate
    
    def _set_result(self, value):
        self.result = value
        self.destroy()

