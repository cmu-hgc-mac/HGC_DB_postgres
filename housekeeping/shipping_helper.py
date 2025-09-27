import asyncpg, asyncio, os, yaml, base64, csv
from cryptography.fernet import Fernet
from natsort import natsorted
from datetime import datetime
import tkinter

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
    


class popup_pack_in_crate(tkinter.Toplevel):
    def __init__(self, parent, title="Add this to the crate?", message=f"Would you like to add this box to the open crate with 15 box(es) (200 module(s))?"):
        super().__init__(parent)
        self.result = None
        self.title(title)
        self.geometry("300x120")
        self.grab_set()  # Make modal
        label = tkinter.Label(self, text=message, wraplength=280)
        label.pack(pady=10)
        btn_frame = tkinter.Frame(self)
        btn_frame.pack(pady=5)
        add_btn = tkinter.Button(btn_frame, text="Add to Crate", width=12,command=lambda: self._set_result("add"))
        add_btn.pack(side="left", padx=5)
        skip_btn = tkinter.Button(btn_frame, text="This is a standalone shipment", width=12,command=lambda: self._set_result("skip"))
        skip_btn.pack(side="left", padx=5)
        cancel_btn = tkinter.Button(btn_frame, text="Cancel", width=12, command=lambda: self._set_result("cancel"))
        cancel_btn.pack(side="left", padx=5)
        self.protocol("WM_DELETE_WINDOW", lambda: self._set_result("cancel"))

    def _set_result(self, value):
        self.result = value
        self.destroy()

