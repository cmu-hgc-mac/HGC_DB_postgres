import asyncpg, asyncio, os, yaml, base64, csv, webbrowser, math
from cryptography.fernet import Fernet
from natsort import natsorted
from datetime import datetime
import tkinter
from tkinter import END, DISABLED, Label as Label
from tkinter import Tk, Button, Checkbutton, Label, messagebox, Frame, Toplevel, Entry, IntVar, StringVar, BooleanVar, Text, LabelFrame, Radiobutton, filedialog, OptionMenu, Menu
from itertools import groupby
import re
from collections import OrderedDict

# ===========================================================================================================
# ===========================================================================================================

# General configuration information
loc = "dbase_info"
conn_yaml_file = os.path.join(loc, "conn.yaml")
db_params = {
     "database": yaml.safe_load(open(conn_yaml_file, "r")).get("dbname"),
     "user": "shipper",
     "host": yaml.safe_load(open(conn_yaml_file, "r")).get("db_hostname"),
     "port": yaml.safe_load(open(conn_yaml_file, "r")).get("port"),
}
institutions_list = ["CMU", "UCSB", "TIFR", "IHEP", "NTU", "TTU"]

# ===========================================================================================================
# ===========================================================================================================

# Function for checking format of box, crate, and containter IDs
def id_setter(id_no, letter):
     if (letter == "B"):
          placeholder = "Enter box ID..."
          error_msg1 = "Must enter box ID."
          error_msg2 = "Box ID must have form [INSTITUTION]-B[NUMBER]."
     elif (letter == "C/X"):
          placeholder = "Enter shipment ID..."
          error_msg1 = "Must enter shipment ID."
          error_msg2 = "Shipment ID must have form [INSTITUTION]-C[NUMBER] for shipments of boxes packed in crates or [INSTITUTION]-X[NUMBER] for shipments of boxes not packed in crates."
     else:
          return
     
     if (id_no == placeholder or id_no == ""):
          show_error_on_top("Upload Error", "See terminal.")
          print(f"Upload Error: {error_msg1}")
          return 1
     else:
          try:
               split_text = id_no.split("-")
               split_text_no = re.split(r"(\d+)", split_text[1])
               valid_letters = {"C", "X"} if (letter == "C/X") else {letter}
               if ((split_text[0].upper() not in institutions_list) or (split_text_no[0].upper() not in valid_letters) or not split_text_no[1].isdigit()):
                    show_error_on_top("Upload Error", "See terminal.")
                    print(f"Upload Error: {error_msg2}")
                    return 1
               elif (len(split_text) > 2):
                    show_error_on_top("Upload Error", "See terminal.")
                    print(f"Upload Error: {error_msg2}")
                    return 1
               else:
                    institution = split_text[0].upper()
                    letter_id = "".join(f"{split_text_no[0].upper()}{split_text_no[1]}")
                    id_components = [institution, letter_id]
                    id_no = "-".join(id_components)
                    return id_no
          except Exception as e:
               show_error_on_top("Upload Error", "See terminal.")
               print(f"Upload Error: {error_msg2}")
               return 1

# ===========================================================================================================
# ===========================================================================================================

# Class for creating temporary default text in the entry widgets where box and shipment IDs are entered
class PlaceholderEntry(tkinter.Entry):
     def __init__(self, parent, placeholder, color = "grey", **kwargs):
          super().__init__(parent, **kwargs)

          self._placeholder = placeholder
          self._placeholder_color = color
          self._default_fg_color = self["fg"]

          self.bind("<FocusIn>", self._focus_in)        # On touch, execute command "_focus_in"
          self.bind("<FocusOut>", self._focus_out)      # On release, execute command "_focus_out"

          self._put_placeholder()

     def _put_placeholder(self):                        # Puts placeholder (temporary) text with color "_placeholder_color" (grey)
          self.insert(0, self._placeholder)
          self["fg"] = self._placeholder_color

     def _focus_in(self, event=None):                   # On touch, delete placeholder text and change to system-default color for text
          if (self["fg"] == self._placeholder_color):
               self.delete(0, "end")
               self["fg"] = self._default_fg_color
            
     def _focus_out(self, event=None):                  # On release, if nothing is in entry widget, put placeholder text
          if (not self.get()):
               self._put_placeholder()

# ===========================================================================================================
# ===========================================================================================================

# Shows popups with either error messages or "yes or no" selections
def show_error_on_top(title, message):
     temp_root = tkinter.Tk()
     temp_root.withdraw()                                                # Hide the window
     temp_root.attributes("-topmost", True)                              # Make it appear above all
     messagebox.showerror(title, message, parent = temp_root)
     temp_root.destroy()                                                 # Clean up

def askyesno_on_top(title, message):
     temp_root = tkinter.Tk()
     temp_root.withdraw()                                                # Hide the window
     temp_root.attributes("-topmost", True)                              # Make it appear above all
     result  = messagebox.askyesno(title, message, parent = temp_root)
     temp_root.destroy()                                                 # Clean up
     return result

# ===========================================================================================================
# ===========================================================================================================

# Updates column "packed_datetime" in "module_info"
def update_packed_timestamp_sync(encrypt_key, password, module_names, timestamp, savetofile = False):
     if (savetofile):
          fileout_name = f"""shipping/packed_{timestamp.strftime('%Y%m%d_%H%M%S')}_modules_{len(module_names)}.txt"""
          os.makedirs("shipping", exist_ok = True)
          with open(fileout_name, "w", newline = "") as file:
               writer = csv.writer(file)
               for module in natsorted(module_names):
                    writer.writerow([module])
               print("Module names saved to", fileout_name)
     asyncio.run(_update_packed_timestamp(encrypt_key = encrypt_key, password = password, module_names = module_names, timestamp = timestamp))

async def _update_packed_timestamp(encrypt_key, password, module_names, timestamp, db_params = db_params):
     cipher_suite = Fernet(encrypt_key)
     dbpassword = cipher_suite.decrypt(base64.urlsafe_b64decode(password)).decode()         # Decode base64 to get encrypted string and then decrypt
     db_params.update({"password": dbpassword})
     query = """UPDATE module_info SET packed_datetime = $1 WHERE module_name = ANY($2)"""
     try:
          conn = await asyncpg.connect(**db_params)
          await conn.execute(query, timestamp, module_names)
          print(f"Updated packed_timestamp for {len(module_names)} modules.")
          await conn.close()
     except Exception as e:
          print(f"Error updating packed_timestamp: {e}")

# ===========================================================================================================
# ===========================================================================================================

# Boolean for whether "shipped_datetime" is not NULL (for making entries immutable if shipped)
def get_shipped_timestamp_bool_sync(encrypt_key, password, module_names):
     return asyncio.run(_get_shipped_timestamp_bool(encrypt_key = encrypt_key, password = password, module_names = module_names))

async def _get_shipped_timestamp_bool(encrypt_key, password, module_names, db_params = db_params):
     cipher_suite = Fernet(encrypt_key)
     dbpassword = cipher_suite.decrypt(base64.urlsafe_b64decode(password)).decode()
     db_params.update({"password": dbpassword})
     query = """SELECT shipped_datetime FROM module_info WHERE module_name = ANY($1) """
     try:
          conn = await asyncpg.connect(**db_params)
          rows = await conn.fetch(query, module_names)
          await conn.close()
          datetimes = [row["shipped_datetime"] for row in rows] if rows else []
          return datetimes
     except Exception as e:
          print(f"Error obtaining shipped_datetime: {e}")

# ===========================================================================================================
# ===========================================================================================================

# Updates column "shipped_datetime" in "module_info"; used in titles of output files
def update_shipped_timestamp_sync(encrypt_key, password, module_names, timestamp):
     fileout_name = asyncio.run(_update_shipped_timestamp(encrypt_key = encrypt_key, password = password, module_names = module_names, timestamp = timestamp))
     return fileout_name

async def _update_shipped_timestamp(encrypt_key, password, module_names, timestamp, db_params = db_params):
     cipher_suite = Fernet(encrypt_key)
     dbpassword = cipher_suite.decrypt(base64.urlsafe_b64decode(password)).decode()
     db_params.update({"password": dbpassword})
     try:
          conn = await asyncpg.connect(**db_params)
          if (timestamp is None):
              query = """UPDATE module_info SET shipped_datetime = NULL WHERE module_name = ANY($1); """                   
              await conn.execute(query, module_names)
              await conn.close()
              print(f"Updated shipped_timestamp for {len(module_names)} modules.")
              return None
     except Exception as e:
          print(f"Error updating shipped_timestamp: {e}")
          return None
     query_fetch = """SELECT DISTINCT packed_datetime FROM module_info WHERE module_name = ANY($1); """
     query_update = """UPDATE module_info SET shipped_datetime = $1 WHERE packed_datetime = ANY($2) RETURNING module_name; """
     try:
          conn = await asyncpg.connect(**db_params)
          rows = await conn.fetch(query_fetch, module_names)
          packed_datetimes = [row["packed_datetime"] for row in rows]
          mod_names_out = await conn.fetch(query_update, timestamp, packed_datetimes)
          await conn.close()
          shipped_modules = [row["module_name"] for row in mod_names_out]
          print(f"Updated shipped_timestamp for {len(shipped_modules)} modules.")
          fileout_name = f"""shipping/shipmentout_{timestamp.strftime('%Y%m%d_%H%M%S')}_modules_{len(shipped_modules)}.csv"""
          os.makedirs("shipping", exist_ok = True)
          with open(fileout_name, "w", newline = "") as file:
               writer = csv.writer(file)
               for module in natsorted(shipped_modules):
                    writer.writerow([module])
          return fileout_name
     except Exception as e:
          print(f"Error updating shipped_timestamp: {e}")
          return None

# ===========================================================================================================
# ===========================================================================================================

# Fetches column "box_number" in "module_info" given modules
def get_box_number_sync(encrypt_key, password, module_names):
     return asyncio.run(_get_box_number(encrypt_key = encrypt_key, password = password, module_names = module_names))

async def _get_box_number(encrypt_key, password, module_names, db_params = db_params):
     cipher_suite = Fernet(encrypt_key)
     dbpassword = cipher_suite.decrypt(base64.urlsafe_b64decode(password)).decode()
     db_params.update({"password": dbpassword})
     query = f"""SELECT box_number FROM module_info WHERE module_name = ANY($1);"""
     try:
          conn = await asyncpg.connect(**db_params)
          rows = await conn.fetch(query, module_names)
          await conn.close()
          boxes = [row["box_number"] for row in rows] if rows else []
          return boxes
     except Exception as e:
          print(f"Error obtaining box_number: {e}")

# ===========================================================================================================
# ===========================================================================================================

# Updates column "box_number" in "module_info"
def update_box_number_sync(encrypt_key, password, module_names, box_number):
     asyncio.run(_update_box_number(encrypt_key = encrypt_key, password = password, module_names = module_names, box_number = box_number))

async def _update_box_number(encrypt_key, password, module_names, box_number, db_params = db_params):
     cipher_suite = Fernet(encrypt_key)
     dbpassword = cipher_suite.decrypt(base64.urlsafe_b64decode(password)).decode()
     db_params.update({"password": dbpassword})
     query = """UPDATE module_info SET box_number = $1 WHERE module_name = ANY($2)"""
     try:
          conn = await asyncpg.connect(**db_params)
          await conn.execute(query, box_number, module_names)
          print(f"Updated box_number for {len(module_names)} modules.")
          await conn.close()
     except Exception as e:
          print(f"Error updating box_number: {e}")

# ===========================================================================================================
# ===========================================================================================================   
        
# Fetches column "shipment_number" in "module_info" given modules
def get_shipment_number_sync(encrypt_key, password, module_names):
     return asyncio.run(_get_shipment_number(encrypt_key = encrypt_key, password = password, module_names = module_names))

async def _get_shipment_number(encrypt_key, password, module_names, db_params = db_params):
     cipher_suite = Fernet(encrypt_key)
     dbpassword = cipher_suite.decrypt(base64.urlsafe_b64decode(password)).decode()
     db_params.update({"password": dbpassword})
     query = f"""SELECT shipment_number FROM module_info WHERE module_name = ANY($1);"""
     try:
          conn = await asyncpg.connect(**db_params)
          rows = await conn.fetch(query, module_names)
          await conn.close()
          shipments = [row["shipment_number"] for row in rows] if rows else []
          return shipments
     except Exception as e:
          print(f"Error obtaining shipment_number: {e}")
          
# ===========================================================================================================
# ===========================================================================================================

# Updates column "shipment_number in "module info"
def update_shipment_number_sync(encrypt_key, password, module_names, shipment_number):
     asyncio.run(_update_shipment_number(encrypt_key = encrypt_key, password = password, module_names = module_names, shipment_number = shipment_number))

async def _update_shipment_number(encrypt_key, password, module_names, shipment_number, db_params = db_params):
     cipher_suite = Fernet(encrypt_key)
     dbpassword = cipher_suite.decrypt(base64.urlsafe_b64decode(password)).decode()
     db_params.update({"password": dbpassword})
     query = """UPDATE module_info SET shipment_number = $1 WHERE module_name = ANY($2)"""
     try:
          conn = await asyncpg.connect(**db_params)
          await conn.execute(query, shipment_number, module_names)
          print(f"Updated shipment_number for {len(module_names)} modules.")
          await conn.close()
     except Exception as e:
          print(f"Error updating shipment_number: {e}")

# ===========================================================================================================
# ===========================================================================================================   

# Fetches list of boxes in database
def get_boxes_sync(encrypt_key, password):
     return asyncio.run(_get_boxes(encrypt_key = encrypt_key, password = password))

async def _get_boxes(encrypt_key, password, db_params = db_params):
     cipher_suite = Fernet(encrypt_key)
     dbpassword = cipher_suite.decrypt(base64.urlsafe_b64decode(password)).decode()
     db_params.update({"password": dbpassword})
     query = f"""SELECT box_number FROM module_info WHERE box_number IS NOT NULL;"""
     try:
          conn = await asyncpg.connect(**db_params)
          rows = await conn.fetch(query)
          await conn.close()
          boxes = [row["box_number"] for row in rows] if rows else []
          sorted_boxes = [list(y) for x, y in groupby(sorted(boxes))]
          available_boxes = []
          for i in range(len(sorted_boxes)):
               if (all(x == sorted_boxes[i][0] for x in sorted_boxes[i])): available_boxes.append(sorted_boxes[i][0])
          return available_boxes
     except Exception as e:
          print(f"Error obtaining box_number: {e}")

# ===========================================================================================================
# ===========================================================================================================   

# Fetches list of shipments in database
def get_shipments_sync(encrypt_key, password):
     return asyncio.run(_get_shipments(encrypt_key = encrypt_key, password = password))

async def _get_shipments(encrypt_key, password, db_params = db_params):
     cipher_suite = Fernet(encrypt_key)
     dbpassword = cipher_suite.decrypt(base64.urlsafe_b64decode(password)).decode()
     db_params.update({"password": dbpassword})
     query = f"""SELECT shipment_number FROM module_info WHERE shipment_number IS NOT NULL;"""
     try:
          conn = await asyncpg.connect(**db_params)
          rows = await conn.fetch(query)
          await conn.close()
          shipments = [row["shipment_number"] for row in rows] if rows else []
          sorted_shipments = [list(y) for x, y in groupby(sorted(shipments))]
          available_shipments = []
          for i in range(len(sorted_shipments)):
               if (all(x == sorted_shipments[i][0] for x in sorted_shipments[i])): available_shipments.append(sorted_shipments[i][0])
          return available_shipments
     except Exception as e:
          print(f"Error obtaining shipment_number: {e}")

'''
# ===========================================================================================================
# ===========================================================================================================   

# Fetches list of containers in database
def get_containers_sync(encrypt_key, password):
     return asyncio.run(_get_containers(encrypt_key = encrypt_key, password = password))

async def _get_containers(encrypt_key, password, db_params = db_params):
     cipher_suite = Fernet(encrypt_key)
     dbpassword = cipher_suite.decrypt(base64.urlsafe_b64decode(password)).decode()
     db_params.update({"password": dbpassword})
     query = f"""SELECT container_number FROM module_info WHERE container_number IS NOT NULL;"""
     try:
          conn = await asyncpg.connect(**db_params)
          rows = await conn.fetch(query)
          await conn.close()
          containers = [row["container_number"] for row in rows] if rows else []
          sorted_containers = [list(y) for x, y in groupby(sorted(containers))]
          available_containers = []
          for i in range(len(sorted_containers)):
               if (all(x == sorted_containers[i][0] for x in sorted_containers[i])): available_containers.append(sorted_containers[i][0])
          return available_containers
     except Exception as e:
          print(f"Error obtaining container_number: {e}")
'''

# ===========================================================================================================
# ===========================================================================================================

# Fetches the names of the boxes inside a shipment with a given number
def get_boxes_in_shipment_sync(encrypt_key, password, shipment_number):
     return asyncio.run(_get_boxes_in_shipment(encrypt_key = encrypt_key, password = password, shipment_number = shipment_number))
        
async def _get_boxes_in_shipment(encrypt_key, password, shipment_number, db_params = db_params):
     cipher_suite = Fernet(encrypt_key)
     dbpassword = cipher_suite.decrypt(base64.urlsafe_b64decode(password)).decode() 
     db_params.update({"password": dbpassword})
     query = f"""SELECT box_number FROM module_info WHERE shipment_number = $1;"""
     try:
          conn = await asyncpg.connect(**db_params)
          rows = await conn.fetch(query, shipment_number)
          await conn.close()
          boxes = [row["box_number"] for row in rows] if rows else []
          sorted_boxes = [list(y) for x, y in groupby(sorted(boxes))]
          return sorted_boxes
     except Exception as e:
          print(f"Error obtaining boxes in shipment: {e}")
          
# ===========================================================================================================
# ===========================================================================================================

# Fetches the names of the modules inside a box with a given number
def get_modules_in_box_sync(encrypt_key, password, box_number):
     return asyncio.run(_get_modules_in_box(encrypt_key = encrypt_key, password = password, box_number = box_number))

async def _get_modules_in_box(encrypt_key, password, box_number, db_params = db_params):
     cipher_suite = Fernet(encrypt_key)
     dbpassword = cipher_suite.decrypt(base64.urlsafe_b64decode(password)).decode()
     db_params.update({"password": dbpassword})
     query = f"""SELECT module_name FROM module_info WHERE box_number = $1;"""
     try:
          conn = await asyncpg.connect(**db_params)
          rows = await conn.fetch(query, box_number)
          await conn.close()
          modules = [row["module_name"] for row in rows] if rows else []
          return modules
     except Exception as e:
          print(f"Error obtaining modules in box: {e}")

# ===========================================================================================================
# ===========================================================================================================

# Fetches the names of the modules inside a shipment with a given number
def get_modules_in_shipment_sync(encrypt_key, password, shipment_number):
     return asyncio.run(_get_modules_in_shipment(encrypt_key = encrypt_key, password = password, shipment_number = shipment_number))

async def _get_modules_in_shipment(encrypt_key, password, shipment_number, db_params = db_params):
     cipher_suite = Fernet(encrypt_key)
     dbpassword = cipher_suite.decrypt(base64.urlsafe_b64decode(password)).decode()
     db_params.update({"password": dbpassword})
     query = f"""SELECT module_name FROM module_info WHERE shipment_number = $1;"""
     try:
          conn = await asyncpg.connect(**db_params)
          rows = await conn.fetch(query, shipment_number)
          await conn.close()
          modules = [row["module_name"] for row in rows] if rows else []
          return modules
     except Exception as e:
          print(f"Error obtaining modules in box: {e}")

'''
# ===========================================================================================================
# ===========================================================================================================

# Fetches the names of the modules inside a container with a given number
def get_modules_in_container_sync(encrypt_key, password, container_number):
     return asyncio.run(_get_modules_in_container(encrypt_key = encrypt_key, password = password, container_number = container_number))

async def _get_modules_in_container(encrypt_key, password, container_number, db_params = db_params):
     cipher_suite = Fernet(encrypt_key)
     dbpassword = cipher_suite.decrypt(base64.urlsafe_b64decode(password)).decode()
     db_params.update({"password": dbpassword})
     query = f"""SELECT module_name FROM module_info WHERE container_number = $1;"""
     try:
          conn = await asyncpg.connect(**db_params)
          rows = await conn.fetch(query, container_number)
          await conn.close()
          modules = [row["module_name"] for row in rows] if rows else []
          return modules
     except Exception as e:
          print(f"Error obtaining modules in box: {e}")
'''

# ===========================================================================================================
# ===========================================================================================================

# Get position of module in box
def get_position_in_box_sync(encrypt_key, password, module_name):
     return asyncio.run(_get_position_in_box(encrypt_key = encrypt_key, password = password, module_name = module_name))

async def _get_position_in_box(encrypt_key, password, module_name, db_params = db_params):
     cipher_suite = Fernet(encrypt_key)
     dbpassword = cipher_suite.decrypt(base64.urlsafe_b64decode(password)).decode()
     db_params.update({"password": dbpassword})
     query = f"""SELECT position_in_box FROM module_info WHERE module_name = $1;"""
     try:
          conn = await asyncpg.connect(**db_params)
          rows = await conn.fetch(query, module_name)
          await conn.close()
          return [row["position_in_box"] for row in rows] if rows else []
     except Exception as e:
          print(f"Error obtaining position_in_box: {e}")

# ===========================================================================================================
# ===========================================================================================================

# Update position of module in box
def update_position_in_box_sync(encrypt_key, password, module_name, position_in_box):
     asyncio.run(_update_position_in_box(encrypt_key = encrypt_key, password = password, module_name = module_name, position_in_box = position_in_box))
     
async def _update_position_in_box(encrypt_key, password, module_name, position_in_box, db_params = db_params):
     cipher_suite = Fernet(encrypt_key)
     dbpassword = cipher_suite.decrypt(base64.urlsafe_b64decode(password)).decode()
     db_params.update({"password": dbpassword})
     query = """UPDATE module_info SET position_in_box = $1 WHERE module_name = $2;"""
     try:
          conn = await asyncpg.connect(**db_params)
          await conn.execute(query, position_in_box, module_name)
          print(f"Updated position_in_box for {module_name}.")
          await conn.close()
     except Exception as e:
          print(f"Error updating position_in_box: {e}")
          
# ===========================================================================================================
# ===========================================================================================================

# Calculates the number of boxes inside a shipment with a given number
def get_number_of_boxes_sync(encrypt_key, password, shipment_number):
     return asyncio.run(_get_number_of_boxes(encrypt_key = encrypt_key, password = password, shipment_number = shipment_number))

async def _get_number_of_boxes(encrypt_key, password, shipment_number, db_params = db_params):
     cipher_suite = Fernet(encrypt_key)
     dbpassword = cipher_suite.decrypt(base64.urlsafe_b64decode(password)).decode()
     db_params.update({"password": dbpassword})
     query = """SELECT box_number FROM module_info WHERE shipment_number = $1;"""
     try:
          conn = await asyncpg.connect(**db_params)
          rows = await conn.fetch(query, shipment_number)
          await conn.close()
          boxes = [row["box_number"] for row in rows] if rows else []
          sorted_boxes = [list(y) for x, y in groupby(sorted(boxes))]
          return len(sorted_boxes)
     except Exception as e:
          print(f"Error obtaining number of boxes: {e}")

# ===========================================================================================================
# ===========================================================================================================

# Gets information about whether a module has been thermal cycled (tested at negative temperatures): "test_iv" is False
def module_thermal_cycled_sync(encrypt_key, password, module_names):
     return asyncio.run(_module_thermal_cycled(encrypt_key = encrypt_key, password = password, module_names = module_names))

async def _module_thermal_cycled(encrypt_key, password, module_names, db_params = db_params):
     cipher_suite = Fernet(encrypt_key)
     dbpassword = cipher_suite.decrypt(base64.urlsafe_b64decode(password)).decode()
     db_params.update({"password": dbpassword})
     query = """SELECT thermal_cycle FROM module_info WHERE module_name = ANY($1);"""
     try:
          conn = await asyncpg.connect(**db_params)
          rows = await conn.fetch(query, module_names)
          await conn.close()
          tested = [row["thermal_cycle"] for row in rows] if rows else []
          return tested
     except Exception as e:
          print(f"Error obtaining thermal_cycle: {e}")

# ===========================================================================================================
# ===========================================================================================================

# Class for handling the recording of modules into boxes; contains module-loading popup as well as popups for giving boxes IDs and for putting them inside shipments (if needed)
class enter_part_barcodes_box(tkinter.Toplevel):
     def __init__(self, parent, encryption_key, dbshipper_pass, upload_file_with_part_out, max_mod_per_box, entries = None):
          super().__init__(parent)
          
          self.title("Enter barcode of parts packed in this module container")
          
          if (entries is None):
               entries = []
               
          top_frame = tkinter.Frame(self)
          middle_frame = tkinter.Frame(self)
          bottom_frame = tkinter.Frame(self)

          box_frame = Frame(top_frame, bd = 1, relief = "sunken", highlightbackground = "black", highlightthickness = 1)
          shipment_frame = Frame(bottom_frame, bd = 1, relief = "sunken", highlightbackground = "black", highlightthickness = 1)
          
          top_frame.pack(pady = 10)
          middle_frame.pack(pady = 10)
          bottom_frame.pack(pady = 10)
          
          datetime_now = datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
          datetime_now_var = StringVar(master = self, value = datetime_now)
          
          available_boxes = get_boxes_sync(encrypt_key = encryption_key, password = dbshipper_pass)
          available_shipments = get_shipments_sync(encrypt_key = encryption_key, password = dbshipper_pass)
     
          upload_from_file_button = Button(top_frame, text = "Upload parts from file (optional)", command = upload_file_with_part_out, state = "disabled")
          datetime_now_label = Label(top_frame, text = f"Now:", justify = "right", anchor = "e")
          datetime_now_entry = Entry(top_frame, textvariable = datetime_now_var, width = 20, bd = 1.5, highlightbackground = "black", highlightthickness = 1, state = "readonly")
          
          upload_from_file_button.grid(row = 0, column = 0, columnspan = 1, padx = 10, sticky = "w")
          datetime_now_label.grid(row = 0, column = 3, columnspan = 1, padx = (10, 0), sticky = "e")
          datetime_now_entry.grid(row = 0, column = 4, columnspan = 1, padx = (0, 10))

          def select_box(box):
               box_id_entry.delete(0, "end")
               box_id_entry.insert(0, box)
               box_id_entry["fg"] = box_id_entry._default_fg_color
               
          def show_box_menu():
               x = box_frame.winfo_rootx()
               y = box_frame.winfo_rooty() + box_frame.winfo_height()
               box_menu.post(x, y)
          
          dropdown_button_box = Button(box_frame, text = "\u25BC", bd = 0, highlightthickness = 0, relief = "flat", command = show_box_menu)
          box_id_entry = PlaceholderEntry(box_frame, "Enter box ID...", width = 15, bd = 0, highlightthickness = 0)

          dropdown_button_box.pack(side = "right", padx = (0, 2), pady = (0, 4))
          box_id_entry.pack(side = "left", padx = (2, 0))

          box_menu = Menu(top_frame, tearoff = 0)
          for box in available_boxes:
               box_menu.add_command(label = box, command = lambda b = box: select_box(b))
                                                                                
          box_frame.grid(row = 0, column = 1, columnspan = 2)
          
          current_modules = []
          def enter():
               box_id = id_setter(box_id_entry.get(), "B")
               if (box_id == 1): return
               box_id_entry.delete(0, "end")
               box_id_entry.insert(0, box_id)
               box_id_entry["state"] = "readonly"
               dropdown_button_box["state"] = "disabled"
               upload_from_file_button["state"] = "normal"
               for item in middle_frame.grid_slaves():
                    item["state"] = "normal"
               export_checkbox["state"] = "normal"
               shipment_checkbox["state"] = "normal"
               submit_button["state"] = "normal"
               cancel_button.grid_remove()
               back_button.grid(row = 1, column = 2, columnspan = 1, sticky = "ne")
               enter_button["state"] = "disabled"
               ship_button["state"] = "normal"
               if (box_id in available_boxes):
                    upload_from_file_button["state"] = "disabled"
                    modules = get_modules_in_box_sync(encryption_key, dbshipper_pass, box_id)
                    sorted_modules = [list(y) for x, y in groupby(sorted(modules))]
                    available_modules = []
                    for i in range(len(sorted_modules)):
                         if (all(x == sorted_modules[i][0] for x in sorted_modules[i])):
                              module_id = sorted_modules[i][0]
                              available_modules.append(module_id)
                              position = get_position_in_box_sync(encryption_key, dbshipper_pass, module_id)
                              entries[int(position[0]) - 1].insert(0, module_id)
                              current_modules.append(entries[int(position[0]) - 1].get())
                    shipments = get_shipment_number_sync(encryption_key, dbshipper_pass, modules)
                    datetimes = get_shipped_timestamp_bool_sync(encryption_key, dbshipper_pass, available_modules)
                    dt_bool = not any(datetimes)
                    shipment_id = shipments[0] if (all(x == shipments[0] for x in shipments)) else None
                    if (dt_bool == False):
                         for i in range(int(max_mod_per_box)):
                              entries[i]["state"] = "disabled"
                         export_checkbox["state"] = "disabled"
                         shipment_checkbox["state"] = "disabled"
                         submit_button["state"] = "disabled"
                         ship_button.pack_forget()
                         received_button.pack(side = "left", padx = 10, pady = 10)
                         received_button["state"] = "normal"
                         if (shipment_id is None):
                              shipment_checkbox.deselect()
                              shipment_id_entry["state"] = "disabled"
                              dropdown_button_shipment["state"] = "disabled"
                              shipment_see_inside["state"] = "disabled"
                         else:
                              shipment_checkbox.select()
                              shipment_id_entry["state"] = "normal"
                              dropdown_button_shipment["state"] = "normal"
                              shipment_see_inside["state"] = "normal"
                              shipment_id_entry.delete(0, "end")
                              shipment_id_entry.insert(0, shipment_id)
                              shipment_id_entry["fg"] = shipment_id_entry._default_fg_color
                              shipment_id_entry["state"] = "disabled"
                              dropdown_button_shipment["state"] = "disabled"
                              shipment_see_inside["state"] = "disabled"
                    else:
                         if (shipment_id is not None):
                              shipment_checkbox.select()
                              shipment_id_entry["state"] = "normal"
                              dropdown_button_shipment["state"] = "normal"
                              shipment_see_inside["state"] = "normal"
                              shipment_id_entry.delete(0, "end")
                              shipment_id_entry.insert(0, shipment_id)
                              shipment_id_entry["fg"] = shipment_id_entry._default_fg_color
                         else:
                              shipment_checkbox.deselect()
                              shipment_id_entry["state"] = "normal"
                              shipment_id_entry.delete(0, "end")
                              shipment_id_entry._put_placeholder()
                              shipment_id_entry["state"] = "disabled"
                              dropdown_button_shipment["state"] = "disabled"

          def back():
               box_id_entry.delete(0, "end")
               box_id_entry._put_placeholder()
               box_id_entry["state"] = "normal"
               box_id_entry["fg"] = box_id_entry._default_fg_color
               dropdown_button_box["state"] = "normal"
               upload_from_file_button["state"] = "disabled"
               for item in middle_frame.grid_slaves():
                    item["state"] = "normal"
               for i in range(int(max_mod_per_box)):
                    entries[i].delete(0, "end")
               for item in middle_frame.grid_slaves():
                    item["state"] = "disabled"
               export_checkbox["state"] = "disabled"
               shipment_checkbox.deselect()
               shipment_checkbox["state"] = "disabled"
               shipment_id_entry["state"] = "normal"
               shipment_id_entry.delete(0, "end")
               shipment_id_entry._put_placeholder()
               shipment_id_entry["state"] = "disabled"
               dropdown_button_shipment["state"] = "disabled"
               shipment_see_inside["state"] = "disabled"
               submit_button["state"] = "disabled"
               received_button.pack_forget()
               ship_button.pack(side = "left", padx = 10, pady = 10)
               ship_button["state"] = "disabled"
               current_modules.clear()
               cancel_button.grid(row = 1, column = 2, columnspan = 1, sticky="ne")
               back_button.grid_remove()
               enter_button["state"] = "normal"
               self.focus_set()
               
          def cancel():
               self.destroy()

          enter_button = Button(top_frame, text = "Enter", command = enter, width = 5)
          back_button = Button(top_frame, text = "Back", command = back, width = 5)
          cancel_button = Button(top_frame, text = "Cancel", command = cancel, width = 5)                      

          enter_button.grid(row = 1, column = 1, columnspan = 1, sticky = "nw")
          back_button.grid_remove()
          cancel_button.grid(row = 1, column = 2, columnspan = 1, sticky = "ne")
          
          module_present_label = Label(middle_frame, wraplength = 600 ,fg = "red", text = f"Modules must be present in postgres `module_info` table to record shipments.", state = "disabled")
          module_present_label.grid(row = 0, column = 0, columnspan = 5, pady = (0, 10), sticky = "s")
          
          num_entries, cols = int(max_mod_per_box), 2
          for i in range(num_entries):
               row, col = (1 + (i // cols)), (i % cols)
               listlabel = Label(middle_frame, text = f"{i + 1}:", state = "disabled")
               listlabel.grid(row = row, column = (col * 2), padx = (10, 0), pady = 2)
               entry = Entry(middle_frame, width = 30, state = "disabled")
               entry.grid(row = row, column = ((col * 2) + 1), padx = (0, 10), pady = 2)
               entries.append(entry)

          def toggle_on_shipment():
               if (shipment_var.get() == 1):
                    shipment_id_entry["state"] = "normal"
                    dropdown_button_shipment["state"] = "normal"
                    shipment_see_inside["state"] = "normal"
                    shipment_checkbox.focus_set()
               else:
                    shipment_id_entry.delete(0, "end")
                    shipment_id_entry._put_placeholder()
                    shipment_id_entry["state"] = "disabled"
                    dropdown_button_shipment["state"] = "disabled"
                    shipment_see_inside["state"] = "disabled"
                    
          def inside_shipment_textbox(shipment_id, message):
               window = Toplevel()
               window.title("Contents of Shipment " + shipment_id)
               from tkinter import scrolledtext
               text_area = scrolledtext.ScrolledText(window, wrap = tkinter.WORD, width = 80, height = 20)
               text_area.pack(padx = 10, pady = 10, fill = tkinter.BOTH, expand = True)
               text_area.insert(END, message)
               text_area.config(state = DISABLED)
                    
          def see_inside_shipment():
               message = ""
               shipment_dict = {}
               if (shipment_var.get() == 1):
                    shipment_id = id_setter(shipment_id_entry.get(), "C/X")
               if (shipment_id == 1): return
               if (shipment_var.get() == 1): boxes = get_boxes_in_shipment_sync(encryption_key, dbshipper_pass, shipment_id)
               number_of_boxes = get_number_of_boxes_sync(encryption_key, dbshipper_pass, shipment_id)
               message += "".join(f"Number of boxes in shipment: {number_of_boxes}\n\n")
               for i in range(len(boxes)):
                    box_list = []
                    if (all(x == boxes[i][0] for x in boxes[i])): box_id = boxes[i][0]
                    modules = get_modules_in_box_sync(encryption_key, dbshipper_pass, box_id)
                    sorted_modules = [list(y) for x, y in groupby(sorted(modules))]
                    for i in range(len(sorted_modules)):
                         if (all(x == sorted_modules[i][0] for x in sorted_modules[i])): module_id = sorted_modules[i][0]
                         position = get_position_in_box_sync(encryption_key, dbshipper_pass, module_id)
                         box_list.append("".join(f"{position[0]}: {module_id}"))
                    shipment_dict["".join(f"{box_id}")] = sorted(box_list)
               for key in shipment_dict.keys():
                    message += "".join(f"{key}\n")
                    for item in shipment_dict[key]:
                         message += "".join(f"{item}\n")
                    message += "\n"
               inside_shipment_textbox(shipment_id, message)
               
          export_var = IntVar()
          shipment_var = IntVar()
          
          export_var.set(1)

          export_checkbox = Checkbutton(bottom_frame, text = "Export to file (shipping/packed...csv)", variable = export_var, state = "disabled")
          shipment_checkbox = Checkbutton(bottom_frame, text = "Pack into shipment?", variable = shipment_var, command = toggle_on_shipment, state = "disabled")
          shipment_see_inside = Button(bottom_frame, text = "See inside", command = see_inside_shipment, state = "disabled")

          export_checkbox.grid(row = (num_entries // 2), column = 0, columnspan = 1, padx = 10)
          shipment_checkbox.grid(row = (num_entries // 2), column = 1, columnspan = 1, padx = (10, 2))
          shipment_see_inside.grid(row = (num_entries // 2), column = 4, columnspan = 1, padx = (2, 10))

          def select_shipment(shipment):
               shipment_id_entry.delete(0, "end")
               shipment_id_entry.insert(0, shipment)
               shipment_id_entry["fg"] = shipment_id_entry._default_fg_color
               
          shipment_menu = Menu(top_frame, tearoff = 0)

          def show_shipment_menu():
               shipment__menu.delete(0, "end")
               if (shipment_var.get() == 1):
                    for shipment in available_shipments:
                         shipment_menu.add_command(label = shipment, command = lambda s = shipment: select_shipment(s))
               x = shipment_frame.winfo_rootx()
               y = shipment_frame.winfo_rooty() + shipment_frame.winfo_height()
               shipment_menu.post(x, y)

          dropdown_button_shipment = Button(shipment_frame, text = "\u25BC", bd = 0, highlightthickness = 0, relief = "flat", command = show_shipment_menu)
          shipment_id_entry = PlaceholderEntry(shipment_frame, "Enter ID...", width = 15, bd = 0, highlightthickness = 0)

          dropdown_button_shipment["state"] = "disabled"
          shipment_id_entry["state"] = "disabled"

          dropdown_button_shipment.pack(side = "right", padx = (0, 2), pady = (0, 4))
          shipment_id_entry.pack(side = "left", padx = (2, 0))
                                                                                
          shipment_frame.grid(row = (num_entries // 2), column = 3, columnspan = 1, padx = (0, 10))

          def update_db_packed():
               module_update_pack = [entry.get() for entry in entries if entry.get().strip() != ""]
               if (len(module_update_pack) > 0):
                    removed_modules = [m for m in current_modules if m not in module_update_pack]
                    added_modules = [m for m in module_update_pack if m not in current_modules]
                    removed_positions = set()
                    occupied_positions = set()
                    if (shipment_var.get() == 1):
                         shipment_id = id_setter(shipment_id_entry.get(), "C/X")
                         if (shipment_id == 1): return
                    else:
                         shipment_id = None
                    if (removed_modules):
                         update_packed_timestamp_sync(encrypt_key = encryption_key, password = dbshipper_pass.strip(), module_names = removed_modules, timestamp = None, savetofile = False)
                         update_box_number_sync(encrypt_key = encryption_key, password = dbshipper_pass.strip(), module_names = removed_modules, box_number = None)
                         for module in removed_modules:
                              position = get_position_in_box_sync(encrypt_key = encryption_key, password = dbshipper_pass.strip(), module_name = module)
                              if (position):
                                   removed_positions.add(int(position[0]))
                         update_shipment_number_sync(encrypt_key = encryption_key, password = dbshipper_pass.strip(), module_names = removed_modules, shipment_number = None)
                         for module in current_modules:
                              position = get_position_in_box_sync(encrypt_key = encryption_key, password = dbshipper_pass.strip(), module_name = module)
                              if (position):
                                   occupied_positions.add(int(position[0]))
                         already_empty = set(range(1, int(max_mod_per_box) + 1)) - occupied_positions
                         all_empty_after = already_empty | removed_positions
                         empty_row_starts = [i for i in range(1, int(max_mod_per_box), 2) if i in all_empty_after and (i + 1) in all_empty_after]
                         occupied_after = occupied_positions - removed_positions
                         should_shift = bool(removed_positions) and any(any(p > row_start + 1 for p in occupied_after) for row_start in empty_row_starts)
                    for module in added_modules:
                         box_id_db = get_box_number_sync(encrypt_key = encryption_key, password = dbshipper_pass, module_names = [module])
                         if ((box_id_entry.get() != box_id_db[0]) and (box_id_db[0] is not None)):
                              show_error_on_top("Upload Error", "See terminal.")
                              print(f"Upload Error: Module {module} already packaged in box {box_id_db[0]}.")
                              return
                         shipment_id_db = get_shipment_number_sync(encrypt_key = encryption_key, password = dbshipper_pass, module_names = [module])
                         if ((shipment_id_entry.get() != shipment_id_db[0]) and (shipment_id_db[0] is not None)):
                              show_error_on_top("Upload Error", "See terminal.")
                              print(f"Upload Error: Module {module} already packaged in shipment {shipment_id_db[0]}.")
                         #tested_mod = module_thermal_cycled_sync(encrypt_key = encryption_key, password = dbshipper_pass, module_names = [module])
                         #if (tested_mod == [None]):
                         #     show_error_on_top("Upload Error", "See terminal.") 
                         #     print(f"Upload Error: Module {module} not thermal cycled.")
                         #     return
                    if (len(datetime_now_var.get().strip()) == 0): datetime_now_var.set(datetime_now)
                    datetime_now_obj = datetime.strptime(datetime_now_var.get().strip(), "%Y-%m-%d %H:%M:%S")
                    for module in removed_modules:
                         update_position_in_box_sync(encrypt_key = encryption_key, password = dbshipper_pass.strip(), module_name = module, position_in_box = None)
                    if ((removed_modules) and (should_shift)):
                         for i, module in enumerate(module_update_pack, start = 1):
                              update_position_in_box_sync(encrypt_key = encryption_key, password = dbshipper_pass.strip(), module_name = module, position_in_box = str(i))
                    else:
                         for i, module in enumerate(module_update_pack, start = 1):
                              if module in set(added_modules):
                                   update_position_in_box_sync(encrypt_key = encryption_key, password = dbshipper_pass.strip(), module_name = module, position_in_box = str(i))
                    if (added_modules):
                         update_box_number_sync(encrypt_key = encryption_key, password = dbshipper_pass.strip(), module_names = added_modules, box_number = box_id_entry.get())
                    update_packed_timestamp_sync(encrypt_key = encryption_key, password = dbshipper_pass.strip(), module_names = module_update_pack, timestamp = datetime_now_obj, savetofile = bool(export_var.get()))
                    update_shipment_number_sync(encrypt_key = encryption_key, password = dbshipper_pass.strip(), module_names = module_update_pack, shipment_number = shipment_id)
                    self.destroy()

          def ready_to_ship():
               modules = []
               for i in range(int(max_mod_per_box)):
                    entries[i]["state"] = "disabled"
                    modules.append(entries[i].get())
               upload_from_file_button["state"] = "disabled"
               shipment_checkbox["state"] = "disabled"
               shipment_id_entry["state"] = "disabled"
               submit_button["state"] = "disabled"
               shipment_see_inside["state"] = "disabled"
               ship_button.pack_forget()
               received_button.pack(side = "left", padx = 10, pady = 10)
               received_button["state"] = "normal"

               if (len(datetime_now_var.get().strip()) == 0): datetime_now_var.set(datetime_now)
               datetime_now_obj = datetime.strptime(datetime_now_var.get().strip(), "%Y-%m-%d %H:%M:%S") 

               fileout_name = update_shipped_timestamp_sync(encrypt_key = encryption_key, password = dbshipper_pass.strip(), module_names = [modules[0]], timestamp = datetime_now_obj)
               print("List of modules saved under ", fileout_name)
               
               if fileout_name:
                   webbrowser.open(f"https://cmsr-shipment.web.cern.ch/tracking/add/")

          def received_shipment():
               modules = []
               for i in range(int(max_mod_per_box)):
                    entries[i]["state"] = "normal"
                    modules.append(entries[i].get())
               shipment_checkbox["state"] = "normal"
               shipment_id_entry["state"] = "normal"
               submit_button["state"] = "normal"
               shipment_see_inside["state"] = "normal"
               received_button.pack_forget()
               ship_button.pack(side = "left", padx = 10, pady = 10)
               ship_button["state"] = "normal"
               update_packed_timestamp_sync(encryption_key, dbshipper_pass, modules, timestamp = None, savetofile = False)
               update_shipped_timestamp_sync(encryption_key, dbshipper_pass, [m for m in modules if m.strip() != ""], timestamp = None)

          button_row_frame = Frame(bottom_frame)
          button_row_frame.grid(row = (num_entries // 2) + 1, column = 0, columnspan = 5)

          submit_button = Button(button_row_frame, text = "Record to DB", command = update_db_packed, state = "disabled")
          submit_button.pack(side = "left", padx = 10, pady = 10)

          ship_button = Button(button_row_frame, text = "Ready to Ship", command = ready_to_ship, state = "disabled", width = 9)
          ship_button.pack(side = "left", padx = 10, pady = 10)

          received_button = Button(button_row_frame, text = "Received", command = received_shipment, state = "disabled", width = 9)
          received_button.pack(side = "left", padx = 10, pady = 10)
          received_button.pack_forget()

# ===========================================================================================================
# ===========================================================================================================

# If we have a bunch of standalone boxes that we want to put into a shipment, we can do so with this class
class enter_part_barcodes_shipment(tkinter.Toplevel):
     def __init__(self, parent, encryption_key, dbshipper_pass, upload_file_with_part_out, max_box_per_shipment, entries = None):
          super().__init__(parent)
          
          self.title("Record Shipment")

          if (entries is None):
               entries = []

          top_frame = tkinter.Frame(self)
          middle_frame = tkinter.Frame(self)
          bottom_frame = tkinter.Frame(self)

          top_frame.pack(pady = 10)
          middle_frame.pack(pady = 10)
          bottom_frame.pack(pady = 10)

          shipment_frame = Frame(top_frame, bd = 1, relief = "sunken", highlightbackground = "black", highlightthickness = 1)
        
          datetime_now = datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
          datetime_now_var = StringVar(master = self, value = datetime_now)

          available_shipments = get_shipments_sync(encrypt_key = encryption_key, password = dbshipper_pass)
     
          upload_from_file_button = Button(top_frame, text = "Upload parts from file (optional)", command = upload_file_with_part_out, state = "disabled")
          datetime_now_label = Label(top_frame, text = f"Now:", justify = "right", anchor = "e")
          datetime_now_entry = Entry(top_frame, textvariable = datetime_now_var, width = 20, bd = 1.5, highlightbackground = "black", highlightthickness = 1, state = "readonly")

          upload_from_file_button.grid(row = 1, column = 0, columnspan = 1, padx = 10, sticky = "w")
          datetime_now_label.grid(row = 1, column = 3, columnspan = 1, padx = (10, 0), sticky = "e")
          datetime_now_entry.grid(row = 1, column = 4, columnspan = 1, padx = (0, 10))

          def select_shipment(shipment):
               shipment_id_entry.delete(0, "end")
               shipment_id_entry.insert(0, shipment)
               shipment_id_entry["fg"] = shipment_id_entry._default_fg_color

          def show_shipment_menu():                                                     
               shipment_menu.delete(0, "end")
               for shipment in available_shipments:
                    shipment_menu.add_command(label = shipment, command = lambda s = shipment: select_shipment(s))
               x = shipment_frame.winfo_rootx()
               y = shipment_frame.winfo_rooty() + shipment_frame.winfo_height()
               shipment_menu.post(x, y)
          
          dropdown_button = Button(shipment_frame, text = "\u25BC", bd = 0, highlightthickness = 0, relief = "flat", command = show_shipment_menu)
          shipment_id_entry = PlaceholderEntry(shipment_frame, "Enter shipment ID...", width = 20, bd = 0, highlightthickness = 0)

          dropdown_button.pack(side = "right", padx = (0, 2), pady = (0, 4))
          shipment_id_entry.pack(side = "left", padx = (2, 0))

          shipment_menu = Menu(top_frame, tearoff = 0)
          shipment_frame.grid(row = 1, column = 1, columnspan = 2)

          current_boxes = []
          def enter():
               shipment_id = id_setter(shipment_id_entry.get(), "C/X")     
               if (shipment_id == 1): return
               shipment_id_entry.delete(0, "end")
               shipment_id_entry.insert(0, shipment_id)
               shipment_id_entry["state"] = "readonly"
               dropdown_button["state"] = "disabled"
               upload_from_file_button["state"] = "normal"
               for item in middle_frame.grid_slaves():
                    item["state"] = "normal"
               submit_button["state"] = "normal"
               ship_button["state"] = "disabled"
               for item in top_frame.grid_slaves():
                    if isinstance(item, Radiobutton): item["state"] = "disabled"
               if (shipment_id in get_shipments_sync(encrypt_key = encryption_key, password = dbshipper_pass)):
                    boxes = get_boxes_in_shipment_sync(encryption_key, dbshipper_pass, shipment_id)
                    datetimes = []
                    all_available_modules = []
                    for i in range(len(boxes)):
                         if (all(x == boxes[i][0] for x in boxes[i])):
                              box_id = boxes[i][0]
                              entries[i].insert(0, boxes[i][0])
                              current_boxes.append(entries[i].get())
                         modules = get_modules_in_box_sync(encryption_key, dbshipper_pass, box_id)
                         sorted_modules = [list(y) for x, y in groupby(sorted(modules))]
                         available_modules = []
                         for i in range(len(sorted_modules)):
                              if (all(x == sorted_modules[i][0] for x in sorted_modules[i])):
                                   module_id = sorted_modules[i][0]
                                   available_modules.append(module_id)
                         all_available_modules.extend(available_modules)
                         datetimes.extend(get_shipped_timestamp_bool_sync(encryption_key, dbshipper_pass, available_modules))
                    dt_bool = not any(datetimes)
                    if (dt_bool == False):
                         upload_from_file_button["state"] = "disabled"
                         for i in range(int(max_box_per_shipment)):
                              entries[i]["state"] = "disabled"
                         submit_button["state"] = "disabled"
                         ship_button.pack_forget()
                         received_button.pack(side = "left", padx = 10, pady = 10)
                         received_button["state"] = "normal"
                    else:
                         ship_button["state"] = "normal"
               cancel_button.grid_remove()
               back_button.grid(row = 2, column = 2, columnspan = 1, sticky = "ne")
            
          def back():
               shipment_id_entry.delete(0, "end")
               shipment_id_entry._put_placeholder()
               shipment_id_entry["state"] = "normal"
               shipment_id_entry["fg"] = shipment_id_entry._default_fg_color
               dropdown_button["state"] = "normal"
               upload_from_file_button["state"] = "disabled"
               for item in middle_frame.grid_slaves():
                    item["state"] = "normal"
               for i in range(int(max_box_per_shipment)):
                    if (entries[i]["state"] == "readonly"):
                         entries[i]["state"] = "normal"
                    entries[i].delete(0, "end")
               for item in middle_frame.grid_slaves():
                    item["state"] = "disabled"
               submit_button["state"] = "disabled"
               current_boxes.clear()
               received_button.pack_forget()
               ship_button.pack(side = "left", padx = 10, pady = 10)
               ship_button["state"] = "disabled"
               cancel_button.grid(row = 2, column = 2, columnspan = 1, sticky = "ne")
               back_button.grid_remove()
               self.focus_set()

          def cancel():
               self.destroy()
               
          enter_button = Button(top_frame, text = "Enter", command = enter, width = 5)
          back_button = Button(top_frame, text = "Back", command = back, width = 5)
          cancel_button = Button(top_frame, text = "Cancel", command = cancel, width = 5)
        
          enter_button.grid(row = 2, column = 1, columnspan = 1, sticky = "nw")
          back_button.grid_remove()
          cancel_button.grid(row = 2, column = 2, columnspan = 1, sticky = "ne")

          cols = 4
          num_entries = max_box_per_shipment ## int(math.ceil(int(max_box_per_shipment)/cols)*cols)
          no_of_rows = math.ceil(num_entries/cols)

          shipment_content_label = Label(middle_frame, wraplength = 1000 ,fg = "red", text = f'Shipment contents will be saved under "shipping/shipmentout_YYYYMMDD_HHMMSS_modules_NNN.csv" for upload to CMSR Shipment Tracking Tool.', state = "disabled")
          instruction_label = Label(middle_frame, fg = "blue", justify = "center", anchor = "center", text = f"Enter the IDs of boxes to be packed in this shipment.", state = "disabled")
          shipment_content_label.grid(row = 0, column = 0, columnspan = int(cols * 3), pady = (0, 10), sticky = "s")
          instruction_label.grid(row = 1, column = 0, columnspan = int(cols * 3), pady = (0, 10), sticky = "ew")

          def inside_box_textbox(box_id, message):
               window = Toplevel()
               window.title("Contents of Box " + box_id)
               from tkinter import scrolledtext
               text_area = scrolledtext.ScrolledText(window, wrap = tkinter.WORD, width = 80, height = 20)
               text_area.pack(padx = 10, pady = 10, fill = tkinter.BOTH, expand = True)
               text_area.insert(END, message)
               text_area.config(state = DISABLED)
                    
          def see_inside_box(entry):
               message = ""
               box_dict = {}
               box_id = id_setter(entry.get(), "B")
               if (box_id == 1): return
               modules = get_modules_in_box_sync(encryption_key, dbshipper_pass, box_id)
               for i in range(len(modules)):
                    if (all(x == modules[i][0] for x in modules[i])):
                         module_id = modules[i][0]
                    else:
                         module_id = modules[i]
                    position = get_position_in_box_sync(encryption_key, dbshipper_pass, module_id)
                    box_dict["".join(f"{position[0]}")] = module_id
               box_dict = OrderedDict(sorted(box_dict.items()))
               for key in box_dict.keys():
                    message += "".join(f"{key}: {box_dict[key]}\n")
               inside_box_textbox(box_id, message)            

          for i in range(num_entries):
               row, col = (2 + (i % no_of_rows)), (i // no_of_rows)
               listlabel = Label(middle_frame, text = f"{i + 1}:", state = "disabled")
               listlabel.grid(row = row, column = (col * 3), padx = (10, 0), pady = 2, sticky = "w")
               entry = Entry(middle_frame, width = 20, state = "disabled")
               entry.grid(row = row, column = ((col * 3) + 1), pady = 2)
               button = Button(middle_frame, text = "See inside", command = lambda e = entry: see_inside_box(e), state = "disabled")
               button.grid(row = row, column = ((col * 3) + 2), padx = (0, 10), pady = 2, sticky = "w")
               entries.append(entry)
               
          def toggle_on():
               for i in range(len(entries)):
                    entries[i]["state"] = "normal"

          def update_db_packed():
               module_update_pack = []
               box_update_pack = [entry.get() for entry in entries if entry.get().strip() != ""]
               if (len(box_update_pack) > 0):
                    removed_boxes = [b for b in current_boxes if b not in box_update_pack]
                    added_boxes = [b for b in box_update_pack if b not in current_boxes]
                    if (len(datetime_now_var.get().strip()) == 0): datetime_now_var.set(datetime_now)
                    datetime_now_obj = datetime.strptime(datetime_now_var.get().strip(), "%Y-%m-%d %H:%M:%S")
                    if (removed_boxes):
                         for i in range(len(removed_boxes)):
                              box_id = removed_boxes[i]
                              modules = get_modules_in_box_sync(encryption_key, dbshipper_pass, box_id)
                              update_packed_timestamp_sync(encrypt_key = encryption_key, password = dbshipper_pass.strip(), module_names = modules, timestamp = None, savetofile = False)
                              update_shipment_number_sync(encrypt_key = encryption_key, password = dbshipper_pass.strip(), module_names = modules, shipment_number = None)
                    for i in range(len(added_boxes)):
                         box_id = box_update_pack[i]
                         modules = get_modules_in_box_sync(encryption_key, dbshipper_pass, box_id)
                         for j in range(len(modules)):
                              module_update_pack.append(modules[j])
                              shipment_id_db = get_shipment_number_sync(encrypt_key = encryption_key, password = dbshipper_pass, module_names = [modules[j]])
                              if ((shipment_id_entry.get() != shipment_id_db[0]) and (shipment_id_db[0] is not None)):
                                   show_error_on_top("Upload Error", "See terminal.")
                                   print(f"Upload Error: Module {modules[j]} already packaged in shipment {shipment_id_db[0]}.")
                                   return
                         update_shipment_number_sync(encrypt_key = encryption_key, password = dbshipper_pass.strip(), module_names = modules, shipment_number = shipment_id_entry.get())
                         update_packed_timestamp_sync(encrypt_key = encryption_key, password = dbshipper_pass.strip(), module_names = modules, timestamp = datetime_now_obj)
                    self.destroy()

          def ready_to_ship():
               modules_for_shipping = []
               boxes = []
               for i in range(int(max_box_per_shipment)):
                    entries[i]["state"] = "disabled"
                    boxes.append(entries[i].get())
               for i in range(len(boxes)):
                    box_id = boxes[i]
                    modules = get_modules_in_box_sync(encryption_key, dbshipper_pass, box_id)
                    for module in modules:
                         modules_for_shipping.append(module)
               upload_from_file_button["state"] = "disabled"
               submit_button["state"] = "disabled"
               ship_button.pack_forget()
               received_button.pack(side = "left", padx = 10, pady = 10)
               received_button["state"] = "normal"

               if (len(datetime_now_var.get().strip()) == 0): datetime_now_var.set(datetime_now)
               datetime_now_obj = datetime.strptime(datetime_now_var.get().strip(), "%Y-%m-%d %H:%M:%S")

               fileout_name = update_shipped_timestamp_sync(encrypt_key = encryption_key, password = dbshipper_pass.strip(), module_names = [modules_for_shipping[0]], timestamp = datetime_now_obj)
               print("List of modules saved under ", fileout_name)
               
               if fileout_name:
                   webbrowser.open(f"https://cmsr-shipment.web.cern.ch/tracking/add/")

          def received_shipment():
               boxes = []
               for i in range(int(max_box_per_shipment)):
                    entries[i]["state"] = "normal"
                    if (entries[i].get().strip() != ""): boxes.append(entries[i].get())
               submit_button["state"] = "normal"
               received_button.pack_forget()
               ship_button.pack(side = "left", padx = 10, pady = 10)
               ship_button["state"] = "normal"
               for i in range(len(boxes)):
                    box_id = boxes[i]
                    modules = get_modules_in_box_sync(encryption_key, dbshipper_pass, box_id)
                    if (not modules): continue
                    update_packed_timestamp_sync(encryption_key, dbshipper_pass, modules, timestamp = None, savetofile = False)
                    update_shipped_timestamp_sync(encryption_key, dbshipper_pass, [m for m in modules if m.strip() != ""], timestamp = None)

          button_row_frame = Frame(bottom_frame)
          button_row_frame.grid(row = (num_entries // 2) + 1, column = 0, columnspan = 5)

          submit_button = Button(button_row_frame, text = "Record to DB", command = update_db_packed, state = "disabled")
          submit_button.pack(side = "left", padx = 10, pady = 10)

          ship_button = Button(button_row_frame, text = "Ready to Ship", command = ready_to_ship, state = "disabled", width = 9)
          ship_button.pack(side = "left", padx = 10, pady = 10)

          received_button = Button(button_row_frame, text = "Received", command = received_shipment, state = "disabled", width = 9)
          received_button.pack(side = "left", padx = 10, pady = 10)
          received_button.pack_forget()
