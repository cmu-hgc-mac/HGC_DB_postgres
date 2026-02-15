
import os, subprocess, yaml, base64, sys, time, atexit, signal, re, shutil
from datetime import datetime
from cryptography.fernet import Fernet
from tkinter import Button, Checkbutton, Label, messagebox, Frame, Toplevel, Entry, IntVar, StringVar, BooleanVar, Text, LabelFrame, Radiobutton, filedialog, OptionMenu, END, DISABLED
from pathlib import Path
import pexpect

def run_ssh_master(dbloader_hostname = 'dbloader-hgcal', scp_persist = 'yes'):
    current_file = Path(__file__).resolve()
    PROJECT_ROOT = next(p for p in current_file.parents if p.name == "HGC_DB_postgres") ## Global path of HGC_DB_postgres
    
    conn_yaml_file = os.path.join(PROJECT_ROOT, os.path.join('dbase_info', 'conn.yaml'))
    conn_info  = yaml.safe_load(open(conn_yaml_file, 'r'))
    dbloader_hostname = conn_info.get('dbloader_hostname', "dbloader-hgcal") #, "hgcaldbloader.cern.ch")  
    
    sched_config_file = os.path.join(PROJECT_ROOT, os.path.join('task_scheduler', 'schedule_config.yaml'))
    sched_config  = yaml.safe_load(open(sched_config_file, 'r'))
    dbl_username = sched_config['CERN_service_account_username']

    with open(sched_config['encrypt_path'], "rb") as key_file:
        encryption_key = key_file.read()
    cipher_suite = Fernet(encryption_key)
    
    with open(sched_config['CERN_service_account_pass_path'], "rb") as f:
        encrypted_password_lxplus = f.read()
    
    service_account_password = cipher_suite.decrypt(encrypted_password_lxplus).decode()
    controlpathname = "ctrl_dbloader"
    sockpath = os.path.expanduser(f"~/.ssh/{controlpathname}")
    os.makedirs(os.path.dirname(sockpath), exist_ok=True)

    ssh_cmd = [ "ssh", "-MY",                       
                "-o", "StrictHostKeyChecking=no",
                "-o", f"ControlPath={sockpath}",
                "-o", f"ControlPersist={scp_persist}", 
                "-o", f"ProxyJump={dbl_username}@lxtunnel.cern.ch",
                f"{dbl_username}@{dbloader_hostname}" ]

    cmd_str = " ".join(ssh_cmd)

    if dbl_username and service_account_password:
        child = pexpect.spawn(cmd_str, encoding="utf-8", timeout=60)
        password_prompt_index1 = child.expect(r'.*?[Pp]assword:', timeout=60)
        if password_prompt_index1 == 0:
            child.sendline(service_account_password)
            password_prompt_index2 = child.expect(r'.*?[Pp]assword:', timeout=60)
            if password_prompt_index2 == 0:
                child.sendline(service_account_password)
                exit_status = child.expect([pexpect.EOF, pexpect.TIMEOUT])  ### This is important. Do not remove!


class SSHConfigManager:
    def __init__(self, path="~/.ssh/config"):
        self.path = os.path.expanduser(path)

    def _read(self):
        if not os.path.exists(self.path):
            return ""
        with open(self.path, "r") as f:
            return f.read()

    def _write(self, content):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "w") as f:
            f.write(content)
        os.chmod(self.path, 0o600)

    def _backup(self):
        if os.path.exists(self.path):
            shutil.copy(self.path, self.path + ".bak")

    def host_exists(self, host: str) -> bool:
        content = self._read()
        pattern = rf"(?im)^\s*Host\s+.*\b{re.escape(host)}\b"
        return re.search(pattern, content) is not None

    def ensure_host_exists(self, host: str, block: str):
        if self.host_exists(host):
            print(f"Host '{host}' already exists. Skipping.")
            return

        self._backup()
        content = self._read()
        new_content = content.rstrip() + "\n\n" + block.strip() + "\n"
        self._write(new_content)
        print(f"Host '{host}' added.")

    def remove_host(self, host: str):
        content = self._read()
        if not content:
            print("No ssh config found.")
            return

        pattern = rf"(?ims)^\s*Host\s+.*\b{re.escape(host)}\b.*?(?=^\s*Host\s+|\Z)"

        if not re.search(pattern, content):
            print(f"Host '{host}' not found.")
            return

        self._backup()
        new_content = re.sub(pattern, "", content).strip() + "\n"
        self._write(new_content)
        print(f"Host '{host}' removed.")

class JobIndicator:
    """
    When the job starts, it creates a marker file
    If the script exits normally, it deletes the file
    If you press Ctrl+C, it deletes the file
    If the process is killed (SIGTERM), it delete the file
    """
    def __init__(self, path):
        self.path = Path(path)

    def __enter__(self):
        content = f"pid={os.getpid()}\nstarted={time.ctime()}\n"  # write indicator file
        self.path.write_text(content)
        atexit.register(self.cleanup) # ensure cleanup on normal exit
        signal.signal(signal.SIGTERM, self.handle_signal)  # ensure cleanup on kill / ctrl+c
        signal.signal(signal.SIGINT, self.handle_signal)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()  # always cleanup when leaving the with-block
        return False     # return False so exceptions are not suppressed
    
    def handle_signal(self, signum, frame):
        self.cleanup()
        sys.exit(1)

    def cleanup(self):
        if self.path.exists():
            self.path.unlink()


class cron_setter():
    def __init__(self, CRON_LINE, JOB_TAG):
        self.CRON_LINE = CRON_LINE
        self.JOB_TAG = JOB_TAG
        self.upsert_cron_job()
    
    def get_current_crontab(self):
        result = subprocess.run(["crontab", "-l"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            return ""
        return result.stdout

    def install_crontab(self, cron_text):
        subprocess.run(["crontab", "-"], input=cron_text, text=True, check=True)

    def delete_cron_job(self):
        current = self.get_current_crontab()
        lines = current.splitlines()
        new_lines = [line for line in lines if self.JOB_TAG not in line]

        if len(new_lines) == len(lines):
            print("No cron job found with that tag.")
            return

        new_cron = "\n".join(new_lines).strip() + "\n"
        self.install_crontab(new_cron)
        print("Cron job deleted.")

    def upsert_cron_job(self):
        current = self.get_current_crontab()
        lines = current.splitlines()
        new_lines = []
        found = False

        for line in lines:
            if self.JOB_TAG in line:
                new_lines.append(self.CRON_LINE)
                found = True
            else:
                new_lines.append(line)

        if not found:
            new_lines.append(self.CRON_LINE)

        new_cron = "\n".join(new_lines).strip() + "\n"
        self.install_crontab(new_cron)

        if found:
            print("Cron job replaced.")
        else:
            print("Cron job created.")


class set_automation_schedule(Toplevel):
    def __init__(self, parent, encryption_key = None, title = "Set automation schedule", job_type = 'import_from_HGCAPI'):
        super().__init__(parent)
        self.title(title)
        self.encryption_key = encryption_key
        self.job_type_keys = {"Import parts from HGCAPI": 'import_from_HGCAPI', "Upload parts to CMSR": 'upload_to_CMSR'}
        self.task_scheduler_path = os.path.join(os.getcwd(), 'task_scheduler')
        self.encrypt_path = os.path.join(self.task_scheduler_path,"secret.key")
        self.config_fname = os.path.join(self.task_scheduler_path, 'schedule_config.yaml')
        self.postgres_pass_path = os.path.join(self.task_scheduler_path,"password_postgres.enc")
        self.lxplus_pass_path = os.path.join(self.task_scheduler_path,"password_lxplus.enc")
        self.config_dict = {}
        self.load_existing_config_file()
        self.selected_days_indices = []
        self.selected_days = []
        Label(self, text="See ./task_scheduler/schedule_config.yaml", fg="blue",wraplength=370,justify="left").pack(pady=1, anchor="w")
        Label(self, text="for any existing jobs.", fg="blue",wraplength=370,justify="left").pack(pady=1, anchor="w")
        Label(self, text="**Enter local DB USER password:**").pack(pady=1)
        self.shipper_var = StringVar()
        shipper_var_entry = Entry(self, textvariable=self.shipper_var, show='*', width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
        shipper_var_entry.pack(pady=0)
        Label(self, text="**Enter CERN Service Account username:**").pack(pady=1)
        self.lxuser_var = StringVar()
        lxuser_var_entry = Entry(self, textvariable=self.lxuser_var, width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
        lxuser_var_entry.pack(pady=0)
        Label(self, text="**Enter CERN Service Account password:**").pack(pady=1)
        self.cern_pass_var = StringVar()
        cern_pass_var_entry = Entry(self, textvariable=self.cern_pass_var, show='*', width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
        cern_pass_var_entry.pack(pady=0)

        Label(self, text="Select type of job").pack(pady=1)
        
        self.selected_job = StringVar(value=list(self.job_type_keys.keys())[-1])  
        dropdown_job_type = OptionMenu(self, self.selected_job, *list(self.job_type_keys.keys()))
        dropdown_job_type.pack(pady=(1,15))

        Label(self, text="Enter Time (HH:MM) in 24hr format").pack(pady=0)
        Label(self, text="(Ideally, early in the morning)").pack(pady=0)
        vcmd = (self.register(self.validate_time), '%P')
        self.time_entry = Entry(self, validate="key", validatecommand=vcmd)
        self.time_entry.pack()
        self.time_entry.insert(0, "2:00")  # default time

        Label(self, text="Repeat every X hrs in that day").pack(pady=2)
        self.repeat_hr_options = [str(i) for i in range(0,21)] ### 24 hr options
        self.selected_repeat = StringVar(value=self.repeat_hr_options[6])
        self.repeat_dropdown = OptionMenu(self, self.selected_repeat, *self.repeat_hr_options)
        self.repeat_dropdown.pack(pady=2)

        Label(self, text="Select days of week to repeat weekly").pack(pady=10)
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"] ## cron index 0 starts on Sunday; datetime index 0 starts on Monday
        self.day_vars = {}
        days_frame = Frame(self)
        days_frame.pack()

        for day in days:
            if day in ["Sunday"]:
                var = BooleanVar(value=False)
            else:
                var = BooleanVar(value=True)
            chk = Checkbutton(days_frame, text=day, variable=var)
            chk.pack(anchor="w")
            self.day_vars[day] = var

        Button(self, text="Save Schedule", command=self.get_schedule).pack(pady=15)
        # Button(self, text="Delete Schedule", command=self.get_schedule).pack(pady=15)


    def load_existing_config_file(self):                
        path = Path(self.config_fname)
        if path.exists():
            with open(path, "r") as f:
                self.config_dict = yaml.safe_load(f)
                if self.config_dict is None:
                    self.config_dict = {}
        else:
            self.config_dict = {}

    def validate_time(self, new_value):
        """Allow typing partial valid time like '1', '12:', '12:3', etc."""
        if new_value == "":
            return True
        if len(new_value) > 5:
            return False
        if not all(c.isdigit() or c==":" for c in new_value):      # Only allow digits and one colon
            return False
        if new_value.count(":") > 1:    # Only one colon allowed
            return False
        parts = new_value.split(":")   # Split by colon if present
        if len(parts) >= 1 and parts[0]:
            if not parts[0].isdigit() or int(parts[0]) > 23:
                return False
        if len(parts) == 2 and parts[1]:
            if not parts[1].isdigit() or int(parts[1]) > 59:
                return False
        if parts[0] and (0 <= int(parts[0]) <= 23):
            max_intervals = 24 - int(parts[0])
            self.repeat_hr_options = list(range(0, max_intervals)) if int(parts[0]) < 12 else [0]
            try:
                self.selected_repeat.set(self.repeat_hr_options[-1])
                menu = self.repeat_dropdown["menu"]
                menu.delete(0, "end")  # clear old options
                for option in self.repeat_hr_options:
                    menu.add_command(label=option, command=lambda value=option: self.selected_repeat.set(value))
            except:
                None
        return True
    

    def get_schedule(self):
        time = self.time_entry.get()
        self.selected_days = [day for day, var in self.day_vars.items() if var.get()]
        self.selected_days_indices = [str(1+self.selected_days.index(day)) for day in self.selected_days] ## cron index 0 starts on Sunday; datetime index 0 starts on Monday

        try:   # Validate final time format HH:MM
            hour, minute = map(int, time.split(":"))
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError
        except:
            self.result_label.config(text="Invalid time! Use HH:MM format (00:00 to 23:59).")
            return

        if not self.selected_days:
            self.result_label.config(text="Please select at least one day.")
            return

        self.save_encrypted_password()
        self.create_cron_schedule_config()
        ### if 'upload' in self.selected_job.get().lower():  self.create_ssh_config_entry()  ### Skip this
        self.destroy() 
        # self.result_label.config(text=f"Weekly on: {days_str} at {time}")

    def save_encrypted_password(self):
        cipher_suite = Fernet(self.encryption_key) ## Generate or load a key. 
        encrypted_postgres_password = cipher_suite.encrypt(self.shipper_var.get().encode())
        encrypted_lxplus_password = cipher_suite.encrypt(self.cern_pass_var.get().encode())
        with open(self.encrypt_path, "wb") as key_file:
            key_file.write(self.encryption_key)
        with open(self.postgres_pass_path, "wb") as f:
            f.write(encrypted_postgres_password)
        with open(self.lxplus_pass_path, "wb") as f:
            f.write(encrypted_lxplus_password)

    def create_cron_schedule_config(self):
        type_of_job = self.job_type_keys[self.selected_job.get()]
        self.config_dict[type_of_job] = {}
        self.config_dict[type_of_job]['cron_job_name'] = f"{type_of_job}_job"
        self.config_dict[type_of_job]['schedule_time'] = self.time_entry.get()
        self.config_dict[type_of_job]['schedule_days'] = ",".join(self.selected_days_indices)
        self.config_dict['python_path'] = sys.executable
        self.config_dict['HGC_DB_postgres_path'] = os.getcwd() # Path of HGC_DB_postgres folder
        self.config_dict['CERN_service_account_username'] = self.lxuser_var.get()
        self.config_dict['CERN_service_account_pass_path'] = self.lxplus_pass_path
        self.config_dict['postgres_shipper_pass_path'] = self.postgres_pass_path
        self.config_dict['encrypt_path'] = self.encrypt_path
        self.config_dict['postgres_username'] = 'shipper'
        
        py_job_fname = os.path.join(self.task_scheduler_path, 'run_as_scheduled.py')
        py_log_fname = os.path.join(self.task_scheduler_path, f'schedule_job_{type_of_job}.log')
    
        hr_time, min_time = self.config_dict[type_of_job]['schedule_time'].split(':')

        if type_of_job == 'import_from_HGCAPI':
            writeout_type = '>'
        elif type_of_job == 'upload_to_CMSR':
            writeout_type = '>>'

        cron_time = f"{hr_time}-23/{self.selected_repeat.get()}" if self.selected_repeat.get() != '0' else hr_time
        cron_command_inputs = [str(int(min_time)), cron_time, '*', '*', self.config_dict[type_of_job]['schedule_days'],
                                self.config_dict['python_path'], py_job_fname, 
                                '-jt', type_of_job, writeout_type, py_log_fname, '2>&1', ## both stderr and stdout appended
                                '#', self.config_dict[type_of_job]['cron_job_name']]
    
            
        self.config_dict[type_of_job]['cron_command'] = " ".join(cron_command_inputs)

        days_str = ", ".join(self.selected_days)
        self.config_dict[type_of_job]['description'] = f"Run {type_of_job} weekly on: {days_str} at {self.config_dict[type_of_job]['schedule_time']} repeating every {self.selected_repeat.get()} hour(s) in localtime."

        cron_setter(CRON_LINE=self.config_dict[type_of_job]['cron_command'], JOB_TAG=self.config_dict[type_of_job]['cron_job_name'])
        
        with open(self.config_fname, 'w') as outfile:
            yaml.dump(self.config_dict, outfile, sort_keys=False) # sort_keys=False preserves original order

        """
        ### Example below of cron job
        30 2 * * 1,2,3,4,5 /full/path/to/python3 /full/path/to/HGC_DB_postgres/task_scheduler/run_as_scheduled.py -jt upload_to_CMSR >> /full/path/to/HGC_DB_postgres/task_scheduler/schedule_job.log 2>&1 # CMSR_UPLOAD_SCHEDULE_JOB
        """
        
    def create_ssh_config_entry(self):
        conn_info = yaml.safe_load(open( os.path.join('dbase_info', 'conn.yaml') , 'r'))
        dbloader_hostname = conn_info.get('dbloader_hostname', "dbloader-hgcal")
        manager = SSHConfigManager()
        HOST_BLOCK = f"""
        Host dbloader
            HostName {dbloader_hostname}
            User {self.config_dict['CERN_service_account_username']}
            ProxyJump {self.config_dict['CERN_service_account_username']}@lxtunnel.cern.ch
            ControlMaster auto
            ControlPath ~/.ssh/ctrl_dbloader
            ControlPersist yes
            StrictHostKeyChecking no
            ForwardX11Trusted yes
        """
        manager.ensure_host_exists("dbloader", HOST_BLOCK)

