
import os, subprocess, yaml, base64, sys, time, atexit, signal, re, shutil
from datetime import datetime
from cryptography.fernet import Fernet
from tkinter import Button, Checkbutton, Label, messagebox, Frame, Toplevel, Entry, IntVar, StringVar, BooleanVar, Text, LabelFrame, Radiobutton, filedialog, OptionMenu, END, DISABLED
from pathlib import Path

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
    def __init__(self, path):
        self.path = Path(path)

    def __enter__(self):
        content = f"pid={os.getpid()}\nstarted={time.ctime()}\n"  # write indicator file
        self.path.write_text(content)
        atexit.register(self.cleanup) # ensure cleanup on normal exit
        signal.signal(signal.SIGTERM, self.handle_signal)  # ensure cleanup on kill / ctrl+c
        signal.signal(signal.SIGINT, self.handle_signal)
        return self

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
    def __init__(self, parent): #, encryption_key):
        super().__init__(parent)
        self.title("Set automation schedule")
        self.config_dict = {}
        self.task_scheduler_path = os.path.join(os.getcwd(), 'task_scheduler')
        self.encrypt_path = os.path.join(self.task_scheduler_path,"secret.key")
        self.postgres_pass_path = os.path.join(self.task_scheduler_path,"password_postgres.enc")
        self.lxplus_pass_path = os.path.join(self.task_scheduler_path,"password_lxplus.enc")
        self.selected_days_indices = []
        self.selected_days = []
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


        self.import_parts_var = BooleanVar(value=True)
        import_parts_var_entry = Checkbutton(self, text="Import parts from CMSR", variable=self.import_parts_var)
        import_parts_var_entry.pack(pady=0)
        self.upload_parts_var = BooleanVar(value=True)
        upload_parts_var_entry = Checkbutton(self, text="Upload parts to CMSR", variable=self.upload_parts_var)
        upload_parts_var_entry.pack(pady=0)


        Label(self, text="Enter Time (HH:MM) in 24hr format").pack(pady=5)
        vcmd = (self.register(self.validate_time), '%P')
        self.time_entry = Entry(self, validate="key", validatecommand=vcmd)
        self.time_entry.pack()
        self.time_entry.insert(0, "2:00")  # default time

        Label(self, text="Select days of week to repeat weekly").pack(pady=10)
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
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
        return True
    

    def get_schedule(self):
        time = self.time_entry.get()
        self.selected_days = [day for day, var in self.day_vars.items() if var.get()]
        self.selected_days_indices = [str(1+self.selected_days.index(day)) for day in self.selected_days]

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

        days_str = ", ".join(self.selected_days)
        self.save_encrypted_password()
        self.create_cron_schedule_config()
        self.create_ssh_config_entry()
        print(f"Weekly on: {days_str} at {time} in localtime.")
        self.destroy() 
        # self.result_label.config(text=f"Weekly on: {days_str} at {time}")

    def save_encrypted_password(self):
        encryption_key = Fernet.generate_key()
        cipher_suite = Fernet(encryption_key) ## Generate or load a key. 
        encrypted_postgres_password = cipher_suite.encrypt(self.shipper_var.get().encode())
        encrypted_lxplus_password = cipher_suite.encrypt(self.cern_pass_var.get().encode())
        with open(self.encrypt_path, "wb") as key_file:
            key_file.write(encryption_key)
        with open(self.postgres_pass_path, "wb") as f:
            f.write(encrypted_postgres_password)
        with open(self.lxplus_pass_path, "wb") as f:
            f.write(encrypted_lxplus_password)

    def create_cron_schedule_config(self):
        self.config_dict['cron_job_name'] = "HGC_DB_SCHEDULE_JOB"
        self.config_dict['schedule_time'] = self.time_entry.get()
        self.config_dict['schedule_days'] = ",".join(self.selected_days_indices)
        self.config_dict['python_path'] = sys.executable
        self.config_dict['HGC_DB_postgres_path'] = os.getcwd() # Path of HGC_DB_postgres folder
        self.config_dict['CERN_service_account_username'] = self.lxuser_var.get()
        self.config_dict['CERN_service_account_pass_path'] = self.lxplus_pass_path
        self.config_dict['postgres_shipper_pass_path'] = self.postgres_pass_path
        self.config_dict['encrypt_path'] = self.encrypt_path
        self.config_dict['postgres_username'] = 'shipper'
        self.config_dict['import_from_HGCAPI'] = self.import_parts_var.get()
        self.config_dict['upload_to_CMSR'] = self.upload_parts_var.get()
        
        py_job_fname = os.path.join(self.task_scheduler_path, 'run_as_scheduled.py')
        py_log_fname = os.path.join(self.task_scheduler_path, 'schedule_job.log')
        config_fname = os.path.join(self.task_scheduler_path, 'schedule_config.yaml')
        
        hr_time, min_time = self.config_dict['schedule_time'].split(':')
        cron_command_inputs = [str(int(min_time)), str(int(hr_time)), '*', '*', self.config_dict['schedule_days'],
                                self.config_dict['python_path'], py_job_fname, '>>', py_log_fname, '2>&1', ## both stderr and stdout appended
                                '#', self.config_dict['cron_job_name']]
        
        self.config_dict['cron_command'] = " ".join(cron_command_inputs)        
        cron_setter(CRON_LINE=self.config_dict['cron_command'], JOB_TAG=self.config_dict['cron_job_name'])

        with open(config_fname, 'w') as outfile:
            yaml.dump(self.config_dict, outfile, sort_keys=False) # sort_keys=False preserves original order

        """
        ### Example below of cron job
        30 2 * * 1,2,3,4,5 /full/path/to/HGC_DB_postgres/task_scheduler/run_as_scheduled.py >> /full/path/to/HGC_DB_postgres/task_scheduler/schedule_job.log 2>&1 # HGC_DB_SCHEDULE_JOB
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

