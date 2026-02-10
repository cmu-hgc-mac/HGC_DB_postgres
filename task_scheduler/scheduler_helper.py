
import os, subprocess, yaml, base64, sys
from datetime import datetime
from tkinter import Button, Checkbutton, Label, messagebox, Frame, Toplevel, Entry, IntVar, StringVar, BooleanVar, Text, LabelFrame, Radiobutton, filedialog, OptionMenu, END, DISABLED
import keyring as kr
from pathlib import Path

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
            if day in ["Saturday", "Sunday"]:
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
        self.create_cron_schedule_config()
        print(f"Weekly on: {days_str} at {time} in localtime.")
        self.destroy() 
        # self.result_label.config(text=f"Weekly on: {days_str} at {time}")

    def create_cron_schedule_config(self):
        config_dict = {}
        config_dict['cron_job_name'] = "HGC_DB_SCHEDULE_JOB"
        config_dict['schedule_time'] = self.time_entry.get()
        config_dict['schedule_days'] = ",".join(self.selected_days_indices)
        config_dict['python_path'] = sys.executable
        config_dict['HGC_DB_postgres_path'] = os.getcwd() # Path of HGC_DB_postgres folder
        config_dict['CERN_service_account_username'] = self.lxuser_var.get()
        config_dict['postgres_username'] = 'shipper'
        config_dict['import_from_HGCAPI'] = self.import_parts_var.get()
        config_dict['upload_to_CMSR'] = self.upload_parts_var.get()
        kr.set_password("POSTGRES", config_dict['postgres_username'],             self.shipper_var.get())
        # kr.set_password("LXPLUS",   config_dict['CERN_service_account_username'], self.cern_pass_var.get())
        
        py_job_fname = os.path.join(os.path.join(config_dict['HGC_DB_postgres_path'], 'task_scheduler'), 'run_as_scheduled.py')
        py_log_fname = os.path.join(os.path.join(config_dict['HGC_DB_postgres_path'], 'task_scheduler'), 'schedule_job.log')
        config_fname = os.path.join(os.path.join(config_dict['HGC_DB_postgres_path'], 'task_scheduler'), 'schedule_config.yaml')
        
        hr_time, min_time = config_dict['schedule_time'].split(':')
        cron_command_inputs = [min_time, hr_time, '*', '*', config_dict['schedule_days'],
                                config_dict['python_path'], py_job_fname,
                                py_log_fname, '2>&1', ## both stderr and stdout appended
                                '#', config_dict['cron_job_name']]
        
        config_dict['cron_command'] = " ".join(cron_command_inputs)        
        cron_setter(CRON_LINE=config_dict['cron_command'], JOB_TAG=config_dict['cron_job_name'])

        with open(config_fname, 'w') as outfile:
            yaml.dump(config_dict, outfile, sort_keys=False) # sort_keys=False preserves original order

        """
        ### Example below of cron job
        30 2 * * 1,2,3,4,5 /full/path/to/HGC_DB_postgres/task_scheduler/run_as_scheduled.py >> /full/path/to/HGC_DB_postgres/task_scheduler/schedule_job.log 2>&1 # HGC_DB_SCHEDULE_JOB
        """
        

