
import os, subprocess, yaml, base64, sys, time, atexit, signal, re, shutil, asyncio
from datetime import datetime
from cryptography.fernet import Fernet
from tkinter import Button, Checkbutton, Label, messagebox, Frame, Toplevel, Entry, IntVar, StringVar, BooleanVar, Text, LabelFrame, Radiobutton, filedialog, OptionMenu, END, DISABLED
from pathlib import Path
import pexpect, paramiko

def get_lxplus_username_password():
    current_file = Path(__file__).resolve()
    PROJECT_ROOT = next(p for p in current_file.parents if p.name == "HGC_DB_postgres") ## Global path of HGC_DB_postgres
    # conn_yaml_file = os.path.join(PROJECT_ROOT, os.path.join('dbase_info', 'conn.yaml'))
    # conn_info  = yaml.safe_load(open(conn_yaml_file, 'r'))
    # dbloader_hostname = conn_info.get('dbloader_hostname', "dbloader-hgcal.cern.ch") #, "hgcaldbloader.cern.ch")  
    sched_config_file = os.path.join(PROJECT_ROOT, os.path.join('task_scheduler', 'schedule_config.yaml'))
    sched_config  = yaml.safe_load(open(sched_config_file, 'r'))
    dbl_username = sched_config['CERN_service_account_username']
    with open(sched_config['encrypt_path'], "rb") as key_file:
        encryption_key = key_file.read()
    cipher_suite = Fernet(encryption_key)
    with open(sched_config['CERN_service_account_pass_path'], "rb") as f:
        encrypted_password_lxplus = f.read()
    
    service_account_password = cipher_suite.decrypt(encrypted_password_lxplus).decode()
    return dbl_username, service_account_password


def run_ssh_master(dbloader_hostname = 'dbloader-hgcal.cern.ch', scp_persist = 'yes'):
    dbl_username, service_account_password = get_lxplus_username_password()
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
        self.job_type_keys = {"Import parts from HGCAPI": 'import_from_HGCAPI'}
        self.task_scheduler_path = os.path.join(os.getcwd(), 'task_scheduler')
        self.encrypt_path = os.path.join(self.task_scheduler_path,"secret.key")
        self.config_fname = os.path.join(self.task_scheduler_path, 'schedule_config.yaml')
        self.xml_source_yaml = os.path.join(os.getcwd(), 'export_data', 'list_of_xmls.yaml')
        self.xml_auto_yaml   = os.path.join(self.task_scheduler_path, 'list_of_xmls_auto.yaml')
        self.postgres_pass_path = os.path.join(self.task_scheduler_path,"password_postgres.enc")
        self.lxplus_pass_path = os.path.join(self.task_scheduler_path,"password_lxplus.enc")
        self.config_dict = {}
        self.load_existing_config_file()
        self.selected_days_indices = []
        self.selected_days = []
        self.selected_job = StringVar()
        self.job_panel = None   # left column job-schedule panel
        self._side_panel = None  # right column side panel
        self._upload_date_range = StringVar(value='since_last_upload')

        # Two-column layout: left holds credentials + buttons + job panel;
        # right holds the XML / parts side panel (starts at same height as credentials)
        self.columns_frame = Frame(self)
        self.columns_frame.pack(fill="both", expand=True, padx=5, pady=(2, 0))
        self.left_col  = Frame(self.columns_frame)
        self.left_col.pack(side="left", fill="y", anchor="n")
        self.right_col = Frame(self.columns_frame)
        self.right_col.pack(side="left", fill="both", expand=True, anchor="n")

        # Credentials go into left_col
        # Label(self.left_col, text="See ./task_scheduler/schedule_config.yaml", fg="blue", wraplength=280, justify="left").pack(pady=1, anchor="w")
        # Label(self.left_col, text="for any existing jobs.", fg="blue", wraplength=280, justify="left").pack(pady=1, anchor="w")
        Label(self.left_col, text="**Enter local DB USER password:**").pack(pady=1)
        self.shipper_var = StringVar()
        _row1 = Frame(self.left_col); _row1.pack(pady=0)
        shipper_var_entry = Entry(_row1, textvariable=self.shipper_var, show='*', width=27, bd=1.5, highlightbackground="black", highlightthickness=1)
        shipper_var_entry.pack(side="left")
        _peek1 = Button(_row1, text="see", padx=2, pady=0, takefocus=0)
        _peek1.pack(side="left", padx=(2, 0))
        _peek1.bind("<ButtonPress-1>",   lambda _: shipper_var_entry.config(show=''))
        _peek1.bind("<ButtonRelease-1>", lambda _: shipper_var_entry.config(show='*'))

        Label(self.left_col, text="**Enter CERN Service Account username:**").pack(pady=1)
        self.lxuser_var = StringVar()
        lxuser_var_entry = Entry(self.left_col, textvariable=self.lxuser_var, width=30, bd=1.5, highlightbackground="black", highlightthickness=1)
        lxuser_var_entry.pack(pady=0)

        Label(self.left_col, text="**Enter CERN Service Account password:**").pack(pady=1)
        self.cern_pass_var = StringVar()
        _row2 = Frame(self.left_col); _row2.pack(pady=0)
        cern_pass_var_entry = Entry(_row2, textvariable=self.cern_pass_var, show='*', width=27, bd=1.5, highlightbackground="black", highlightthickness=1)
        cern_pass_var_entry.pack(side="left")
        _peek2 = Button(_row2, text="see", padx=2, pady=0, takefocus=0)
        _peek2.pack(side="left", padx=(2, 0))
        _peek2.bind("<ButtonPress-1>",   lambda _: cern_pass_var_entry.config(show=''))
        _peek2.bind("<ButtonRelease-1>", lambda _: cern_pass_var_entry.config(show='*'))

        buttons_frame = Frame(self.left_col)
        buttons_frame.pack(pady=(5, 0))

        for label, key in self.job_type_keys.items():
            short = label.split()[0]  # "Import" or "Upload"
            btn = Button(buttons_frame, text=f"View {short} job",
                         command=lambda k=key, l=label: self._toggle_job_panel(k, l))
            btn.pack(side="left", padx=5)


    def load_existing_config_file(self):                
        path = Path(self.config_fname)
        if path.exists():
            with open(path, "r") as f:
                self.config_dict = yaml.safe_load(f)
                if self.config_dict is None:
                    self.config_dict = {}
        else:
            self.config_dict = {}

    def _load_existing_job_values(self, job_key):
        """Return (time_str, repeat_val, active_day_indices) from yaml/crontab, or None if not found."""
        job_cfg = self.config_dict.get(job_key)
        if job_cfg:
            time_str  = job_cfg.get('schedule_time', '2:00')
            days_str  = job_cfg.get('schedule_days', '')   # e.g. "1,2,3,4,5,6"
            # recover repeat from cron_command hour field: "2" or "2-23/6"
            repeat_val = 'Do not repeat'
            cron_cmd = job_cfg.get('cron_command', '')
            if cron_cmd:
                parts = cron_cmd.split()
                if len(parts) >= 2:
                    hr_field = parts[1]   # e.g. "2" or "2-23/6"
                    if '/' in hr_field:
                        repeat_val = hr_field.split('/')[1]  # "6"
            active_indices = set(days_str.split(',')) if days_str else set()
            return time_str, repeat_val, active_indices
        # fallback: check crontab directly
        job_name = f"{job_key}_job"
        result = subprocess.run(["crontab", "-l"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        for line in result.stdout.splitlines():
            if job_name in line and not line.strip().startswith('#'):
                parts = line.split()
                if len(parts) >= 2:
                    min_f, hr_f = parts[0], parts[1]
                    hr_start = hr_f.split('-')[0].split('/')[0]
                    time_str = f"{hr_start}:{min_f.zfill(2)}"
                    repeat_val = hr_f.split('/')[1] if '/' in hr_f else 'Do not repeat'
                    days_f = parts[4] if len(parts) >= 5 else ''
                    active_indices = set(days_f.split(',')) if days_f and days_f != '*' else set()
                    return time_str, repeat_val, active_indices
        return None

    def _toggle_job_panel(self, job_key, job_label):
        """Show the schedule sub-panel for the chosen job, collapsing any other open one."""
        # collapse existing panels
        if self._side_panel is not None:
            self._side_panel.destroy()
            self._side_panel = None
        if self.job_panel is not None:
            self.job_panel.destroy()
            self.job_panel = None
            if self.selected_job.get() == job_key:
                self.selected_job.set("")
                return  # clicking same button again just closes it

        self.selected_job.set(job_key)
        panel = Frame(self.left_col, relief="groove", bd=1)
        panel.pack(fill="x", padx=0, pady=(5, 0))
        self.job_panel = panel

        if job_key == 'upload_to_CMSR' and not os.path.exists(self.xml_auto_yaml):
            shutil.copy(self.xml_source_yaml, self.xml_auto_yaml)

        existing = self._load_existing_job_values(job_key)
        default_time   = existing[0] if existing else "2:00"
        default_repeat = existing[1] if existing else "Do not repeat"
        active_indices = existing[2] if existing else set()

        status_text = "(existing schedule loaded)" if existing else "(no existing schedule)"
        Label(panel, text=f"--- {job_label} schedule --- {status_text}", fg="blue").pack(pady=(4, 0))

        # Label(panel, text="(Ideally, early in the morning)").pack(pady=0)
        time_row = Frame(panel)
        time_row.pack(pady=0)
        Label(time_row, text="Enter Start Time (HH:MM) in 24hr format:").pack(side="left")
        vcmd = (self.register(self.validate_time_panel), '%P')
        time_entry = Entry(time_row, validate="key", validatecommand=vcmd, width=7, bd=1.5, highlightbackground="black", highlightthickness=1)
        time_entry.pack(side="left", padx=(4, 0))
        time_entry.insert(0, default_time)

        repeat_row = Frame(panel)
        repeat_row.pack(pady=2)
        Label(repeat_row, text="Repeat every X hour(s) till end of day").pack(side="left")
        repeat_hr_options = ['Do not repeat'] + [str(i) for i in range(1, 21)]
        selected_repeat = StringVar(value=str(default_repeat))
        repeat_dropdown = OptionMenu(repeat_row, selected_repeat, *repeat_hr_options)
        repeat_dropdown.pack(side="left", padx=(4, 0))

        # store refs so validate_time_panel can update the dropdown
        panel._repeat_hr_options = repeat_hr_options
        panel._selected_repeat = selected_repeat
        panel._repeat_dropdown = repeat_dropdown
        panel._time_entry = time_entry
        self._active_panel = panel  # used by validate_time_panel

        Label(panel, text="Select days of week to repeat weekly").pack(pady=(8, 0))
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        day_vars = {}
        days_frame = Frame(panel)
        days_frame.pack()
        for i, day in enumerate(days):
            if active_indices:
                checked = str(i + 1) in active_indices  # cron day index 1=Monday … 7=Sunday
            else:
                checked = (day != "Sunday")
            var = BooleanVar(value=checked)
            chk = Checkbutton(days_frame, text=day, variable=var)
            chk.pack(anchor="w")
            day_vars[day] = var

        result_label = Label(panel, text="", fg="red")
        result_label.pack(pady=2)

        if job_key == 'upload_to_CMSR':
            # Date range radio buttons
            saved_range = self.config_dict.get('upload_to_CMSR', {}).get('upload_date_range', 'since_last_upload')
            self._upload_date_range.set(saved_range)
            Label(panel, text="Upload XMLs generated:").pack(pady=(2, 0))
            radio_frame = Frame(panel)
            radio_frame.pack(pady=(2, 0))
            for label, val in [("Since last upload", "since_last_upload"),
                                ("For this week",     "this_week"),
                                ("For all time",      "all_time")]:
                Radiobutton(radio_frame, text=label, variable=self._upload_date_range, value=val).pack(side="left", padx=4)

            Button(panel, text="Select type of XMLs", command=lambda: self._toggle_side_panel('xml')).pack(pady=(10, 0))

        elif job_key == 'import_from_HGCAPI':
            Button(panel, text="Select type of parts",
                   command=lambda: self._toggle_side_panel('parts')).pack(pady=(2, 0))

        btns_frame = Frame(panel)
        btns_frame.pack(pady=(5, 8))
        Button(btns_frame, text="Save Schedule",
               command=lambda: self._save_schedule(job_key, time_entry, selected_repeat, day_vars, result_label)).pack(side="left", padx=5)
        Button(btns_frame, text="Delete Schedule", fg="red",
               command=lambda: self._delete_schedule(job_key)).pack(side="left", padx=5)

    # ------------------------------------------------------------------ #
    #  Side-panel helpers                                                  #
    # ------------------------------------------------------------------ #

    def _toggle_side_panel(self, panel_type):
        if self._side_panel is not None:
            self._side_panel.destroy()
            self._side_panel = None
        side = Frame(self.right_col, relief="groove", bd=1)
        side.pack(fill="both", expand=True, pady=0)
        self._side_panel = side
        if panel_type == 'xml':
            self._build_xml_side_panel(side)
        else:
            self._build_parts_side_panel(side)

    def _load_xml_yaml(self):
        """Return the xml list from list_of_xmls_auto.yaml."""
        with open(self.xml_auto_yaml, 'r') as f:
            return yaml.safe_load(f)

    def _build_xml_side_panel(self, parent):
        Label(parent, text="Select XMLs to upload", fg="blue").pack(pady=(4, 0))
        xml_list = self._load_xml_yaml()
        checkbox_vars = {}

        def create_checkboxes(data, container):
            if isinstance(data, dict):
                for key, value in data.items():
                    frame = LabelFrame(container, text=key, padx=5, pady=0.5)
                    frame.pack(fill="x", expand=True, padx=10, pady=0.5)
                    checkbox_vars[key] = create_checkboxes(value, frame)
            elif isinstance(data, list):
                vars_list = []
                for item in data:
                    for key, value in item.items():
                        var = IntVar(value=1 if value else 0)
                        Checkbutton(container, text=key, variable=var).pack(anchor="w", padx=10, pady=0.5)
                        vars_list.append({key: var})
                return vars_list
            return {}

        create_checkboxes(xml_list, parent)
        toggle_state = {"all_selected": True}

        def toggle_all():
            toggle_state["all_selected"] = not toggle_state["all_selected"]
            new_state = 1 if toggle_state["all_selected"] else 0
            def apply_toggle(data):
                if isinstance(data, list):
                    for item in data:
                        for _, var in item.items():
                            var.set(new_state)
                elif isinstance(data, dict):
                    for v in data.values():
                        apply_toggle(v)
            apply_toggle(checkbox_vars)

        Button(parent, text="(De)Select All", command=toggle_all).pack(pady=2)

        def save_xml_selection():
            def collect(data, cvars):
                if isinstance(data, dict):
                    return {k: collect(v, cvars[k]) for k, v in data.items()}
                elif isinstance(data, list):
                    return [{k: (cvars[i][k].get() == 1) for k in item.keys()} for i, item in enumerate(data)]
                return data
            updated = collect(xml_list, checkbox_vars)
            with open(self.xml_auto_yaml, 'w') as f:
                yaml.dump(updated, f, sort_keys=False)
            self._side_panel.destroy()
            self._side_panel = None
            # messagebox.showinfo("Saved", f"XML selection saved to\n{self.xml_auto_yaml}")

        Button(parent, text="Save selection", command=save_xml_selection).pack(pady=(4, 8))

    def _build_parts_side_panel(self, parent):
        Frame(parent, height=340).pack()  # spacer to align with time entry on the left
        Label(parent, text="Select parts to import", fg="blue").pack(pady=(4, 0))
        # load existing choices from config if present
        saved = self.config_dict.get('import_from_HGCAPI', {})
        part_keys = [("baseplates",      "getbp"),
                     ("hexaboards",      "gethxb"),
                     ("sensors",         "getsen"),
                     ("other inventory", "getmmtsinv")]
        part_vars = {}
        for label, flag in part_keys:
            var = BooleanVar(value=saved.get(flag, True))
            Checkbutton(parent, text=label, variable=var).pack(anchor="w", padx=15, pady=1)
            part_vars[flag] = var

        def save_parts_selection():
            if 'import_from_HGCAPI' not in self.config_dict:
                self.config_dict['import_from_HGCAPI'] = {}
            for flag, var in part_vars.items():
                self.config_dict['import_from_HGCAPI'][flag] = var.get()
            with open(self.config_fname, 'w') as f:
                yaml.dump(self.config_dict, f, sort_keys=False)
            self._side_panel.destroy()
            self._side_panel = None
            # messagebox.showinfo("Saved", "Part-type selection saved.")

        Button(parent, text="Save selection", command=save_parts_selection).pack(pady=(4, 8))

    def validate_time_panel(self, new_value):
        """Validate time entry and update the repeat dropdown on the active panel."""
        panel = getattr(self, '_active_panel', None)
        if new_value == "":
            return True
        if len(new_value) > 5:
            return False
        if not all(c.isdigit() or c == ":" for c in new_value):
            return False
        if new_value.count(":") > 1:
            return False
        parts = new_value.split(":")
        if len(parts) >= 1 and parts[0]:
            if not parts[0].isdigit() or int(parts[0]) > 23:
                return False
        if len(parts) == 2 and parts[1]:
            if not parts[1].isdigit() or int(parts[1]) > 59:
                return False
        if panel and parts[0] and (0 <= int(parts[0]) <= 23):
            max_intervals = 24 - int(parts[0])
            new_options = ['Do not repeat'] + list(range(1, max_intervals))
            try:
                current = panel._selected_repeat.get()
                # Only reset selection if current value is no longer in the new options
                current_in_new = (current == 'Do not repeat') or (current.isdigit() and int(current) in range(1, max_intervals))
                if not current_in_new:
                    panel._selected_repeat.set(new_options[-1])
                menu = panel._repeat_dropdown["menu"]
                menu.delete(0, "end")
                for opt in new_options:
                    menu.add_command(label=opt, command=lambda v=opt: panel._selected_repeat.set(v))
            except Exception:
                pass
        return True

    def _save_schedule(self, job_key, time_entry, selected_repeat, day_vars, result_label):
        time_val = time_entry.get()
        selected_days = [day for day, var in day_vars.items() if var.get()]
        self.selected_days_indices = [str(1 + list(day_vars.keys()).index(day)) for day in selected_days]
        self.selected_days = selected_days

        # store so create_cron_schedule_config can read them
        self._current_time_entry = time_entry
        self._current_selected_repeat = selected_repeat

        try:
            hour, minute = map(int, time_val.split(":"))
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError
        except Exception:
            result_label.config(text="Invalid time! Use HH:MM format (00:00 to 23:59).")
            return

        if not selected_days:
            result_label.config(text="Please select at least one day.")
            return

        # resolve key → display label for create_cron_schedule_config
        reverse = {v: k for k, v in self.job_type_keys.items()}
        self.selected_job.set(reverse[job_key])

        # Validate DB password
        from export_data.src import check_good_conn
        if not asyncio.run(check_good_conn(self.shipper_var.get().strip(), user_type='editor')):
            messagebox.showerror("Password Error", "Database password is incorrect. Please update and try again.")
            return

        # Validate LXPLUS password only if an upload job exists or this is an upload job
        upload_job_exists = job_key == 'upload_to_CMSR' or 'upload_to_CMSR' in self.config_dict
        if upload_job_exists:
            try:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect('lxplus.cern.ch', username=self.lxuser_var.get().strip(), password=self.cern_pass_var.get().strip(), timeout=15)
                ssh.close()
            except Exception as e:
                print(e)
                messagebox.showerror("Password Error", "LXPLUS authentication failed. Please check your CERN username and password.")
                return

        self.save_encrypted_password()
        self.create_cron_schedule_config()
        self.job_panel.destroy()
        self.job_panel = None
        self.selected_job.set("")
        messagebox.showinfo("Schedule Saved", f"{reverse[job_key]} schedule saved.\nCheck ./task_scheduler/schedule_config.yaml.")
        self.lift()
        self.focus_force()

    def _delete_schedule(self, job_key):
        job_name = f"{job_key}_job"
        cs = object.__new__(cron_setter)
        cs.CRON_LINE = ""
        cs.JOB_TAG = job_name
        cs.delete_cron_job()
        # remove from yaml config
        if job_key in self.config_dict:
            del self.config_dict[job_key]
            with open(self.config_fname, 'w') as f:
                yaml.dump(self.config_dict, f, sort_keys=False)
        self.job_panel.destroy()
        self.job_panel = None
        self.selected_job.set("")
        messagebox.showinfo("Schedule Deleted", f"{job_key} cron job removed.")
        self.lift()
        self.focus_force()

    # keep old name as alias so nothing external breaks
    def get_schedule(self):
        pass

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
        self.config_dict[type_of_job]['schedule_time'] = self._current_time_entry.get()
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

        repeat_val = self._current_selected_repeat.get()
        cron_time = f"{hr_time}-23/{repeat_val}" if str(repeat_val).isdigit() else hr_time
        cron_command_inputs = [str(int(min_time)), cron_time, '*', '*', self.config_dict[type_of_job]['schedule_days'],
                                self.config_dict['python_path'], py_job_fname,
                                '-jt', type_of_job, writeout_type, py_log_fname, '2>&1', ## both stderr and stdout appended
                                '#', self.config_dict[type_of_job]['cron_job_name']]


        self.config_dict[type_of_job]['cron_command'] = " ".join(cron_command_inputs)

        if type_of_job == 'upload_to_CMSR':
            self.config_dict[type_of_job]['upload_date_range'] = self._upload_date_range.get()

        days_str = ", ".join(self.selected_days)
        self.config_dict[type_of_job]['description'] = f"Run {type_of_job} weekly on: {days_str} at {self.config_dict[type_of_job]['schedule_time']} repeating every {repeat_val} hour(s) in localtime."

        cron_setter(CRON_LINE=self.config_dict[type_of_job]['cron_command'], JOB_TAG=self.config_dict[type_of_job]['cron_job_name'])
        
        with open(self.config_fname, 'w') as outfile:
            yaml.dump(self.config_dict, outfile, sort_keys=False) # sort_keys=False preserves original order

        """
        ### Example below of cron job
        30 2 * * 1,2,3,4,5 /full/path/to/python3 /full/path/to/HGC_DB_postgres/task_scheduler/run_as_scheduled.py -jt upload_to_CMSR >> /full/path/to/HGC_DB_postgres/task_scheduler/schedule_job.log 2>&1 # CMSR_UPLOAD_SCHEDULE_JOB
        """
        
    def create_ssh_config_entry(self):
        conn_info = yaml.safe_load(open( os.path.join('dbase_info', 'conn.yaml') , 'r'))
        dbloader_hostname = conn_info.get('dbloader_hostname', "dbloader-hgcal.cern.ch")
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

