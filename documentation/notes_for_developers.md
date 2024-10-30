# Notes for developers

## How to change columns in postgres tables at all MACs?
To add columns to existing tables and propagate that change to all MACs, modify the appropriate `.csv` file under [dbase_info/postgres_tables](../dbase_info/postgres_tables). Git commit/push the changes. Then at each MAC, run `python postgres_control_panel.py` and click on the `Modify existing tables` button. Refresh pgAdmin4.

## Git pull settings
The program runs `git pull` every time `postgres_control_panel.py` is run.
