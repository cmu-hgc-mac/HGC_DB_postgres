# HGC_DB_postgres
Code to set up a local PostgreSQL database at an HGC MAC
```
 git clone https://github.com/cmu-hgc-mac/HGC_DB_postgres.git
```

## Using the control panel
If the database already exists, and you wish to access only the postgres control panel on a different computer, do the following:
```
git clone https://github.com/cmu-hgc-mac/HGC_DB_postgres.git
cd HGC_DB_postgres
pip install -r housekeeping/requirements.txt
## Create a copy of dbase_info/conn.py and update with the right credentials.
python3 postgres_control_panel.py
```

 ## Setting up the postgreSQL database

Each MAC requires the following to replicate this setup:
1. A database computer with a static IP address or host name with postgreSQL-15 with pgAdmin4 installed. 
2. Ensure ```port``` in [conn.yaml](https://github.com/murthysindhu/HGC_DB_postgres/blob/main/dbase_info/conn_example.yaml#L2) matches with the port for postgreSQL installed on your computer. Update insitution abbreviation. (Change `db_hostname` only if database is hosted on a different computer.) Verify the CERN database location.
3. In the long run, it is preferred that the computers at the other stations have static IPs as well for security reasons.
4. Decide a postgres superuser password (keep safe), a user password (with write access), and a viewer password (with only read permission; doesn't have to be too complicated.) (See [password guidelines](https://github.com/cmu-hgc-mac/HGC_DB_postgres/blob/main/documentation/pgAdmin_instructions.md#database-password-guidelines).)
5. Install [postgreSQL-15 with pgAdmin4](https://www.pgadmin.org/download/) on your computers. Make sure you add ```psql``` to your path. Use the postgres superuser password with pgAdmin4.
6. Follow [instructions](https://github.com/cmu-hgc-mac/HGC_DB_postgres/blob/main/documentation/pg_hba_documentation.md) to update the two config files to listen for the other computers in the lab and make interconnections.
7. In a Python virtual environment or Conda environment:
<!-- ```
pip install asyncpg tk matplotlib pwinput pillow pyyaml paramiko cryptography
``` -->
```
pip install -r housekeeping/requirements.txt
```
8. Create database and tables with appropriate passwords:
```
python3 postgres_control_panel.py
```
Click on the `Modify Dbase tables` button to implement the latest updates to the tables.

![Screenshot 2025-02-04 at 12 55 06 PM](https://github.com/user-attachments/assets/98a21c6b-2dad-4c60-b333-eaa5c2424d04)


9. It is recomended to verify parts once they are received at a MAC. This can be done by uploading a list of barcodes.
10. The import parts button downloads from INT2R/CMSR the parts that are marked for the specific MAC.

    (Col `kind` gets populated when parts data are imported from CERN; Col `date_received_verify` gets populated upon verification; Discrepancy between these columns suggests missing parts)
11. The `Refresh` button updates primary keys and foreign keys and connects the parts in the various tables. This should happen automatically during the assembly steps but the button may be used in instances it doensn't happen for some reason.

12. The control panel also opens browser-based AdminerEvo for vieweing data. This requires installation of [PHP](https://github.com/cmu-hgc-mac/HGC_DB_postgres/blob/main/documentation/php_installation.md).

![image](https://github.com/user-attachments/assets/3083f1fd-7606-41e9-8697-823591bd1f48)

12. (Optional) Follow [instructions](https://github.com/cmu-hgc-mac/HGC_DB_postgres/blob/main/documentation/pgAdmin_instructions.md#4-view-the-list-of-tables) to view tables in pgAdmin4 tool on the database computer. The tables are found under `hgcdb -> Schemas -> public -> Tables`. Right-click on a selected table to `view/edit data` for the `Last 100 Rows`.
13. (Optional) Follow [instructions](https://github.com/cmu-hgc-mac/HGC_DB_postgres/blob/main/documentation/pgAdmin_instructions.md) to view tables in pgAdmin4 tool on the other stations.
<img src="https://raw.githubusercontent.com/cmu-hgc-mac/HGC_DB_postgres/main/documentation/images/table_example.png" alt="Postgres Control Panel" width="95%">




