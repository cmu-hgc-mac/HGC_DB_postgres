# HGC_DB_postgres
Code to set up a local PostgreSQL database at an HGC MAC
```
 git clone https://github.com/cmu-hgc-mac/HGC_DB_postgres.git
```

 ## Getting started

Each MAC requires the following to replicate this setup:
1. A database computer with a static IP address or host name with postgreSQL-15 with pgAdmin4 installed. 
2. Ensure ```port``` in [conn.yaml](https://github.com/murthysindhu/HGC_DB_postgres/blob/main/dbase_info/conn.yaml#L2) matches with the port for postgreSQL installed on your computer. Update insitution abbreviation. (Change `db_hostname` only if database is hosted on a different computer.) Verify the CERN database location.
3. In the long run, it is preferred that the computers at the other stations have static IPs as well for security reasons.
4. Decide a postgres superuser password (keep safe), a user password (with write access), and a viewer password (with only read permission; doesn't have to be too complicated.)
5. Install [postgreSQL-15 with pgAdmin4](https://www.pgadmin.org/download/) on your computers. Make sure you add ```psql``` to your path. Use the postgres superuser password with pgAdmin4.
6. Follow [instructions](https://github.com/cmu-hgc-mac/HGC_DB_postgres/blob/main/documentation/pg_hba_documentation.md) to update the two config files to listen for the other computers in the lab and make interconnections.
7. In Python 3.6 or greater:
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
Click on the `Modify existing tables` button to implement the latest updates to the tables.

<img src="https://raw.githubusercontent.com/cmu-hgc-mac/HGC_DB_postgres/main/documentation/images/postgres_control_panel.png" alt="Postgres Control Panel" width="25%">

9. Follow [instructions](https://github.com/cmu-hgc-mac/HGC_DB_postgres/blob/main/documentation/pgAdmin_instructions.md#4-view-the-list-of-tables) to view tables in pgAdmin4 tool on the database computer. The tables are found under `hgcdb -> Schemas -> public -> Tables`. Right-click on a selected table to `view/edit data` for the `Last 100 Rows`.
10. Follow [instructions](https://github.com/cmu-hgc-mac/HGC_DB_postgres/blob/main/documentation/pgAdmin_instructions.md) to view tables in pgAdmin4 tool on the other stations.
<img src="https://raw.githubusercontent.com/cmu-hgc-mac/HGC_DB_postgres/main/documentation/images/table_example.png" alt="Postgres Control Panel" width="95%">




