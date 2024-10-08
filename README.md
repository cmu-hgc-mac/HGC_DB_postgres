# HGC_DB_postgres
 Code to set up a local PostgreSQL database at an HGC MAC

 ## Getting started

Each MAC requires the following to replicate this setup:
1. A database computer with a static IP address or host name with postgreSQL-15 with pgAdmin4 installed. Ensure ```port``` in [conn.yaml](https://github.com/murthysindhu/HGC_DB_postgres/blob/main/dbase_info/conn.yaml#L2) matches with the port for postgreSQL installed on your computer.
2. In the long run, it is preferred that the computers at the other stations have static IPs as well for security reasons.
3. Decide a superuser postgres password (keep safe), a user password (with write access), and a viewer password (with only read permission; doesn't have to be too complicated.)
4. Install [postgreSQL-15 with pgAdmin4](https://www.postgresql.org/download/) on your computers. Make sure you add ```psql``` to your path.
5. Follow [instructions](https://github.com/murthysindhu/HGC_DB_postgres/tree/main/documentation#1-database-interconnection-one-time-setup) to update the two config files to listen for the other computers in the lab and make interconnections.
6. In Python 3.6 or greater:
```
pip install asyncpg tk matplotlib pwinput
```
7. Create database and tables with appropriate passwords:
```
python3 postgres_control_panel.py
```
8. Follow [instructions](https://github.com/murthysindhu/HGC_DB_postgres/tree/main/documentation#2-view-tables-with-pgadmin4) to view tables.