# Viewing tables at a workstation with pgAdmin4

IP Adress whitelist needs to be set up on the database prior to this step. ([Instructions](https://github.com/murthysindhu/HGC_DB_postgres/blob/main/documentation/pg_hba_documentation.md))

## Register a new server at the workstation
![image](https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/475dafdb-725f-44b5-96b4-f50dff813446)

### 1. Specify a server name of your choice
![image](https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/47753236-a575-4f51-8a73-16c6d6cb1596)

### 2. Under `Connection`, set the database hostname (or static IP address) under `host name`. Set username and password for the type of role as found in [tables.yaml](https://github.com/murthysindhu/HGC_DB_postgres/blob/main/dbase_info/tables.yaml). Save.
**Table view and write permissions will be apply here.**

![image](https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/361fa679-a7c7-480b-8759-3c7b8c4118d4)

### 3. Refresh to see the new server.
![image](https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/153edfff-ff50-468e-b53d-6668c1ec5115)
### 4. The database tables can be viewed here.
![image](https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/df494f82-b62e-4aac-8d64-96eb0ab700ce)

### 5. To view data in tables
![image](https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/ee409bd4-bb05-4aed-ba64-efa33327157d)

