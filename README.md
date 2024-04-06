# HGC_DB_postgres
 Code to set up a local PostgreSQL database at an HGC MAC

 ## Getting started

Install [postgreSQL-15 with pgAdmin4](https://www.postgresql.org/download/) on your computers. Make sure you add ```psql``` to your path.

In Python 3.6 or greater:
```
pip install asyncpg
pip install pwinput
```

Prior to the creation of the database, do the following:
1. Check the names of the database and the tables in the text files in the `database_info` directory.
2. The database has multiple users with various password-protected permissions. Modify the list of users with permissions in the ... text file. Set password when creating the database. The password is not saved anywhere, so please keep it safe. The database cannot be accessed without a password.

To create database and tables:

```
cd create
python3 create_database.py
python3 create_tables.py
```

Each MAC requires the following to replicate this setup:
1. A database computer with a static IP address or host name with postgreSQL-15 with pgAdmin4 installed.
2. The computers at the other stations need to have static IPs as well.
3. Update the two config files to listen for the other computers in the lab.
4. Decide superuser and other user passwords (doesn't have to be too complicated.)
5. Follow [instructions](https://github.com/murthysindhu/HGC_DB_postgres/tree/main/documentation#1-database-interconnection-one-time-setup) to make interconnections.
6. Follow [instructions](https://github.com/murthysindhu/HGC_DB_postgres/tree/main/documentation#2-view-tables-with-pgadmin4] to view tables.

