# HGC_DB_postgres
 Code to set up a local PostgreSQL database at an HGC MAC

 ## Getting started

Install [postgreSQL-15 with pgAdmin4](https://www.postgresql.org/download/) on your computers.

In Python 3.6 or greater:
```
pip install psycopg2
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
