# How to connect a workstation computer to the database
This section goes through a step-by-step instruction to connect a computer to a database computer. 
A whitelist of IP addresses of the other stations need to be added to ```pg_hba.conf``` on the database.
**The following actions should be done just once** when you set up the database. 

- This step can be performed only after postgres is installed and ```psql``` is added to path.
- The database and station IP addresses needs to be static.
- One entry into ```pg_hba.conf``` per station in the lab.

# Find path to .conf files and edit
1. In command prompt: `psql -U postgres -c 'SHOW config_file'` in Mac/Linux. For Windows, do the same in Powershell as Administrator.
2. Enter postrges password when prompted.
3. Note the global path to `postgresql.conf` file. The `pg_hba.conf` can be found in the same directory.

# Edit `postgresql.conf`
4. `sudo nano /[global_path_to_conf]/postgresql.conf` to open and edit in Mac/Linux. <br />
(In Windows, open as Administrator with `notepad /[global_path_to_conf]/postgresql.conf`)                                                                                                               
**note**:
In Mac/Linux, it is customary to find it under ```PostgreSQL/15/main/pg_hba.conf``` . In Windows, it is typically found under ```C:/Program Files/PostgreSQL/l5/data/pg_hba.conf```.

5. Under `Connections and Authentication`, change `listen_addresses` to `'*'` from `localhost`.
6. Save and close `postgresql.conf`. ([Restart required](pg_hba_documentation.md#restart-postgresql)).

```
#------------------------------------------------------------------------------
# CONNECTIONS AND AUTHENTICATION
#------------------------------------------------------------------------------

# - Connection Settings -

listen_addresses = '*'          # what IP address(es) to listen on;
                                        # comma-separated list of addresses;
                                        # defaults to 'localhost'; use '*' for all
                                        # (change requires restart)
port = 5432                             # (change requires restart)
```

# Edit `pg_hba.conf`

7. `sudo nano /[global_path_to_conf]/pg_hba.conf` to open and edit in Mac/Linux. <br />
(In Windows, open as Administrator with `notepad /[global_path_to_conf]/pg_hba.conf`)                                                                                                                
8. After the first entry under ```# IPv4 local connections:```, add the following line for each station connecting into the database: <br />
 **```host  all  all  [station ip address or hostname] trust```**
9. Save and close `pg_hba.conf`.
10. [Restart](pg_hba_documentation.md#restart-postgresql) postgreSQL15.

### Example
```
# TYPE  DATABASE        USER            ADDRESS                 METHOD

# "local" is for Unix domain socket connections only
local   all             all                                         scram-sha-256
# IPv4 local connections:
host    all             all             127.0.0.1/32                scram-sha-256
host    all             all             mycomp1.phys.school.edu     trust ## OGP
host    all             all             mycomp2.phys.school.edu     trust ## Gantry 
host    all             all             mycomp3.phys.school.edu     trust ## Test stand
host    all             all             192.168.0.1/32              trust ## Shipping
```

**note**:
- For numerical IP addresses, **`/32` must be included after the address.**
- Do **NOT** include `/32` for human-readable hostname.

# Restart postgreSQL
Closing and opening pgAdmin4 should restart postgreSQL.

For Linux computers, try
```sudo service postgresql restart```
 
# Test connection
After adding the station hostname to the database, run the following in `python3` at that station with the appropriate **database hostname and password** for the default `postgres` usertype and database. Try the same with other usertypes after the database has been created.
<pre>
import asyncpg, asyncio
conn = asyncio.run(asyncpg.connect(
        host=<b>'db_hostname.phys.school.edu'</b>,
        password=<b>'db_password'</b>
        database='postgres',
        user=<b>'postgres'</b>))
print('Connection successful!')
</pre>
