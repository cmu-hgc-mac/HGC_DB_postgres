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

5. Set `max_connections = 500` and verify that `port = 5432` is available.
6. Under `Connections and Authentication`, list static IPs or hostnames of other stations under `listen_addresses` along with `localhost`. Note: if IP addresses are not known, it can be configured to listen on all addresses with `listen_addresses = '*'`.
7. Save and close `postgresql.conf`. ([Restart required](pg_hba_documentation.md#restart-postgresql)).

```
#------------------------------------------------------------------------------
# CONNECTIONS AND AUTHENTICATION
#------------------------------------------------------------------------------

# - Connection Settings -

listen_addresses = '*'                  # what IP address(es) to listen on;
                                        # comma-separated list of addresses;
                                        # defaults to 'localhost'; use '*' for all
                                        # (change requires restart)
port = 5432                             # (change requires restart)
```

# Edit `pg_hba.conf`

8. `sudo nano /[global_path_to_conf]/pg_hba.conf` to open and edit in Mac/Linux. <br />
(In Windows, open as Administrator with `notepad /[global_path_to_conf]/pg_hba.conf`)                                                                                                                
9. After the first entry under ```# IPv4 local connections:```, add the following line for each station connecting into the database: <br />
 **```host  all  all  [station ip address or hostname]  scram-sha-256```**
10. You may contact your institution's IT department to get the **IP address and netmask** for their network to restrict access and keep your computer secure. ```host  all  all  $$$.$$$.$$$.$$$/$$  scram-sha-256```
Note: if IP addresses are not known, it can be configured to accept all connections with ```host  all  all  0.0.0.0/0  scram-sha-256```. This is not secure and hence not recommended during production.
11. The `viewer` user can be set to be publicly accessible without password with ```host  all  viewer  0.0.0.0/0  trust```. A `viewer` may only read from the database and has no edit permissions. Various user permissions for the differernt tables are in [dbase_info/tables.yaml](https://github.com/cmu-hgc-mac/HGC_DB_postgres/blob/main/dbase_info/tables.yaml#L37).
12. Save and close `pg_hba.conf`.
13. [Restart](pg_hba_documentation.md#restart-postgresql) postgreSQL15.

### Example
```
# TYPE  DATABASE        USER            ADDRESS                     METHOD

# "local" is for Unix domain socket connections only
local   all             all                                         scram-sha-256
# IPv4 local connections:
host    all             all             127.0.0.1/32                scram-sha-256
host    all             viewer          $$$.$$$.$$$.$$$/$$          trust          ## Viewer w/o password on school network
host    all             all             $$$.$$$.$$$.$$$/$$          scram-sha-256  ## Require password to write to db from school network

host    all             all             mycomp1.phys.school.edu     md5            ## Test stand
host    all             all             192.168.0.1/32              scram-sha-256  ## Shipping
host    all             all             0.0.0.0/0                   scram-sha-256  ## From anywhere w/ password -- Not recommended

host    all             viewer          188.184.0.0/15    trust    ## CERN datacenter LCG, ITS
host    all             viewer          128.142.0.0/16    trust    ## CERN datacenter LCG
host    all             viewer          137.138.0.0/16    trust    ## CERN datacenter ITS
host    all             viewer          131.225.0.0/16    trust    ## Viewer w/o password on FNAL network
```

How to read above example --
- A user can connect to the database as `viewer` from any IP address and with no password required.
- A user from the set Gantry IP address may connect to the database as `gantry_user` only and with password required.
- A user from the set Test stand IP addresscan may connect to the database with any user type but with password required.
- `scram-sha-256` and `md5` are password encrytion methods used by postgres to save passwords. The former is more secure.

**note**:
- For numerical IP addresses, **`/32` must be included after the address.**
- Do **NOT** include `/32` for human-readable hostname.

# Restart postgreSQL
The simplest way to restart postgreSQL is to restart the computer.

To reload, `pg_hba.conf`, connect to the server and database on pgAdmin. Open the PSQL Tool (last Icon in Object Explorer in top left corner) and run: ```SELECT pg_reload_conf();```

Postgres can also be restarted in the command line. For Linux computers, try in command line
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
