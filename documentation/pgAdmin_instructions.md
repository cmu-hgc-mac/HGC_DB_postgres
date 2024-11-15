# Viewing tables at a workstation with pgAdmin4

#### Outline
1. [Register a new server at the workstation](#step1)

2. [Specify a server name of your choice](#step2)
	
3. [Under `Connection`, set the database hostname (or static IP address) under `host name`. ](#step3)
	
4.  [Refresh to see the new server.](#step4)

5. [View the list of tables](#step5)

6. [View data in tables](#step6)
   
[Database password guidelines](#dbpassword)


**Remark**:
IP Adress whitelist needs to be set up on the database prior to this step. ([Instructions](https://github.com/murthysindhu/HGC_DB_postgres/blob/main/documentation/pg_hba_documentation.md))


---
## <a name="step1"></a>Register a new server at the workstation
<!--![image](https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/475dafdb-725f-44b5-96b4-f50dff813446)-->

<img src="https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/475dafdb-725f-44b5-96b4-f50dff813446"  width="80%">

### <a name="step2"></a>1. Specify a server name of your choice
<!--![image](https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/47753236-a575-4f51-8a73-16c6d6cb1596)-->
<img src="https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/47753236-a575-4f51-8a73-16c6d6cb1596"  width="70%">


### <a name="step3"></a>2. Under `Connection`, set the database hostname (or static IP address) under `host name`. Set username and password for the type of role. Save.
(The available roles are `ogp_user`, `gantry_user`, `viewer`, `teststand_user`, and `shipper` as found in [tables.yaml](https://github.com/murthysindhu/HGC_DB_postgres/blob/main/dbase_info/tables.yaml)) **Table view and write permissions will be apply here.** See below for [password guidelines](#dbpassword).

<img src="https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/361fa679-a7c7-480b-8759-3c7b8c4118d4" width="70%">

### <a name="step4"></a>3. Refresh to see the new server.
**Note:** The database on the database computer will be under the default `PostgreSQL` server.
<!--![image](https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/153edfff-ff50-468e-b53d-6668c1ec5115)-->

<img src="https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/153edfff-ff50-468e-b53d-6668c1ec5115" width="70%">

### <a name="step5"></a>4. View the list of tables
**Note:** The database on the database computer will be under the default `PostgreSQL` server.
<!--![image](https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/df494f82-b62e-4aac-8d64-96eb0ab700ce)-->
<img src="https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/df494f82-b62e-4aac-8d64-96eb0ab700ce" height="70%">

### <a name="step6"></a>5. View data in tables
<!--![image](https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/ee409bd4-bb05-4aed-ba64-efa33327157d)-->

<img src="https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/ee409bd4-bb05-4aed-ba64-efa33327157d" width="70%">

# <a name="dbpassword"></a>Database password guidelines

There are a number of passwords in this database setup. The `postgres` user is the superuser for the database and has permission to delete the database. This password should be kept secure. The `viewer` has permissions to view only. The other `user`s have various _write_ permissions specific to their function as listed in [tables.yaml](https://github.com/cmu-hgc-mac/HGC_DB_postgres/blob/main/dbase_info/tables.yaml#L1-L35). You may choose to use the same password for all of `user`s. Apart from these, if you have pgAdmin4 installed at the various workstations, it will ask you to set a password here as well. You can choose a simple password and even choose to `Save password` for future logins. This is okay because the pgAdmin tool is distict from the PostgreSQL database and acts as a GUI. This means that it is okay to (re)uninstall pgAdmin4 (but not PostgreSQL) and still have access to your tables. However, refrain from saving password for the default `postgres` user.

In short: the left instance below need not be secure but be judicious with who has access to the right.

<p float="left">
<img src="https://github.com/user-attachments/assets/f5d64378-cff8-4f53-8df1-afc33abad3ca" width="500" />
<img src="https://github.com/user-attachments/assets/578f611d-9b65-43e0-8338-877833b86aa7" width="500" />
</p>

## Updating passwords
On pgAdmin4, select the database and open the `PSQL tool`.
To list users in the `hgcdb` database, in the PSQL terminal type
```
\du
```
To change password, for example, to change `shipper` password:

```
\password shipper
```
![image](https://github.com/user-attachments/assets/4e5bbde8-bd28-4215-bab2-8f3756e56643)


