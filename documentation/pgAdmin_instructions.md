# Viewing tables at a workstation with pgAdmin4

#### Outline
1. [Register a new server at the workstation](#step1)

2. [Specify a server name of your choice](#step2)
	
3. [Under `Connection`, set the database hostname (or static IP address) under `host name`. ](#step3)
	
4.  [Refresh to see the new server.](#step4)

5. [How to see the list of tables](#step5)

6. [How to view data in tables] (#step6)


**Remark**:
IP Adress whitelist needs to be set up on the database prior to this step. ([Instructions](https://github.com/murthysindhu/HGC_DB_postgres/blob/main/documentation/pg_hba_documentation.md))


---
## <a name="step1"></a>Register a new server at the workstation
<!--![image](https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/475dafdb-725f-44b5-96b4-f50dff813446)-->

<img src="https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/475dafdb-725f-44b5-96b4-f50dff813446"  width="80%">

### <a name="step2"></a>1. Specify a server name of your choice
<!--![image](https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/47753236-a575-4f51-8a73-16c6d6cb1596)-->
<img src="https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/47753236-a575-4f51-8a73-16c6d6cb1596"  width="70%">


### <a name="step3"></a>2. Under `Connection`, set the database hostname (or static IP address) under `host name`. Set username and password for the type of role as found in [tables.yaml](https://github.com/murthysindhu/HGC_DB_postgres/blob/main/dbase_info/tables.yaml). Save.
**Table view and write permissions will be apply here.**
<img src="https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/361fa679-a7c7-480b-8759-3c7b8c4118d4" width="70%">

### <a name="step4"></a>3. Refresh to see the new server.
<!--![image](https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/153edfff-ff50-468e-b53d-6668c1ec5115)-->

<img src="https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/153edfff-ff50-468e-b53d-6668c1ec5115" width="70%">

### <a name="step5"></a>4. How to see the list of tables
<!--![image](https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/df494f82-b62e-4aac-8d64-96eb0ab700ce)-->
<img src="https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/df494f82-b62e-4aac-8d64-96eb0ab700ce" height="70%">

### <a name="step6"></a>5. How to view data in tables
<!--![image](https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/ee409bd4-bb05-4aed-ba64-efa33327157d)-->

<img src="https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/ee409bd4-bb05-4aed-ba64-efa33327157d" width="70%">

