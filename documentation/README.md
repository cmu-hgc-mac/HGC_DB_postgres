# Database Set-up Instruction

## Contents
1. [Database interconnection one-time setup](#step1)
	1. [IP Addresses whitelist on database](#step1a)
 	2. [Connection configuration at workstations](#step1b)
2. [Table viewer with pgAdmin](#step2)
3. User permissions at various workstations(#step3)

## 1. <a name="step1"></a>Database interconnection one-time setup
Once the database has been set up, other workstations in the lab need to connect to the database to write data to it. 
1. <a name="step1a"></a>IP Addresses whitelist on database: [Instructions](https://github.com/murthysindhu/HGC_DB_postgres/blob/main/documentation/pg_hba_documentation.md)
2. <a name="step1b"></a>Connection configuration at workstations
	- Go to [postgres_tools/conn.yaml](https://github.com/murthysindhu/HGC_DB_postgres/blob/454b0b3f5756493c995ed8181591e92e924318e6/read-write-ogp/postgres_tools/conn.yaml) (the link is an example for OGP computer)

	- Update IP address, institution name, and institution code. 

## 2. <a name="step2"></a>View tables with pgAdmin4
1. View tables with pgAdmin on the database: [Instructions](https://github.com/murthysindhu/HGC_DB_postgres/blob/main/documentation/pgAdmin_instructions.md#3-refresh-to-see-the-new-server)
2. View tables with pgAdmin at workstations: [Instructions](https://github.com/murthysindhu/HGC_DB_postgres/blob/main/documentation/pgAdmin_instructions.md)
