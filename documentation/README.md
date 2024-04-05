# Database Set-up Instruction

In this documentation, we will cover some steps to install and set up database. The instruction may depend on your computer role (i.e., database computer, OGP computer, or Gantry computer). Make sure you find the right document for your purpose. 

### First-time setup
1. Database computer
	- Set up hostname: [pg_hba_documentation.md](https://github.com/murthysindhu/HGC_DB_postgres/blob/main/documentation/pg_hba_documentation.md)
2. Non-Database Computer
	- Go to [postgres_tools/conn.yaml](https://github.com/murthysindhu/HGC_DB_postgres/blob/454b0b3f5756493c995ed8181591e92e924318e6/read-write-ogp/postgres_tools/conn.yaml) (the link is an example for OGP computer)

	- Update IP address, institution name, and institution code. 

### pgAdmin4
1. Set up pgAdmin4: [pgAdmin_instructions.md](https://github.com/murthysindhu/HGC_DB_postgres/blob/main/documentation/pgAdmin_instructions.md)
2. How to view the list of tables and data in a table: [pgAdmin_instructions.md](https://github.com/murthysindhu/HGC_DB_postgres/blob/main/documentation/pgAdmin_instructions.md)