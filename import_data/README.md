## Reading from central database to local database with PASCAL
Clone PASCAL tool:

```https://gitlab.cern.ch/hgcal-database/pascal.git```

Change PACSAL path in the python file. For this to work, `cd` into the PASCAL project. Run the source file with CERN credentials. 
```
cd pascal
source env_lxplus.sh my_cern_id
```
Then `cd` back into this directory and run the python file here.
```
cd HGC_DB_postgres/import
python import_sensor_iv_data.py
```
