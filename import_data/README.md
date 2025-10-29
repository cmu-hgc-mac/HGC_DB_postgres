## Reading from HGCAPI

[CMSR HGCAPI](https://hgcapi.web.cern.ch/docs#/mac/mac_part_full_mac_part__search_id__full_get)
[INTR HGCAPI](https://hgcapi-intg.web.cern.ch/docs#/mac/mac_part_full_mac_part__search_id__full_get)

### Example of QC data JSON for download
https://hgcapi.web.cern.ch/mac/qc/pedestals/320MLF3TCCM0221


## Reading from central database to local database with PASCAL
Clone [PASCAL tool](https://gitlab.cern.ch/hgcal-database/pascal.git):

```git clone https://gitlab.cern.ch/hgcal-database/pascal.git```

Change PACSAL path in the python file. For this to work, `cd` into the PASCAL project. Run the source file with CERN credentials. 
```
cd pascal
source env_lxplus.sh <my_cern_id>
```
Then `cd` back into this directory and run the python file here.
```
cd HGC_DB_postgres
python import_data/import_sensor_iv_data.py
```
