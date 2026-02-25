# View postgres of other MACs on your browser with Adminer

Launch Adminer in the postgres control panel by selecting the `Search/Edit Postgres Data` button. Then run the following in your terminal with your lxplus credentials:
```
ssh \
  -L 15432:cmsmac04.phys.cmu.edu:5432 -L 15433:gut.physics.ucsb.edu:5432  -L 15434:lxhgcdb02.tifr.res.in:5432 \
  -L 15435:hgcal.ihep.ac.cn:5432      -L 15436:hep11.phys.ntu.edu.tw:5432 -L 15437:dbod-ttu-mac-local.cern.ch:6621 \
  <YOUR_CERN_USERNAME>@lxtunnel.cern.ch
```

Then click on the below links to launch the table viewer for the various MACs.

- [CMU](http://127.0.0.1:8083/adminer-pgsql.php?pgsql=localhost%3A15432&username=viewer&db=hgcdb&ns=public&select=module_info&columns%5B0%5D%5Bfun%5D=&columns%5B0%5D%5Bcol%5D=&where%5B0%5D%5Bcol%5D=&where%5B0%5D%5Bop%5D=%3D&where%5B0%5D%5Bval%5D=&order%5B0%5D=module_no&desc%5B0%5D=1&order%5B01%5D=&limit=500&text_length=100)

- [IHEP](http://127.0.0.1:8083/adminer-pgsql.php?pgsql=localhost%3A15435&username=viewer&db=postgres&ns=public&select=module_info&columns%5B0%5D%5Bfun%5D=&columns%5B0%5D%5Bcol%5D=&where%5B0%5D%5Bcol%5D=&where%5B0%5D%5Bop%5D=%3D&where%5B0%5D%5Bval%5D=&order%5B0%5D=module_no&desc%5B0%5D=1&order%5B01%5D=&limit=500&text_length=100)

- [NTU](http://127.0.0.1:8083/adminer-pgsql.php?pgsql=localhost%3A15436&username=viewer&db=hgcdb&ns=public&select=module_info&columns%5B0%5D%5Bfun%5D=&columns%5B0%5D%5Bcol%5D=&where%5B0%5D%5Bcol%5D=&where%5B0%5D%5Bop%5D=%3D&where%5B0%5D%5Bval%5D=&order%5B0%5D=module_no&desc%5B0%5D=1&order%5B01%5D=&limit=500&text_length=100)

- [TIFR](http://127.0.0.1:8083/adminer-pgsql.php?pgsql=localhost%3A15434&username=viewer&db=hgcdb&ns=public&select=module_info&columns%5B0%5D%5Bfun%5D=&columns%5B0%5D%5Bcol%5D=&where%5B0%5D%5Bcol%5D=&where%5B0%5D%5Bop%5D=%3D&where%5B0%5D%5Bval%5D=&order%5B0%5D=module_no&desc%5B0%5D=1&order%5B01%5D=&limit=500&text_length=100)

- [TTU](http://127.0.0.1:8083/adminer-pgsql.php?pgsql=localhost%3A15437&username=viewer&db=ttu_mac_local&ns=public&select=module_info&columns[0][fun]=&columns[0][col]=&where[0][col]=&where[0][op]=%3D&where[0][val]=&order[0]=module_no&desc[0]=1&order[01]=&limit=500&text_length=100)

- [UCSB](http://127.0.0.1:8083/adminer-pgsql.php?pgsql=localhost%3A15433&username=viewer&db=hgcdb&ns=public&select=module_info&columns%5B0%5D%5Bfun%5D=&columns%5B0%5D%5Bcol%5D=&where%5B0%5D%5Bcol%5D=&where%5B0%5D%5Bop%5D=%3D&where%5B0%5D%5Bval%5D=&order%5B0%5D=module_no&desc%5B0%5D=1&order%5B01%5D=&limit=500&text_length=100)

---
---


# Download module testing & QC data
A standalone script [`module_qc_data_download`](https://github.com/cmu-hgc-mac/HGC_DB_postgres/blob/main/mac_public/module_qc_data_download.py) has been implemented for CMU, UCSB where it saves the module testing data from the MAC into a CSV file. Examples - 
```
python module_qc_data_download.py -mac CMU  -dt mod_ped -mn 320MLF2CXCM0001 320MLF2CXCM0002
python module_qc_data_download.py -mac CMU  -dt mod_iv   ##### Saves all available data
python module_qc_data_download.py -mac CMU  -dt mod_qcs -mn 320MLF3TCCM0116
python module_qc_data_download.py -mac UCSB -dt mod_iv  -mn 320MHF1T4SB0015
```

# Get monthly module summary
This script outputs the module summary for a month in a year.
```
python mac_public/module_counts_for_month.py -m 3 -y 2025 -mac UCSB
```

# Read with custom queries (from other MACs, FNAL)

IP adrresses of the six MACs are in [mac_public/macs_db_conn.yaml](https://github.com/cmu-hgc-mac/HGC_DB_postgres/blob/main/mac_public/macs_db_conn.yaml).

Sample queries can be found in [mac_public/queries.py](https://github.com/cmu-hgc-mac/HGC_DB_postgres/blob/main/mac_public/queries.py).

From the main directory, run
```
python mac_public/save_macs_data.py -q mod_simple_query -of csv
```

Run for options
```
python mac_public/save_macs_data.py --help
```

Alternatively, you can also import `get_macs_data()` and pass your own query.
```
from HGC_DB_postgres.mac_public.get_macs_data import get_macs_data

my_query = """SELECT module_name, hxb_name FROM module_info"""
data_list = get_macs_data(query = my_query, macs_conn_file = 'HGC_DB_postgres/mac_public/macs_db_conn.yaml')
```

---
---

# Allow access from FNAL/LPC
To allow acces to your MAC local database from FNAL/LPC, MACs will need to include the below lines in their pg_hba.conf file ([Instructions](https://github.com/cmu-hgc-mac/HGC_DB_postgres/blob/main/documentation/pg_hba_documentation.md#example)). 
```
host    all    viewer    188.184.0.0/15    trust    ## CERN datacenter LCG, ITS
host    all    viewer    128.142.0.0/16    trust    ## CERN datacenter LCG
host    all    viewer    137.138.0.0/16    trust    ## CERN datacenter ITS
host    all    viewer    131.225.0.0/16    trust    ## Viewer w/o password on FNAL network
```

# Run on LXPLUS with local file
```
ssh <USERNAME>@lxplus.cern.ch "python3 - -mac UCSB -y 2025 -m 7" < module_counts_for_month.py
```
