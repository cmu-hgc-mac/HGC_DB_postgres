# Access data from other MACs

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

# Get monthly module summary
This has been implemented for CMU, UCSB where it outputs the module summary for a month in a year.
```
python mac_public/module_counts_for_month.py -m 3 -y 2025 -mac UCSB
```