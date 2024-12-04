# Access data from other MACs

IP adrresses of the six MACs are in [mac_public/macs_db_conn.yaml](https://github.com/cmu-hgc-mac/HGC_DB_postgres/blob/main/mac_public/macs_db_conn.yaml).

From the main directory, run
```
python mac_public/get_macs_data.py -q mod_simple_query -of csv
```

Run for options
```
python mac_public/get_macs_data.py --help
```
