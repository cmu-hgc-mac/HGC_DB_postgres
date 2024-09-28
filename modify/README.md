After getting the latest pull, verify that the relevant columns in the `.csv` files in `db_info` have been modified. Then do:


### To modify all tables
```
cd modify
python3 modify_table.py -t all
```
### To modify select table with name `table_name`
```
cd modify
python3 modify_table.py -t table_name
```
