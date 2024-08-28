After getting the latest pull, verify that the relevant columns in the `.csv` files in `db_info` have been modified.

```
cd modify
python3 modify_table.py
>> Select the table you want to make change(s) -- (type here)
```

**Note:**
Removing a column from a `.csv` file and running the modify script will delete the column regardless of whether it has data or not. Use with caution. TBD: add a check for this.
