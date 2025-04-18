# Renaming (proto)module names
For rectifying modules that were incorrectly named during production, save the old and new names in the first and second columns respectively of a headerless CSV file. Eg.
```
320MHL2CXCM0001,320MHL2CCCM0001
320MLL3W2NT0051,320MLL3WCNT0051
```

From the `HGC_DB_postgres` directory, run the following and provide the `editor` password in the terminal:
```
python3 housekeeping/rectify_part_names.py -fp <module_names_old_new.csv>
```
The protomodule names will also get updated with the same changes.
