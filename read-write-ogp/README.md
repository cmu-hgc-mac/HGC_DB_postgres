# OGP Station
 Code to write to the local DB from the OGP station

## Getting started
In Python 3.6 or greater on the OGP computer: 
```
pip install asyncpg
pip install pwinput
pip install tk
```
(In the future, this will be moved to an initial config file)
1. Make sure the .yaml file on the OGP computer is up to date with the correct database name.
2. Save the location of the **watch directory**  in the .yaml file.
3. As the OGP user, please have access to the OGP database password.

## Starting a GUI instance (will be moved into a config file)
```
cd read-write-ogp
python3 file_selector.py
```

## How to use:
This GUI contains two tabs: 'View Plots' and 'Upload Files'.
- View plots shows plots data from the OGP. (TBD: Change limit on number of files)
- Upload Files lets the user upload .XLS output files containing OGP meaurements to the local db.
  - The **watch directory** _must contain_ the following subdirectories for the GUI to work:
    - baseplates
    - hexaboards
    - protomodules
    - modules
  - The OGP inspection files _must be configured to write to these locations_ by default type.
    - The .XLS files must be saved with the ID of the component and will be used as the part ID in the db.

![OGP_GUI](https://github.com/murthysindhu/HGC_DB_postgres/assets/58646122/dbeddf4c-2dc8-4da7-8f26-f916d1c69b74)

## How this works (for developers)
- [```file_selector.py```](https://github.com/murthysindhu/HGC_DB_postgres/blob/main/read-write-ogp/file_selector.py) runs the GUI instance
- Code for reading/writing from local db are in [```postgres_tools/upload_inspect.py```](https://github.com/murthysindhu/HGC_DB_postgres/blob/main/read-write-ogp/postgres_tools/upload_inspect.py).
- The 'Upload Files' tab calls [```process_im.py```](https://github.com/murthysindhu/HGC_DB_postgres/blob/main/read-write-ogp/process_im.py) to process data by module type.
- The processing itself happens in [```ogp_height_plotter.py```](https://github.com/murthysindhu/HGC_DB_postgres/blob/main/read-write-ogp/ogp_height_plotter.py) to process data by module type.

## To create a watcher instance (will be moved into a config file)
(To ber combined with file_selector GUI)
```
cd read-write-ogp
python3 auto_upload.py
```

## Using pgAdmin4 to view tables (Instructions to follow)
Install [postgreSQL-15 with pgAdmin4](https://www.postgresql.org/download/) on your computers. Make sure you add ```psql``` to your path.


