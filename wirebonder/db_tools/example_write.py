import numpy as np
import asyncio
from datetime import datetime
from postgres_tools import upload_PostgreSQL

db_table_name = 'back_wirebond'
db_table_name = 'front_wirebond'
db_table_name = 'bond_pull_test'
modname = 'testname' ### change these or load them into below dictionary directly
tech = 'meeeeeee'
comment = 'test'
wedge_id = None
spool_batch = None  
bond_count = 1
avg_pull_strg_g = 1
dummy_list_int = [1,2,3,4,5]
dummy_list_str = ['B','B','G','G','B']

db_upload = {
    'module_name': modname,
    'wedge_id': wedge_id,
    'spool_batch': spool_batch,
    'technician': tech, 
    'date_bond' : datetime.now().date(),
    'time_bond' : datetime.now().time(),    
    'comment':comment
    }

if db_table_name == 'back_wirebond':
    db_upload.update({'bond_count': bond_count})
elif db_table_name == 'bond_pull_test':
    db_upload.update({'avg_pull_strg_g': avg_pull_strg_g, 'std_pull_strg_g': avg_pull_strg_g})
elif db_table_name == 'front_wirebond':
    db_upload.update({'list_grounded_cells': dummy_list_int,
                    'list_unbonded_cells': dummy_list_int,
                    'cell_no': dummy_list_int,
                    'bond_count_for_cell': dummy_list_int,
                    'bond_type': dummy_list_str
                    })
else:
    print('Table not found! Exiting...')  
    exit()  

try:
    asyncio.run(upload_PostgreSQL(db_table_name, db_upload)) ## python 3.7
except:
    (asyncio.get_event_loop()).run_until_complete(upload_PostgreSQL(db_table_name, db_upload)) ## python 3.6
print(modname, 'uploaded!')
