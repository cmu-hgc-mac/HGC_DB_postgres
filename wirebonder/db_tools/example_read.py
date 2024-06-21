import numpy as np
import asyncio
from postgres_tools import fetch_PostgreSQL


modname = '320-ML-F2CX-CM-0002' 

read_query = f"""SELECT hexaboard.list_dead_cell_init, hexaboard.list_noisy_cell_init
            FROM module_info
            JOIN hexaboard ON module_info.module_no = hexaboard.module_no
            WHERE module_info.module_name = '{modname}' LIMIT 1;"""


res = dict(asyncio.run(fetch_PostgreSQL(read_query))[0])
print(res)

dead = res['list_dead_cell_init']
noisy = res['list_noisy_cell_init'] 
ground = np.union1d(dead, noisy)

print('list_dead_cell:', dead)
print('list_noisy_cell:', noisy)
print('To be grounded:', ground)
    