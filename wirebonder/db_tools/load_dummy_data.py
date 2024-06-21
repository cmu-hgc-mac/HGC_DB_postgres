import asyncio
from postgres_tools import upload_PostgreSQL

def upload(db_table_name, db_upload):
    try:
        asyncio.run(upload_PostgreSQL(db_table_name, db_upload)) ## python 3.7
    except:
        (asyncio.get_event_loop()).run_until_complete(upload_PostgreSQL(db_table_name, db_upload)) ## python 3.6

dat = []
dat.append([1, '320-ML-F2CX-CM-0001', 'hexboard1', [4,23,34,63], [43,23,74,83]])
dat.append([2, '320-ML-F2CX-CM-0002', 'hexboard2', [54,12,35,98], [76,73,74,12]])

for dat_i in dat:
    table_name = 'module_info'
    db_upload = {'module_no': dat_i[0], 'module_name': dat_i[1]}
    upload(table_name, db_upload)
    table_name = 'hexaboard'
    db_upload = {'module_no': dat_i[0], 'hxb_name': dat_i[2], 'list_dead_cell_init': dat_i[3],'list_noisy_cell_init': dat_i[4] }
    upload(table_name, db_upload)
