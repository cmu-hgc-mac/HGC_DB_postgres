import psycopg2
import os
import sys
import csv
import sys
sys.path.append('../')
from HGC_DB_postgres.src.utils import connect_db, get_table_name


## read a data from OGP written in txt file
## here, we work on bp_inspection

## import data stored in a text file
txt_dir = 'upload/FNAL3_2_v1.txt'
f = open(txt_dir, 'r')
lines = f.readlines()

conn = connect_db()
cursor = conn.cursor()

insert_query = """
INSERT INTO bp_inspect (bp_name, date_inspect, time_inspect, inspector, feature_type, feature_name, value, actual, 
nominal, deviation, neg_tolerance, pos_tolerance, out_of_tolerance, comment)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

## retrieve the data that is common to all rows
bp_name = lines[0].split('=')[1].split('\n')[0]
date_inspect = lines[2].split('=')[1].split('T')[0]
time_inspect = lines[2].split('=')[1].split('T')[1].split('\n')[0]
inspector = lines[7].split(',')[2]

common_info = []
common_info.append(bp_name)
common_info.append(date_inspect)
common_info.append(time_inspect)
common_info.append(inspector)

## retrieve the rest of data that varies row by row
numeric_data_starting_row_num = 11## here i'm skipping the lines that contains $$ 

for i in range(numeric_data_starting_row_num, len(lines)):
    data = lines[i].strip().split(',')[:-1]
    data = common_info + data

    if lines[i] == 'End!\n':
        break

    cursor.execute(insert_query, tuple(data))
conn.commit()

print('Data is successfully uploaded to the database!')

## close connection
cursor.close()
conn.close()

    