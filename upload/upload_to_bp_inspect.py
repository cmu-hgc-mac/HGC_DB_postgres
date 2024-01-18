import psycopg2
import os
import sys
import csv
import sys
sys.path.append('../')
from src.utils import connect_db, get_table_name

## read a data from OGP written in txt file
## here, we work on bp_inspection


def find_numeric_data_row(txt_dir = 'upload/FNAL3_2_v1.txt'):
    '''
    This function assumes that the text file has the same structure as 'FNAL3_2_v1.txt'
    Please change it accordingly.
    '''
    f = open('../' + txt_dir, 'r')
    lines = f.readlines()
    
    count = 0

    for i in range(len(lines)):
        if lines[i][:2] == ',,':
            count += 1
        if (count > 1) & (lines[i][:2] != ',,'):
            num_data_row = i
            break
    return num_data_row

## import data stored in a text file
txt_dir = '../upload/FNAL3_2_v1.txt'
f = open(txt_dir, 'r')
lines = f.readlines()

conn = connect_db()
cursor = conn.cursor()

insert_query = """
INSERT INTO bp_inspect (bp_name, date_inspect, time_inspect, inspector, sensor_resolution, geometry, 
feature_type, feature_name, value, actual, nominal, deviation, neg_tolerance, pos_tolerance, 
out_of_tolerance, comment)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

## retrieve the data that is common to all rows
bp_name = lines[6].split(',')[2]
date_inspect = lines[2].split('=')[1].split('T')[0]
time_inspect = lines[2].split('=')[1].split('T')[1].split('\n')[0]
inspector = lines[7].split(',')[2]
sensor_resolution = lines[8].split(',')[2]
geometry = lines[9].split(',')[2]

common_info = []
common_info.append(bp_name)
common_info.append(date_inspect)
common_info.append(time_inspect)
common_info.append(inspector)
common_info.append(sensor_resolution)
common_info.append(geometry)

## retrieve the rest of data that varies row by row
numeric_data_starting_row_num = find_numeric_data_row()

for i in range(numeric_data_starting_row_num, len(lines)):
    data = lines[i].strip().split(',')[:-1]
    data = common_info + data

    if lines[i] == 'End!\n':
        break

    cursor.execute(insert_query, tuple(data))
conn.commit()

print('Data is successfully uploaded to the database!')

## close connection
# cursor.close()
# conn.close()

## delete blank rows where value IS blank
# conn = connect_db()
# cursor = conn.cursor()

delete_blank_query = """
DELETE FROM bp_inspect
WHERE TRIM(value) = '';
"""

cursor.execute(delete_blank_query)
conn.commit()

print('Blank rows in Value column is successfully removed!')
## close connection
cursor.close()
conn.close()

    