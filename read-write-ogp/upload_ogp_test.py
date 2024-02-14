# OGP Computer
# 172.24.114.203

import psycopg2
conn = psycopg2.connect(
    host = 'cmsmac04.phys.cmu.edu',
    database = 'testdb3',
    user = 'postgres',
    password = 'hgcal'
)

cursor = conn.cursor()

cursor.execute(
"""
SELECT bp_name
FROM bp_inspect
WHERE bp_row_no = '2'
"""
)

value = cursor.fetchone()
print(value)

cursor.close()
conn.close()