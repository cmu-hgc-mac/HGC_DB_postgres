import psycopg2
from datetime import datetime

def get_query_write(table_name, column_names):
    pre_query = f""" INSERT INTO {table_name} ({', '.join(column_names)}) VALUES """
    data_placeholder = ', '.join([f'%s' for _ in range(len(column_names))])  # Use %s for psycopg2 placeholders
    return f"""{pre_query} ({data_placeholder})"""

def init_conn(conn_info):
    return psycopg2.connect(
        host=conn_info[0],
        database=conn_info[1],
        user=conn_info[2],
        password=conn_info[3],)

def main():
    now = datetime.now()
    conn = init_conn(conn_info = ['localhost', 'hgcdb_test', 'shipper', 'mypassword'] ) 
    db_upload_data = {
        'log_timestamp': now,
        'log_location': 'main_clean_room',
        'temp_c': 22.5,
        'rel_hum': 45.5,
        'prtcls_per_cubic_m_500nm': 100,
        'prtcls_per_cubic_m_1um': 234,
        'prtcls_per_cubic_m_5um': 234,}

    query = get_query_write(table_name = "environmental_conditions", column_names = list(db_upload_data.keys()))
    with conn.cursor() as cursor:
        cursor.execute(query, tuple(db_upload_data.values())) 
    conn.commit(); conn.close()

main()