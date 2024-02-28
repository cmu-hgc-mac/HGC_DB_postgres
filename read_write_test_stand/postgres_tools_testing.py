import asyncpg
import asyncio

def get_query(table_name):
    if table_name == 'module_pedestal_test':
        pre_query = f""" 
        INSERT INTO {table_name}  
        (module_name, rel_hum, temp_c, bias_vol, chip, channel, channeltype, adc_median, adc_iqr, tot_median, tot_iqr, toa_median, toa_iqr, adc_mean, adc_stdd, tot_mean, tot_stdd, toa_mean, toa_stdd, tot_efficiency, tot_efficiency_error, toa_efficiency, toa_efficiency_error, pad, x, y, date_test, time_test, inspector, comment) 
        VALUES   """  ### maintain space
    elif table_name == 'module_iv_test':
        pre_query = f""" 
        INSERT INTO {table_name} 
        (module_name, rel_hum, prog_v, meas_v, meas_i, meas_r, date_test, time_test, inspector, comment)  
        VALUES  """  ### maintain space
    elif table_name == 'hxb_pedestal_test':
        pre_query = f""" 
        INSERT INTO {table_name} 
        (hxb_name, rel_hum, temp_c, chip, channel, channeltype, adc_median, adc_iqr, tot_median, tot_iqr, toa_median, toa_iqr, adc_mean, adc_stdd, tot_mean, tot_stdd, toa_mean, toa_stdd, tot_efficiency, tot_efficiency_error, toa_efficiency, toa_efficiency_error, pad, x, y, date_test, time_test, inspector, comment) 
        VALUES  """  ### maintain space
    data_placeholder = ', '.join(['${}'.format(i) for i in range(1, len(pre_query.split(','))+1)])
    query = f"""{pre_query} {'({})'.format(data_placeholder)}"""
    return query

async def upload_PostgreSQL(table_name, db_upload_data):
    conn = await asyncpg.connect(
        host='cmsmac04.phys.cmu.edu',
        database='hgcdb',
        user='postgres',
        password='hgcal'
    )
    
    print('Connection successful.\n')

    schema_name = 'public'
    table_exists_query = """
    SELECT EXISTS (
        SELECT 1 
        FROM information_schema.tables 
        WHERE table_schema = $1 
        AND table_name = $2
    );
    """
    table_exists = await conn.fetchval(table_exists_query, schema_name, table_name)  ### Returns True/False
    if table_exists:
        query = get_query(table_name)
        await conn.execute(query, *db_upload_data)
        print(f'Executing query: {query}')
        print(f'Data is successfully uploaded to the {table_name}!')
    else:
        print(f'Table {table_name} does not exist in the database.')
    await conn.close()

# from datetime import datetime
# date_inspect = datetime.strptime(date, '%Y-%m-%d')
# time_inspect = datetime.strptime(time, '%H:%M:%S.%f')

# from postgres_tools import upload_PostgreSQL
# db_upload_ped = [module_name, rel_hum, temp_c, bias_vol, chip, channel, channeltype, adc_median, adc_iqr, tot_median, tot_iqr, toa_median, toa_iqr, adc_mean, adc_stdd, tot_mean, tot_stdd, toa_mean, toa_stdd, tot_efficiency, tot_efficiency_error, toa_efficiency, toa_efficiency_error, pad, x, y, date_inspect, time_inspect, inspector, comment]
# await upload_PostgreSQL(table_name = 'module_pedestal_test', db_upload_data = db_upload_ped)

# db_upload_iv = [module_name, rel_hum, temp_c, prog_v, meas_v, meas_i, meas_r, date_inspect, time_inspect, inspector, comment]
# await upload_PostgreSQL(table_name = 'module_iv_test', db_upload_data = db_upload_iv)

# db_upload_hxped = [hxb_name, rel_hum, temp_c, chip, channel, channeltype, adc_median, adc_iqr, tot_median, tot_iqr, toa_median, toa_iqr, adc_mean, adc_stdd, tot_mean, tot_stdd, toa_mean, toa_stdd, tot_efficiency, tot_efficiency_error, toa_efficiency, toa_efficiency_error, pad, x, y, date_inspect, time_inspect, inspector, comment]
# await upload_PostgreSQL(table_name = 'hxb_pedestal_test', db_upload_data = db_upload_hxped)
