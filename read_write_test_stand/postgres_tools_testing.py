from io import BytesIO
from PIL import Image, ImageTk
import asyncio, asyncpg

def get_query_read(table_name, part_name = None):
    if table_name == 'module_pedestal_test':
        query = f"""SELECT module_name, rel_hum, temp_c, bias_vol, date_test, time_test, inspector, comment                                                                                                         
            FROM {table_name}                                                                                                                                                                              
            WHERE inspector = 'acrobert'                                                                                                                                                                           
            ORDER BY date_test DESC, time_test DESC LIMIT 10;"""
    elif table_name == 'hxb_pedestal_test':
        query = f"""SELECT hxb_name, rel_hum, temp_c, date_test, time_test, inspector, comment                                                                                                                      
            FROM {table_name}                                                                                                                                                                                 
            WHERE inspector = 'acrobert'                                                                                                                                                                           
            ORDER BY date_test DESC, time_test DESC LIMIT 10;"""
    elif table_name == 'module_iv_test':
        query = f"""SELECT module_name, rel_hum, prog_v, meas_v, meas_i, meas_r, date_test, time_test, inspector, comment                                                                                           
            FROM {table_name}                                                                                                                                                                                    
            WHERE inspector = 'acrobert'                                                                                                                                                                           
            ORDER BY date_test DESC, time_test DESC LIMIT 10;"""
    elif table_name == 'module_pedestal_plots' and part_name is not None:
        query = f"""SELECT adc_mean_hexmap                                                                                           
            FROM {table_name}   
            WHERE module_name = '{part_name}';"""
    elif table_name == 'module_pedestal_plots':
        query = f"""SELECT adc_mean_hexmap                                                                                           
            FROM {table_name}                                                                                                                                                                                
            ORDER BY date_test DESC, time_test DESC LIMIT 10;"""
    else:
        query = None
        print('Table not found. Check argument.')
    return query


async def fetch_PostgreSQL(table_name, part_name = None):
    conn = await asyncpg.connect(
        host='cmsmac04.phys.cmu.edu',
        database='hgcdb',
        user='teststand_user',
        password='hgcal'
    )
    value = await conn.fetch(get_query_read(table_name, part_name))
    await conn.close()
    return value

result = await fetch_PostgreSQL('baseplate')
result = await fetch_PostgreSQL('module_pedestal_test')
for r in result: print(r)


table_name, part_name = '', ''
im = asyncio.run(fetch_PostgreSQL(table_name, part_name))
if im != []:
    image = Image.open(BytesIO(im[0]['adc_mean_hexmap']))
    image.show() ### to show
    image.save("new_image.jpg") ### to save










######################
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
    elif table_name == 'module_pedestal_plots':
        pre_query = f""" 
        INSERT INTO {table_name} 
        (module_name, adc_mean_hexmap, adc_stdd_hexmap, noise_channel_chip0, noise_channel_chip1, noise_channel_chip2, pedestal_channel_chip0, pedestal_channel_chip1, pedestal_channel_chip2, total_noise_chip0, total_noise_chip1, total_noise_chip2, inspector, grade, comment_plot_test) 
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

from io import BytesIO  
import matplotlib.pyplot as plt
### To write, convert im to bytes
def fig_to_bytes(fig):  
    buffer = BytesIO()
    fig.savefig(buffer, format='png', bbox_inches='tight'); plt.close()
    return buffer.getvalue

# from postgres_tools import upload_PostgreSQL
# db_upload_ped = [module_name, rel_hum, temp_c, bias_vol, chip, channel, channeltype, adc_median, adc_iqr, tot_median, tot_iqr, toa_median, toa_iqr, adc_mean, adc_stdd, tot_mean, tot_stdd, toa_mean, toa_stdd, tot_efficiency, tot_efficiency_error, toa_efficiency, toa_efficiency_error, pad, x, y, date_inspect, time_inspect, inspector, comment]
# await upload_PostgreSQL(table_name = 'module_pedestal_test', db_upload_data = db_upload_ped)

# db_upload_iv = [module_name, rel_hum, temp_c, prog_v, meas_v, meas_i, meas_r, date_inspect, time_inspect, inspector, comment]
# await upload_PostgreSQL(table_name = 'module_iv_test', db_upload_data = db_upload_iv)

# db_upload_hxped = [hxb_name, rel_hum, temp_c, chip, channel, channeltype, adc_median, adc_iqr, tot_median, tot_iqr, toa_median, toa_iqr, adc_mean, adc_stdd, tot_mean, tot_stdd, toa_mean, toa_stdd, tot_efficiency, tot_efficiency_error, toa_efficiency, toa_efficiency_error, pad, x, y, date_inspect, time_inspect, inspector, comment]
# await upload_PostgreSQL(table_name = 'hxb_pedestal_test', db_upload_data = db_upload_hxped)

# db_upload_plots = [module_name, adc_mean_hexmap, adc_stdd_hexmap, noise_channel_chip0, noise_channel_chip1, noise_channel_chip2, pedestal_channel_chip0, pedestal_channel_chip1, pedestal_channel_chip2, total_noise_chip0, total_noise_chip1, total_noise_chip2, inspector, grade, comment_plot_test]
# await upload_PostgreSQL(table_name = 'module_pedestal_plot', db_upload_data = db_upload_plots)
