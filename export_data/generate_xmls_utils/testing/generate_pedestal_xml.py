import asyncio
import asyncpg
import numpy as np
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from export_data.src import *

async def fetch_test_data(conn):
    # Retrieve the first row of chip and channel arrays
    query = """
        SELECT chip, channel, adc_mean, adc_stdd, channeltype, module_name FROM module_pedestal_test LIMIT 1;
    """
    row = await conn.fetchrow(query)
    
    if row is None:
        raise ValueError("No data found in pedestal_table.")

    chip = row['chip']
    channel = row['channel']
    channel_type = row['channeltype']
    adc_mean = row['adc_mean']
    adc_stdd = row['adc_stdd']
    module_name = row['module_name']

    # Check if the lengths match
    if len(chip) != len(channel):
        raise ValueError("chip and channel arrays must have the same length.")

    # Create test_data as list of [chip, channel, channel_type, adc_mean, adc_stdd] pairs
    test_data = np.array(list(zip(chip, channel, channel_type, adc_mean, adc_stdd)))## DO NOT CHANGE THE ORDER!!!!!
    
    return module_name, test_data

def remap_channels(data):
    updated_data = data.copy()
    for i in range(len(updated_data)):
        channel = updated_data[i, 1]
        channel_type = updated_data[i, 2]
        
        if channel_type == 1:
            updated_data[i, 1] += 80
        elif channel_type == 100:
            updated_data[i, 1] += 90
    return updated_data

def check_duplicate_combo(data):
    skim_data = data[:, :2]## takes only chip and channel
    unq, count = np.unique(skim_data, axis=0, return_counts=True)
    duplicates = unq[count > 1]
    print(f'the followings are duplicated pairs of [chip, channel]\n{duplicates}')
    return None

async def main():
    conn = await get_conn(dbpassword='hgcal')

    try:
        module_name, data = await fetch_test_data(conn)
        test_data = remap_channels(data)
    
        print(f'module -- {module_name}')
        # print(f'test_data shape -- {test_data.shape} as {type(test_data.shape)}')
        # print(test_data[:, 1:3])
        check_duplicate_combo(test_data)
        # print(remap_channels(test_data))
        
    finally:
        await conn.close()

# Run if this is the main script
if __name__ == "__main__":
    asyncio.run(main())
