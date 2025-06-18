import asyncio, asyncpg, pwinput
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
from lxml import etree
import yaml, os, base64, sys, argparse, traceback, datetime, tzlocal, pytz
from cryptography.fernet import Fernet
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from export_data.define_global_var import LOCATION, INSTITUTION
from export_data.src import *

yaml_file = 'export_data/table_to_xml_var.yaml'

# Define the database tables and their columns to be exported
db_dict = {
    "module_assembly": ["ass_run_date", "ass_time_begin", "temp_c", "rel_hum"],
    "module_pedestal_test": ["date_test", "time_test", "temp_c", "rel_hum"],
    "proto_assembly": ["ass_run_date", "ass_time_begin", "temp_c", "rel_hum"],
    "module_iv_test": ["date_test", "time_test", "temp_c", "rel_hum"]
}

async def fetch_temp_humidity_data(conn, time):
    time_gap = f"ABS(EXTRACT(EPOCH FROM log_timestamp - '{time}'))"
    # query to find the closest timestamp for a given time
    query = f"""
    SELECT log_timestamp, temp_c, rel_hum
    FROM temp_humidity
    WHERE {time_gap} < 600
    ORDER BY {time_gap}
    LIMIT 1;
    """
    return await conn.fetchrow(query)
async def main():
    conn = await asyncpg.connect(
        host=DBHostname,
        database=DBDatabase,
        user=DBUsername,
        password=DBPassword
    )

    # Load the table_to_xml_var.yaml file:
    with open(yaml_file, 'r') as file:
        yaml_data = yaml.safe_load(file)

    env_data = yaml_data['env_data']
    if not env_data:
        print("No data found in YAML file")
        return

    table = "module_iv_test"
    partname = table.split("_")[0] + "_name"

    query = f"""
    SELECT DISTINCT REPLACE({partname}, '-', '') AS partname
    FROM {table}
    """
    results = await conn.fetch(query)
    test_part = results[0]['partname']

    print(f"\nTesting part: {test_part}\n")

    db_values = {}
    # Run the query just once
    try:
        query = f"""
        SELECT {", ".join(db_dict[table])}
        FROM {table}
        WHERE REPLACE({partname}, '-', '') = '{test_part}'
        """
        results = await conn.fetchrow(query)
    except Exception as e:
        print(f"QUERY: {query}")
        print(f"Error: {e}")
        return  # skip this part if query failed

    # Extract date and time
    run_date = results.get(db_dict[table][0], "")
    time_begin = results.get(db_dict[table][1], "")
    time = f"{run_date}T{time_begin}Z"
    temp_humidity_data = await fetch_temp_humidity_data(conn, time)

    # Start populating db_values
    for entry in env_data:
        xml_var = entry['xml_temp_val']

        if xml_var == 'LOCATION':
            db_values[xml_var] = LOCATION

        elif xml_var == 'INSTITUTION':
            db_values[xml_var] = INSTITUTION

        elif xml_var == 'ID':
            db_values[xml_var] = format_part_name(partname)

        elif xml_var == 'KIND_OF_PART':
            db_values[xml_var] = await get_kind_of_part(test_part, table, conn)

        elif xml_var == 'RUN_NUMBER':
            db_values[xml_var] = get_run_num(LOCATION)

        elif xml_var == 'TEMPSENSOR_ID':
            db_values[xml_var] = "NULL"
        elif xml_var == 'COMMENTS_UPLOAD':
            db_values[xml_var] = "NULL"
        elif xml_var == "RUN_BEGIN_TIMESTAMP_":
            db_values[xml_var] = format_datetime(run_date, time_begin)
        elif xml_var == "RUN_END_TIMESTAMP_":
            db_values[xml_var] = format_datetime(run_date, time_begin)
        elif xml_var == "MeasurementTime":
            print(" >>>>  MeasurementTime fetching.....")
            if temp_humidity_data and temp_humidity_data.get("log_timestamp"):
                db_values[xml_var] = format_datetime(temp_humidity_data["log_timestamp"])
            else:
                db_values[xml_var] = format_datetime(run_date, time_begin)

        elif xml_var == "TEMP_C":
            db_values[xml_var] = temp_humidity_data.get("temp_c", "") if temp_humidity_data else results.get("temp_c", "")

        elif xml_var == "HUMIDITY_REL":
            db_values[xml_var] = temp_humidity_data.get("rel_hum", "") if temp_humidity_data else results.get("rel_hum", "")


    # print out the example db_values
    print("\nFinal example db_values:")
    for k, v in db_values.items():
        print(f"{k}: {v}")

    # convert the data into XML
    xml_template_path = './env_upload.xml'
    xml_output_path = f'./{test_part}_env_upload.xml'

    await update_xml_with_db_values(xml_template_path, xml_output_path, db_values)

    await conn.close()

asyncio.run(main())