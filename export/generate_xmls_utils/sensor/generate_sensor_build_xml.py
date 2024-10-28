import asyncio
import asyncpg
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
from lxml import etree
import yaml
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..')))
import pwinput
from HGC_DB_postgres.export.define_global_var import LOCATION
from HGC_DB_postgres.export.src import get_conn, fetch_from_db, update_xml_with_db_values, get_parts_name, get_kind_of_part, update_timestamp_col


async def process_module(conn, yaml_file, xml_file_path, output_dir):
    # Load the YAML file
    with open(yaml_file, 'r') as file:
        yaml_data = yaml.safe_load(file)

    # Retrieve module data from the YAML file
    module_data = yaml_data['sensor_build']
    # module_data = [item for item in yaml_data if 'module' in item['dbase_table']]
    
    if not module_data:
        print("No 'module' data found in YAML file")
        return
    db_tables = ['sensor']
    sensor_tables = ['sensor']
    _sensor_list = []
    for sensor_table in sensor_tables:
        _sensor_list.extend(await get_parts_name('sen_name', sensor_table, conn))
    sensor_list = list(set(_sensor_list))

    # Fetch database values for the XML template variables
    for sen_name in sensor_list:
        print(f'getting values for {sen_name}...')
        db_values = {}
        for entry in module_data:
            xml_var = entry['xml_temp_val']

            if xml_var in ['LOCATION', 'INSTITUTION']:
                db_values[xml_var] = LOCATION
            elif xml_var == 'KIND_OF_PART':
                db_values[xml_var] = get_kind_of_part(sen_name)
            else:
                dbase_col = entry['dbase_col']
                dbase_table = entry['dbase_table']

                # Skip entries without a database column or table
                if not dbase_col or not dbase_table:
                    continue

                # Ignore nested queries for now
                if entry['nested_query']:
                    query = entry['nested_query'] + f" WHERE sensor.sen_name = '{sen_name}';"
                    
                else:
                    # Modify the query to get the latest entry
                    query = f"SELECT {dbase_col} FROM {dbase_table} WHERE sen_name = '{sen_name}' ORDER BY sen_received DESC, sen_received DESC LIMIT 1"
                results = await fetch_from_db(query, conn)  # Use conn directly
                
                if results:
                    if xml_var == "RUN_BEGIN_TIMESTAMP_":
                        # Fetching both ass_run_date and ass_time_begin
                        run_date = results.get("ass_run_date", "")
                        time_begin = results.get("ass_time_begin", "")
                        db_values[xml_var] = f"{run_date}T{time_begin}"
                    elif xml_var == "RUN_END_TIMESTAMP_":
                        # Fetching both ass_run_date and ass_time_end
                        run_date = results.get("ass_run_date", "")
                        time_end = results.get("ass_time_end", "")
                        db_values[xml_var] = f"{run_date}T{time_end}"
                    else:
                        db_values[xml_var] = results.get(dbase_col, '') if not entry['nested_query'] else list(results.values())[0]

        # Update the XML with the database values
        output_file_name = f'{sen_name}_{os.path.basename(xml_file_path)}'
        output_file_path = os.path.join(output_dir, output_file_name)
        await update_xml_with_db_values(xml_file_path, output_file_path, db_values)
        await update_timestamp_col(conn,
                                   update_flag=True,
                                   table_list=db_tables,
                                   column_name='xml_gen_datetime',
                                   part='sensor',
                                   part_name=sen_name)
async def main(output_dir):
    # Configuration
    yaml_file = 'table_to_xml_var.yaml'  # Path to YAML file
    xml_file_path = 'template_examples/sensor/build_upload.xml'# XML template file path
    xml_output_dir = output_dir + '/sensor'  # Directory to save the updated XML

    # Create PostgreSQL connection
    conn = await get_conn()

    try:
        await process_module(conn, yaml_file, xml_file_path, xml_output_dir)
    finally:
        await conn.close()

# Run the asyncio program
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: script_b.py <output_dir>")
        sys.exit(1)
    
    output_dir = sys.argv[1]
    asyncio.run(main(output_dir))
