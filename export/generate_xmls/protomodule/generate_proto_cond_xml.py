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

    # Retrieve wirebond data from the YAML file
    wb_data = yaml_data['proto_cond']
    if not wb_data:
        print("No wirebond data found in YAML file")
        return
    db_tables = ['proto_assembly', 'proto_inspect']

    proto_tables = ['proto_assembly', 'proto_inspect']
    _proto_list = []
    for proto_table in proto_tables:
        _proto_list.extend(await get_parts_name('proto_name', proto_table, conn))
    proto_list = list(set(_proto_list))
    
    # Fetch database values for the XML template variables
    for proto_name in proto_list:
        # Fetch database values for the XML template variables
        print(f'getting values for {proto_name}...')
        db_values = {}

        for entry in wb_data:
            xml_var = entry['xml_temp_val']

            if xml_var in ['LOCATION', 'INSTITUTION']:
                db_values[xml_var] = LOCATION
            elif xml_var == 'KIND_OF_PART':
                db_values[xml_var] = get_kind_of_part(proto_name)
            else:
                dbase_col = entry['dbase_col']
                dbase_table = entry['dbase_table']

                # Skip entries without a database column or table
                if not dbase_col and not dbase_table:
                    continue

                # Ignore nested queries for now
                if entry['nested_query']:
                    query = entry['nested_query'] + f" WHERE {dbase_table}.proto_name = '{proto_name}';"

                else:
                    # Modify the query to get the latest entry
                    if dbase_table in ['proto_assembly']:
                        query = f"""
                        SELECT {dbase_col} FROM {dbase_table}
                        WHERE proto_name = '{proto_name}'
                        AND xml_gen_datetime IS NULL
                        LIMIT 1;
                        """
                    else:
                        query = f"""
                        SELECT {dbase_col} FROM {dbase_table} 
                        WHERE proto_name = '{proto_name}'
                        AND xml_gen_datetime IS NULL
                        ORDER BY date_inspect DESC, time_inspect DESC LIMIT 1;
                        """
                # print(f'Executing query -- \n\t{query}')
                results = await fetch_from_db(query, conn)  # Use conn directly

                if results:
                    if xml_var == "RUN_BEGIN_TIMESTAMP_":
                        # Fetching both ass_run_date and ass_time_begin
                        run_date = results.get("ass_run_date", "")
                        time_begin = results.get("ass_time_begin", "")
                        db_values[xml_var] = f"{run_date}T{time_begin}"
                    elif xml_var == "RUN_END_TIMESTAMP_":
                        run_date = results.get("ass_run_date", "")
                        time_end = results.get("ass_time_end", "")
                        db_values[xml_var] = f"{run_date}T{time_end}"
                    elif xml_var == "CURE_BEGIN_TIMESTAMP_":
                        run_date = results.get("ass_run_date", "")
                        time_end = results.get("ass_time_begin", "")
                        db_values[xml_var] = f"{run_date}T{time_end}"
                    elif xml_var == "CURE_END_TIMESTAMP_":
                        run_date = results.get("cure_date_end", "")
                        time_end = results.get("cure_time_end", "")
                        db_values[xml_var] = f"{run_date}T{time_end}"
                    else:
                        db_values[xml_var] = results.get(dbase_col, '') if not entry['nested_query'] else list(results.values())[0]

        output_file_name = f'{proto_name}_{os.path.basename(xml_file_path)}'
        output_file_path = os.path.join(output_dir, output_file_name)
        await update_xml_with_db_values(xml_file_path, output_file_path, db_values)
        await update_timestamp_col(conn,
                                   update_flag=True,
                                   table_list=db_tables,
                                   column_name='xml_gen_datetime',
                                   part='protomodule',
                                   part_name=proto_name)

async def main():
    # Configuration
    yaml_file = 'table_to_xml_var.yaml'  # Path to YAML file
    xml_file_path = 'template_examples/protomodule/cond_upload.xml'# XML template file path
    output_dir = 'generated_xml/protomodule'  # Directory to save the updated XML

    # Create PostgreSQL connection pool
    conn = await get_conn()

    try:
        await process_module(conn, yaml_file, xml_file_path, output_dir)
    finally:
        await conn.close()

# Run the asyncio program
if __name__ == "__main__":
    asyncio.run(main())
