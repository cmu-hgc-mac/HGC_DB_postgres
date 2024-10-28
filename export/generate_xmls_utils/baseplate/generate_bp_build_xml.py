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
    wb_data = yaml_data['bp_build']
    if not wb_data:
        print("No wirebond data found in YAML file")
        return
    # get the unique database tables that are directly associated with the xml creation for updating timestamp cols
    db_tables = ['baseplate']

    bp_tables = ['baseplate', 'bp_inspect']
    _bp_list = []
    for bp_table in bp_tables:
        _bp_list.extend(await get_parts_name('bp_name', bp_table, conn))
    bp_list = list(set(_bp_list))

    for bp_name in bp_list:
        # Fetch database values for the XML template variables
        print(f'getting values for {bp_name}...')
        db_values = {}

        for entry in wb_data:
            xml_var = entry['xml_temp_val']

            if xml_var in ['LOCATION', 'INSTITUTION']:
                db_values[xml_var] = LOCATION
            elif xml_var == 'KIND_OF_PART':
                db_values[xml_var] = get_kind_of_part(bp_name)
            else:
                dbase_col = entry['dbase_col']
                dbase_table = entry['dbase_table']

                # Skip entries without a database column or table
                if not dbase_col and not dbase_table:
                    continue

                # Ignore nested queries for now
                if entry['nested_query']:
                    if dbase_col == 'comment':
                        query = f"""
                        (
                            SELECT comment AS back_wirebond_comment
                            FROM back_wirebond
                            WHERE module_name = '{bp_name}'
                            AND xml_gen_datetime IS NULL
                            ORDER BY bp_received DESC
                            LIMIT 1
                        )
                        UNION ALL
                        (
                            SELECT comment AS front_wirebond_comment
                            FROM front_wirebond
                            WHERE module_name = '{bp_name}'
                            AND xml_gen_datetime IS NULL
                            ORDER BY bp_received DESC
                            LIMIT 1
                        );
                        """
                    else:
                        query = entry['nested_query'] + f" WHERE {dbase_table}.bp_name = '{bp_name}';"
                    
                    # print(f'Executing query: {query}')

                else:
                    # Modify the query to get the latest entry
                    if dbase_table in ['module_info']:
                        query = f"""
                        SELECT {dbase_col} FROM {dbase_table}
                        WHERE module_name = '{bp_name}'
                        AND xml_gen_datetime IS NULL
                        LIMIT 1;
                        """
                    else:
                        query = f"""
                        SELECT {dbase_col} FROM {dbase_table} 
                        WHERE bp_name = '{bp_name}'
                        AND xml_gen_datetime IS NULL
                        ORDER BY bp_received DESC LIMIT 1;
                        """
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

        output_file_name = f'{bp_name}_{os.path.basename(xml_file_path)}'
        output_file_path = os.path.join(output_dir, output_file_name)
        await update_xml_with_db_values(xml_file_path, output_file_path, db_values)
        await update_timestamp_col(conn,
                                   update_flag=True,
                                   table_list=db_tables,
                                   column_name='xml_gen_datetime',
                                   part='baseplate',
                                   part_name=bp_name)

async def main(output_dir):
    # Configuration
    yaml_file = 'table_to_xml_var.yaml'  # Path to YAML file
    xml_file_path = 'template_examples/baseplate/build_upload.xml'# XML template file path
    xml_output_dir = output_dir + '/baseplate'  # Directory to save the updated XML


    # Create PostgreSQL connection pool
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
