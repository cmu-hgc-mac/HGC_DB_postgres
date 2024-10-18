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
from HGC_DB_postgres.export.src import get_conn, fetch_from_db, update_xml_with_db_values, get_parts_name, get_kind_of_part

async def process_module(conn, yaml_file, xml_file_path, output_dir):
    # Load the YAML file
    with open(yaml_file, 'r') as file:
        yaml_data = yaml.safe_load(file)

    # Retrieve module data from the YAML file
    module_data = yaml_data['module_assembly']
    # module_data = [item for item in yaml_data if 'module' in item['dbase_table']]
    
    if not module_data:
        print("No 'module' data found in YAML file")
        return

    # get the unique module_name among all tables that contain module_name by taking the union of the tables
    module_ass_table = await get_parts_name('module_name', 'module_assembly', conn)
    module_hxb_table = await get_parts_name('module_name', 'mod_hxb_other_test', conn)
    module_info_table = await get_parts_name('module_name', 'module_info', conn)
    module_inspect_table = await get_parts_name('module_name', 'module_inspect', conn)
    module_ivtest_table = await get_parts_name('module_name', 'module_iv_test', conn)
    module_pedestal_test_table = await get_parts_name('module_name', 'module_pedestal_test', conn)
    module_pedestal_plot_table = await get_parts_name('module_name', 'module_pedestal_plots', conn)
    module_qc_summ_table = await get_parts_name('module_name', 'module_qc_summary', conn)
    module_list = list(set(module_ass_table) | set(module_hxb_table) | set(module_info_table) | set(module_inspect_table) | set(module_ivtest_table) | set(module_pedestal_test_table) | set(module_pedestal_plot_table) | set(module_qc_summ_table))


    # Fetch database values for the XML template variables
    for module in module_list:
        print(f'getting values for {module}...')
        db_values = {}
        for entry in module_data:
            xml_var = entry['xml_temp_val']

            if xml_var in ['LOCATION', 'INSTITUTION']:
                db_values[xml_var] = LOCATION
            elif xml_var == 'KIND_OF_PART':
                db_values[xml_var] = get_kind_of_part(module)
            else:
                dbase_col = entry['dbase_col']
                dbase_table = entry['dbase_table']

                # Skip entries without a database column or table
                if not dbase_col or not dbase_table:
                    continue

                # Ignore nested queries for now
                if entry['nested_query']:
                    query = entry['nested_query'] + f" WHERE module_assembly.module_name = '{module}';"
                    
                else:
                    # Modify the query to get the latest entry
                    query = f"SELECT {dbase_col} FROM {dbase_table} WHERE module_name = '{module}' ORDER BY ass_run_date DESC, ass_time_begin DESC LIMIT 1"

                result = await fetch_from_db(query, conn)  # Use conn directly
                
                if result:
                    if xml_var == "RUN_BEGIN_TIMESTAMP_":
                        # Fetching both ass_run_date and ass_time_begin
                        run_date = result.get("ass_run_date", "")
                        time_begin = result.get("ass_time_begin", "")
                        db_values[xml_var] = f"{run_date}T{time_begin}"
                    elif xml_var == "RUN_END_TIMESTAMP_":
                        # Fetching both ass_run_date and ass_time_end
                        run_date = result.get("ass_run_date", "")
                        time_end = result.get("ass_time_end", "")
                        db_values[xml_var] = f"{run_date}T{time_end}"
                    else:
                        db_values[xml_var] = result.get(dbase_col, '') if not entry['nested_query'] else list(result.values())[0]

        # Update the XML with the database values
        output_file_name = f'{module}_{os.path.basename(xml_file_path)}'
        output_file_path = os.path.join(output_dir, output_file_name)
        await update_xml_with_db_values(xml_file_path, output_file_path, db_values)

async def main():
    # Configuration
    yaml_file = '../../../export/table_to_xml_var.yaml'  # Path to YAML file
    xml_file_path = '../../../export/template_examples/module/assembly_upload.xml'# XML template file path
    output_dir = '../../../export/generated_xml/module'  # Directory to save the updated XML

    # Create PostgreSQL connection
    conn = await get_conn()

    try:
        await process_module(conn, yaml_file, xml_file_path, output_dir)
    finally:
        await conn.close()

# Run the asyncio program
if __name__ == "__main__":
    asyncio.run(main())
