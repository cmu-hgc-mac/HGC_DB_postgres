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

    # Retrieve wirebond data from the YAML file
    wb_data = yaml_data['wirebond']
    
    if not wb_data:
        print("No wirebond data found in YAML file")
        return

    module_ass_table = await get_parts_name('module_name', 'module_assembly', conn)
    module_hxb_table = await get_parts_name('module_name', 'mod_hxb_other_test', conn)
    module_info_table = await get_parts_name('module_name', 'module_info', conn)
    module_inspect_table = await get_parts_name('module_name', 'module_inspect', conn)
    module_ivtest_table = await get_parts_name('module_name', 'module_iv_test', conn)
    module_pedestal_test_table = await get_parts_name('module_name', 'module_pedestal_test', conn)
    module_pedestal_plot_table = await get_parts_name('module_name', 'module_pedestal_plots', conn)
    module_qc_summ_table = await get_parts_name('module_name', 'module_qc_summary', conn)
    module_list = list(set(module_ass_table) | set(module_hxb_table) | set(module_info_table) | set(module_inspect_table) | set(module_ivtest_table) | set(module_pedestal_test_table) | set(module_pedestal_plot_table) | set(module_qc_summ_table))

    for module in module_list:
        # Fetch database values for the XML template variables
        print(f'getting values for {module}...')
        db_values = {}

        for entry in wb_data:
            xml_var = entry['xml_temp_val']

            if xml_var in ['LOCATION', 'INSTITUTION']:
                db_values[xml_var] = LOCATION
            elif xml_var == 'KIND_OF_PART':
                db_values[xml_var] = get_kind_of_part(module)
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
                            WHERE module_name = '{module}'
                            AND xml_gen_datetime IS NULL
                            ORDER BY date_bond DESC, time_bond DESC
                            LIMIT 1
                        )
                        UNION ALL
                        (
                            SELECT comment AS front_wirebond_comment
                            FROM front_wirebond
                            WHERE module_name = '{module}'
                            AND xml_gen_datetime IS NULL
                            ORDER BY date_bond DESC, time_bond DESC
                            LIMIT 1
                        );
                        """
                    else:
                        query = entry['nested_query'] + f" WHERE {dbase_table}.module_name = '{module}';"
                    
                    # print(f'Executing query: {query}')

                else:
                    # Modify the query to get the latest entry
                    if dbase_table in ['back_encap', 'front_encap']:
                        query = f"""
                        SELECT {dbase_col} FROM {dbase_table}
                        WHERE module_name = '{module}'
                        AND xml_gen_datetime IS NULL
                        ORDER BY date_encap DESC, time_encap DESC
                        LIMIT 1;
                        """
                    elif dbase_table == 'module_info':
                        query = f"""
                        SELECT {dbase_col} FROM {dbase_table}
                        WHERE module_name = '{module}'
                        AND xml_gen_datetime IS NULL;
                        """
                    else:
                        query = f"""
                        SELECT {dbase_col} FROM {dbase_table} 
                        WHERE module_name = '{module}'
                        AND xml_gen_datetime IS NULL
                        ORDER BY date_bond DESC, time_bond DESC LIMIT 1;
                        """
                # print(f'Executing query -- \n\t{query}')
                results = await fetch_from_db(query, conn)  # Use conn directly

                if results:
                    if xml_var == "RUN_BEGIN_TIMESTAMP_":
                        # Fetching both ass_run_date and ass_time_begin
                        run_date = results.get("date_bond", "")
                        time_begin = results.get("time_bond", "")
                        db_values[xml_var] = f"{run_date}T{time_begin}"
                    elif xml_var == "RUN_NAME_TIME_STAMP":
                        run_date = results.get("date_bond", "")
                        time_end = results.get("time_bond", "")
                        db_values[xml_var] = f"{run_date}T{time_end}"
                    elif xml_var == "WIREBOND_COMMENTS_CONCAT":
                        bk_comment = results.get("back_wirebond_comment", "")
                        fr_comment = results.get("front_wirebond_comment", "")
                        db_values[xml_var] = f"{bk_comment}-{fr_comment}"
                    elif xml_var == "ENCAPSULATION_COMMENTS_CONCAT":
                        bk_comment = results.get("back_encap_comment", "")
                        fr_comment = results.get("front_encap_comment", "")
                        db_values[xml_var] = f"{bk_comment}-{fr_comment}"
                    else:
                        db_values[xml_var] = results.get(dbase_col, '') if not entry['nested_query'] else list(results.values())[0]

        output_file_name = f'{module}_{os.path.basename(xml_file_path)}'
        output_file_path = os.path.join(output_dir, output_file_name)
        await update_xml_with_db_values(xml_file_path, output_file_path, db_values)

async def main():
    # Configuration
    yaml_file = 'table_to_xml_var.yaml'  # Path to YAML file
    xml_file_path = 'template_examples/module/wirebond_upload.xml'# XML template file path
    output_dir = 'generated_xml/module'  # Directory to save the updated XML

    # Create PostgreSQL connection pool
    conn = await get_conn()

    try:
        await process_module(conn, yaml_file, xml_file_path, output_dir)
    finally:
        await conn.close()

# Run the asyncio program
if __name__ == "__main__":
    asyncio.run(main())
