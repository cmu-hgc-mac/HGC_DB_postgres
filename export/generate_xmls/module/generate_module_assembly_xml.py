import asyncio
import asyncpg
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
from lxml import etree
import yaml
import sys
import os
import pwinput

async def get_conn():
    loc = '../../../dbase_info/'
    yaml_file = f'{loc}tables.yaml'
    db_params = {
        'database': yaml.safe_load(open(yaml_file, 'r'))['dbname'],
        'user': 'postgres',
        'password': pwinput.pwinput(prompt='Enter superuser password: ', mask='*'),
        'host': yaml.safe_load(open(yaml_file, 'r'))['db_hostname']}   
    conn = await asyncpg.connect(**db_params)
    return conn

async def fetch_from_db(query, conn):
    """Helper function to fetch data from the PostgreSQL database using a single connection."""
    result = await conn.fetchrow(query)
    return dict(result) if result else {}

async def update_xml_with_db_values(xml_file_path, output_file_path, db_values):
    """Update XML template with values from the database."""
    # Parse the XML file
    tree = etree.parse(xml_file_path)
    root = tree.getroot()

    # Convert db_values keys to lowercase for case-insensitive matching
    db_values_lower = {k.lower(): v for k, v in db_values.items()}

    # Iterate through the db_values and replace corresponding placeholders in XML
    for xml_var, value in db_values_lower.items():
        # XPath to find elements containing the placeholder (lowercase comparison)
        elements = root.xpath(f".//*[contains(text(), '{{{{ {xml_var} }}}}')]")

        if elements:
            for element in elements:
                # Replace the placeholder with the actual value, or empty string if None
                if value is None:
                    value = ""  # Default to an empty string for None values

                # Replace the placeholder text
                element.text = element.text.replace(f"{{{{ {xml_var} }}}}", str(value))

    # Handle the 'ID' placeholder separately (case-sensitive)
    if 'ID' in db_values:
        id_value = db_values['ID']
        id_elements = root.xpath(".//*[contains(text(), '{{ ID }}')]")
        if id_elements:
            for element in id_elements:
                if id_value is None:
                    id_value = ""
                element.text = element.text.replace("{{ ID }}", str(id_value))

    # Check if the directory to store outputted xml file exists
    output_dir_path = os.path.dirname(output_file_path)
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    
    # save the file to the directory
    if not os.path.isdir(output_file_path):
        tree.write(output_file_path, pretty_print=True, xml_declaration=True, encoding='UTF-8')
        print(f"XML file updated and saved to: {output_file_path}")
    else:
        print(f"Error: {output_file_path} is a directory, not a file.")

async def get_parts_name(name, table, conn):
    ##  returns part name in a specific table
    ##  i.e., baseplate-> get bp_name
    query = f"SELECT DISTINCT {name} FROM {table};"
    print(query)
    fetched_query = await conn.fetch(query)
    name_list = [record[name] for record in fetched_query]
    return name_list

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

            if 'default_value' in entry:
                db_values[xml_var] = entry['default_value']
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
    output_dir = '../../../export/generated_xml'  # Directory to save the updated XML

    # Create PostgreSQL connection
    conn = await get_conn()

    try:
        await process_module(conn, yaml_file, xml_file_path, output_dir)
    finally:
        await conn.close()

# Run the asyncio program
if __name__ == "__main__":
    asyncio.run(main())
