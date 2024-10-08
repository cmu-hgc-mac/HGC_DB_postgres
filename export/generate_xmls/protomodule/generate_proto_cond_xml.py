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
    '''
    Does: get connection to database
    Return: connection
    '''

    loc = '../dbase_info/'
    yaml_file = f'{loc}tables.yaml'
    db_params = {
        'database': yaml.safe_load(open(yaml_file, 'r'))['dbname'],
        'user': 'postgres',
        'password': pwinput.pwinput(prompt='Enter superuser password: ', mask='*'),
        'host': yaml.safe_load(open(yaml_file, 'r'))['db_hostname']}   
    conn = await asyncpg.connect(**db_params)
    return conn

async def fetch_from_db(query, conn):
    '''
    params: sql query, connection
    return: {[db_col]:[retreived value from table]}
    '''
    result = await conn.fetchrow(query) 
    return dict(result) if result else {}  # Convert the row to a dictionary if it exists


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

    # Save the updated XML to the output directory
    tree.write(output_file_path, pretty_print=True, xml_declaration=True, encoding='UTF-8')
    print(f"XML file updated and saved to: {output_file_path}")

async def process_module(conn, yaml_file, xml_file_path, output_dir):
    # Load the YAML file
    with open(yaml_file, 'r') as file:
        yaml_data = yaml.safe_load(file)

    # Retrieve wirebond data from the YAML file
    wb_data = yaml_data['proto_cond']
    
    if not wb_data:
        print("No wirebond data found in YAML file")
        return

    # Construct the output file path
    # output_file_path = os.path.join(output_dir, os.path.basename(xml_file_path))
    proto_names_query = "SELECT DISTINCT proto_name FROM proto_assembly;"
    protos = await conn.fetch(proto_names_query)
    proto_list = [record['proto_name'] for record in protos]

    # Fetch database values for the XML template variables
    for proto_name in proto_list:
        # Fetch database values for the XML template variables
        print(f'getting values for {proto_name}...')
        db_values = {}

        for entry in wb_data:
            xml_var = entry['xml_temp_val']

            if 'default_value' in entry:
                db_values[xml_var] = entry['default_value']
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
                print(f'Executing query -- \n\t{query}')
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
                    elif xml_var == "KIND_OF_PART":
                        sen_thickness = results.get("sen_thickness", "")
                        resolution = results.get("resolution", "")
                        geometry = results.get("geometry", "")
                        db_values[xml_var] = f"{sen_thickness}_{resolution}_{geometry}"
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

async def main():
    # Configuration
    yaml_file = '../export/table_to_xml_var.yaml'  # Path to YAML file
    xml_file_path = '../export/template_examples/protomodule/cond_upload.xml'# XML template file path
    output_dir = '../export/generated_xml/protomodule'  # Directory to save the updated XML

    # Create PostgreSQL connection pool
    conn = await get_conn()

    try:
        await process_module(conn, yaml_file, xml_file_path, output_dir)
    finally:
        await conn.close()

# Run the asyncio program
if __name__ == "__main__":
    asyncio.run(main())
