import asyncpg, asyncio
import argparse, csv, datetime

async def fetch_testing_data(macid, data_type, module_list = None):
    mac_dict = {'CMU' : {'host': 'cmsmac04.phys.cmu.edu',   'dbname':'hgcdb'}, 
                'UCSB': {'host': 'gut.physics.ucsb.edu',    'dbname':'hgcdb'}, 
                'NTU' : {'host': 'hep11.phys.ntu.edu.tw',   'dbname':'hgcdb'}, }
                # 'TTU' : {'host': '129.118.107.198',         'dbname':'ttu_mac_local'},}
    conn = await asyncpg.connect(
        user='viewer',
        database=mac_dict[macid]['dbname'],
        host= mac_dict[macid]['host'])  
    
    placeholders = ', '.join(f'${i+1}' for i in range(len(module_list)))
    
    module_filter = f"WHERE module_name IN ({placeholders})" if module_list[0] != 'ALL' else ""
    query_mod_iv = f"""SELECT * FROM module_iv_test {module_filter} ORDER BY mod_ivtest_no;"""
    query_mod_ped = f"""SELECT * FROM module_pedestal_test {module_filter} ORDER BY mod_pedtest_no;"""
    query_mod_qcs = f"""SELECT * FROM module_qc_summary {module_filter} ORDER BY mod_qc_no;"""

    query_type_dict = {'mod_iv': query_mod_iv,
                       'mod_ped': query_mod_ped,
                       'mod_qcs': query_mod_qcs,}

    if module_list[0] == 'ALL':
        rows = await conn.fetch(query_type_dict[data_type])
    else:
        rows = await conn.fetch(query_type_dict[data_type], *module_list)
    await conn.close()
    return rows

async def main():
    parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
    parser.add_argument('-dt', '--data_type', default=None, required=True, help="mod_iv, mod_ped, mod_qc")
    # parser.add_argument('-pfp', '--module_filepath', default=None, required=True, help="mod_iv, mod_ped, mod_qc")
    parser.add_argument('-mn', '--module_names', nargs='+', default=None, required=False, help='Module name(s) separated by spaces')
    parser.add_argument('-mac', '--mac', default=None, required=True, help="MAC: CMU, UCSB")
    args = parser.parse_args()
    module_names = ['ALL'] if not args.module_names else [mn.upper() for mn in args.module_names]
    print(f'Fetching {args.data_type} for module(s) {module_names} assembled at {args.mac}  ...')
    rows = await fetch_testing_data(args.mac.upper(), args.data_type, module_list = module_names)
    now = datetime.datetime.now().strftime('%Y-%m-%dT%H%M%S')
    outfilename = f"{args.mac}_{args.data_type}_asof_{now}.csv" if not args.module_names else f"{args.mac}_{args.data_type}_custom_{now}.csv"
    if rows:
        column_names = rows[0].keys()
        with open(outfilename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(column_names)                            # Write headers
            writer.writerows([tuple(row.values()) for row in rows])  # Write data rows
            print(f"Output saved in ./{outfilename} ...")
    else:
        print("No results found.")

asyncio.run(main())
