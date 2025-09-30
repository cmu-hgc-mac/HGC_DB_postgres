import asyncpg, asyncio
import argparse, csv, datetime

async def fetch_testing_data(macid, data_type, module_list = None):
    mac_dict = {'CMU' : {'host': 'cmsmac04.phys.cmu.edu',   'database':'hgcdb'}, 
                'UCSB': {'host': 'gut.physics.ucsb.edu',    'database':'hgcdb'}, 
                'TIFR': {'host': 'lxhgcdb02.tifr.res.in',   'database':'hgcdb', 'password': 'hgcal'},
                'IHEP': {'host': 'hgcal.ihep.ac.cn',        'database':'postgres',},
                'NTU' : {'host': 'hep11.phys.ntu.edu.tw',   'database':'hgcdb'}, }
                # 'TTU' : {'host': '129.118.107.198',         'dbname':'ttu_mac_local'},}
    conn = await asyncpg.connect(user='viewer', **mac_dict[macid])
    
    placeholders = ', '.join(f'${i+1}' for i in range(len(module_list)))
    

    query_builder = {'module_name': {
        'module_info' : 'module_no',
        'module_iv_test': 'mod_ivtest_no',
        'module_pedestal_test': 'mod_pedtest_no',
        'module_qc_summary': 'mod_qc_no',
        'module_assembly': 'module_ass',
        'module_inspect': 'module_row_no',
        'front_wirebond': 'frwirebond_no',
        'back_wirebond': 'bkwirebond_no',
        'front_encap': 'frencap_no',
        'back_encap': 'bkencap_no',
    },
                     'proto_name': {
                         'proto_assembly': 'proto_no',
                         'proto_inspect': 'proto_row_no',},
                     'hxb_name': {'hxb_inspect':'hxb_row_no',
                                   'hxb_pedestal_test': 'hxb_pedtest_no',
                                   'hexaboard': 'hxb_no',},
                     'sen_name': {'sensor': 'sen_no'},
                     'bp_name': {'baseplate': 'bp_no',
                                   'bp_inspect': 'bp_row_no',},
                     }
    
    def get_query(table_name, part_type):
        primary_key = query_builder[part_type][table_name]
        module_filter = f"WHERE {part_type} IN ({placeholders})" if module_list[0] != 'ALL' else ""
        query = f"""SELECT * FROM {table_name} {module_filter} ORDER BY {primary_key};"""
    
    # query_mod_iv = f"""SELECT * FROM module_iv_test {module_filter} ORDER BY mod_ivtest_no;"""
    # query_mod_ped = f"""SELECT * FROM module_pedestal_test {module_filter} ORDER BY mod_pedtest_no;"""
    # query_mod_qcs = f"""SELECT * FROM module_qc_summary {module_filter} ORDER BY mod_qc_no;"""

    query_type_dict = {'mod_iv': get_query('module_iv_test', 'module_name'),
                       'mod_ped': get_query('module_ped_test', 'module_name'),
                       'mod_qcs': get_query('module_qc_summary', 'module_name'),
                       'mod_info': get_query('module_info', 'module_name'),
                       }

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
