import asyncio, asyncpg, ast
from datetime import datetime
import glob, os, csv, pickle, yaml, argparse, base64, traceback, json
from queries import hxb_ped_query, mod_ped_query, mod_iv_query, mod_simple_query
from get_macs_data import get_macs_data, fetch_postgres_data

query_dict = {
    "hxb_ped_query": hxb_ped_query,
    "mod_ped_query": mod_ped_query,
    "mod_iv_query": mod_iv_query,
    "mod_simple_query": mod_simple_query,
    # "mod_sum_query": mod_sum_query,
}

parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
parser.add_argument('--debug', action='store_true', help="Print errors and list available queries.")
parser.add_argument('-q', '--query', default="mod_simple_query", required=False, help="Query to run. See mac_public/queries.py.")
parser.add_argument('-of', '--output_format', default="csv", required=False, help="Outfile format: csv, pkl, json in mac_public/output")
args = parser.parse_args()

if args.debug:
    print('#'*100)
    print("Available queries are", list(query_dict.keys()))
    print("Add query to mac_public/get_macs_data.py and then list in query_dict in get_macs_data.py.")
    print('#'*100)

data_list = get_macs_data(query = query_dict[args.query], macs_conn_file = os.path.join('mac_public', 'macs_db_conn.yaml'))

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
outfilename = f"{args.query}_all_macs_{timestamp}"
out_dir = "mac_public/output"

if args.output_format == 'csv':
    fname = os.path.join(out_dir,f"{outfilename}.csv")
    with open(fname , "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data_list[0].keys())
        writer.writeheader()  
        writer.writerows(data_list)

elif args.output_format == 'pkl':
    fname = os.path.join(out_dir,f"{outfilename}.pkl")
    with open( fname), "wb" as f:
        pickle.dump(data_list, f)

elif args.output_format == 'json':
    fname = os.path.join(out_dir,f"{outfilename}.json")
    with open(fname, "w") as f:
        json.dump(data_list, f, indent=4)  

print("Data saved to", fname)


