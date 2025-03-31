import asyncpg, asyncio, argparse
from tabulate import tabulate  

async def fetch_unique_counts(month, year, macid):
    mac_dict = {'CMU': {'host': 'cmsmac04.phys.cmu.edu', 'dbname':'hgcdb'}, 'UCBS':{'host': 'gut.physics.ucsb.edu', 'dbname':'hgcdb'}}
    conn = await asyncpg.connect(
        user='viewer',
        database=mac_dict[macid]['dbname'],
        host= mac_dict[macid]['host'])  
    
    query = f"""SELECT geometry, resolution, bp_material, sen_thickness, roc_version, COUNT(*) AS count
    FROM module_info
    WHERE EXTRACT(YEAR FROM assembled) = {year}
      AND EXTRACT(MONTH FROM assembled) IN ({month})
    GROUP BY geometry, resolution, bp_material, sen_thickness, roc_version
    ORDER BY count DESC;"""
    rows = await conn.fetch(query)
    await conn.close()
    return rows

async def main():
    parser = argparse.ArgumentParser(description="A script that modifies a table and requires the -t argument.")
    parser.add_argument('-m', '--month', default=None, required=True, help="Month eg. 6 for June")
    parser.add_argument('-y', '--year', default=None, required=True, help="Year eg. 2025")
    parser.add_argument('-mac', '--mac', default=None, required=True, help="MAC: CMU, UCSB, TTU, IHEP, NTU, TIFR")
    args = parser.parse_args()
    print(f'Modules assembled at {args.mac} during {args.year}/{args.month} --')
    rows = await fetch_unique_counts(args.month, args.year, args.mac)
    if rows:
        headers = ["Geometry", "Resolution", "BP Material", "Sensor Thickness", "ROC Version", "Count"]
        table = [list(row.values()) for row in rows]
        print(tabulate(table, headers=headers, tablefmt="grid"))
    else:
        print("No results found.")

asyncio.run(main())
