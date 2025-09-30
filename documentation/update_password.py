import asyncpg, asyncio, pwinput
async def update_passwords():
    ## change HOSTNAME, database name
    mac_dict = {'host': 'HOSTNAME',   'database':'hgcdb', 'password': pwinput.pwinput(prompt='Enter superuser password: ', mask='*'), 'user': 'postgres'}
    conn = await asyncpg.connect(**mac_dict)
    
    user_types = ['shipper', 'ogp_user', 'gantry_user', 'teststand_user', 'wirebond_user', 'encap_user',  'editor']
    new_password = input(f'Enter new user password: ',)
    for u in user_types:
        query = f"""ALTER USER {u} WITH PASSWORD '{new_password}';"""
        result = await conn.execute(query)
        print(u, result)

    await conn.close()

asyncio.run(update_passwords())
