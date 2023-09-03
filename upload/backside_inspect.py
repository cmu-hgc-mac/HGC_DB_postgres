import psycopg2
conn = psycopg2.connect(
    host="localhost",
    database="assembly",
    user="postgres",
    password="hgcal"
)

cursor = conn.cursor()

fname='/Users/sindhu/My Drive/Semesters/Spring-2023/database/baseplate_V6.txt'

with open(fname, 'r') as file:
    for i, line in enumerate(file):
        if (i <= 6): continue;         
        if 'End!' in line: break;            
    
        values = line.strip().split(';')
        values.append( (fname.split('/')[-1]).split('.txt')[0]  )

        query = """INSERT INTO new_backside_baseplate (feature_type, feature_name, value, actual, nominal, deviation, neg_tolerance, pos_tolerance, out_of_tolerance, comment, id_baseplate) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        cursor.execute(query, tuple(values))



conn.commit()
cursor.close()
conn.close()
print('Entries inserted!')