import psycopg2
conn = psycopg2.connect(
    host="localhost",
    database="assembly",
    user="postgres",
    password="hgcal"
)

cursor = conn.cursor()

nameF='/Users/sindhu/My Drive/Semesters/Spring-2023/database/LD_V3_epoxy_dummy_2_circles_XY_graphic_standard.txt'


with open(nameF, 'r') as file:
    for i, line in enumerate(file):
        if (i <= 5): continue;         
        if 'End!' in line: break;            
    
        values = line.strip().split(';')
        values.append( (nameF.split('/')[-1]).split('.txt')[0]  )
        for j, entry in enumerate(values):
            if (j >=3 and j<= 7):
                values[j] = float(values[j])
        values[8] = 0.0

        query = 'INSERT INTO frontside_inspection (feature_type, feature_name, value, actual, nominal, deviation, neg_tolerance, pos_tolerance, out_of_tolerance, comment, id_module) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        cursor.execute(query, tuple(values))



conn.commit()
cursor.close()
conn.close()
print('Entries inserted!')