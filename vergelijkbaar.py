import psycopg2
import random

conn = psycopg2.connect("dbname=data user=postgres password=postgres")
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS same;")

cur.execute("CREATE TABLE same (id serial PRIMARY KEY, "
            "product varchar, "
            "vergelijkbaar varchar);")

cur.execute("select name from products;")
total = cur.fetchall()

def teller(filter, name):
    minimum = []
    for i in range(0, len(filter)):
        if name[i] == None:
            uitvoeren = f"select id from products where {filter[i]} = null;"
        else:
            naam = name[i]
            if "'" in str(naam):
                print('ja hier')
                naam = name[i].replace("'", "''")
            uitvoeren = f"select id from products where {filter[i]} = "'{}'"".format("'{}'".format(naam))
        print(uitvoeren, 'uitvoeren')
        cur.execute(uitvoeren)
        aantal = cur.fetchall()
        minimum.append(len(aantal))
    x = min(minimum)
    for y in range(0, len(minimum)):
        if minimum[y] == x:
            min_filt = filter[y]
            return min_filt, y

count = 0
for i in range(0, len(total)):
    filter = ['discount', 'targetaudience', 'category', 'subcategory']
    filt = ''
    for a in range(0, len(filter)):
        if a != len(filter) - 1:
            filt += filter[a] + ", "
        else:
            filt += filter[a]
    count += 1
    verg = []
    name = str(total[i][0])
    print(name)
    if "'" in name:
        name = name.replace("'", "''")
    name = "'" + name + "'"
    func = f"select {filt} from products where name = "'{}'";".format(name)
    cur.execute(func)
    allprod = cur.fetchall()
    allprod = allprod[0]
    uitk = teller(filter, allprod)

    koek = allprod[uitk[1]]
    if str(koek) == None:
        uitsmallen = f"select id from products where {uitk[0]} = null;"
    else:
        uitsmallen = f"select id from products where {uitk[0]} = "'{}'"".format("'{}'".format(koek.replace("'", '')))
    print(uitsmallen, 'uitsmallenn')
    cur.execute(uitsmallen)
    versmald = cur.fetchall()

    for d in range(0, len(versmald)):
        if name.replace("'", '') in verg:
            verg.remove(name.replace("'", ''))
        if len(verg) < 4:
            name2 = str(versmald[d][0])
            if "'" in name2:
                name2 = name2.split("'")
                name2 = name2[0] + "''" + name2[1]
            name2 = "'" + name2 + "'"

            func2 = "select discount, targetaudience, category, subcategory from products where id = "'{}'";".format(name2)
            cur.execute(func2)
            allgerela = cur.fetchall()
            if allprod == allgerela[0]:
                verg.append(name2.replace("'", ''))
        else:
            break

    print(verg, 'verg')
    for last in verg:
        cur.execute("INSERT INTO same (product, vergelijkbaar) VALUES (%s, %s)", (name, last))
    if count >= 10:
        break

conn.commit()

cur.execute("select vergelijkbaar from same;")
allrecommended = cur.fetchall()
recommendation = []
while len(recommendation) < 3:
    rand_keuze = random.choice(allrecommended)
    if rand_keuze not in recommendation:
        recommendation.append(rand_keuze)
print(recommendation)

# Close communication with the database
cur.close()
conn.close()