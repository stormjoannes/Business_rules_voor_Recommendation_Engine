#https://www.jetbrains.com/help/pycharm/installing-uninstalling-and-upgrading-packages.html
#https://api.mongodb.com/python/current/tutorial.html
#https://www.psycopg.org/docs/usage.html

import psycopg2
import random

conn = psycopg2.connect("dbname=data user=postgres password=postgres")
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS same;")

cur.execute("CREATE TABLE same (id serial PRIMARY KEY, "
            "basisproduct varchar, "
            "vergelijkbaar varchar);")

cur.execute("select name from products;")
total = cur.fetchall()

count = 0

for i in range(0, len(total)):
    count += 1
    verg = []
    name = str(total[i][0])
    if "'" in name:
        name = name.split("'")
        name = name[0] + "''" + name[1]
    name = "'" + name + "'"
    print(name)

    func = "select discount, targetaudience, category, subcategory from products where name = "'{}'";".format(name)
    cur.execute(func)
    allprod = cur.fetchall()

    for d in range(0, len(total)):
        if name.replace("'", '') in verg:
            verg.remove(name.replace("'", ''))
        if len(verg) < 3:
            name2 = str(total[d][0])
            if "'" in name2:
                name2 = name2.split("'")
                name2 = name2[0] + "''" + name2[1]
            name2 = "'" + name2 + "'"

            func2 = "select discount, targetaudience, category, subcategory from products where name = "'{}'";".format(name2)
            cur.execute(func2)
            allgerela = cur.fetchall()
            if allprod == allgerela:
                verg.append(name2.replace("'", ''))
        else:
            break

    for last in verg:
        cur.execute("INSERT INTO same (basisproduct, vergelijkbaar) VALUES (%s, %s)", (name, last))
    if count >= 281:
        break
    print(verg)
            # verg.append(name2)

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