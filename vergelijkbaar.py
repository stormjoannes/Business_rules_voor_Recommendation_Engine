#https://www.jetbrains.com/help/pycharm/installing-uninstalling-and-upgrading-packages.html
#https://api.mongodb.com/python/current/tutorial.html
#https://www.psycopg.org/docs/usage.html

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

count = 0
for i in range(0, len(total)):
    count += 1
    verg = []
    name = str(total[i][0])
    print(name)
    if "'" in name:
        name = name.replace("'", "''")
    name = "'" + name + "'"

    func = "select discount, targetaudience, category, subcategory from products where name = "'{}'";".format(name)
    cur.execute(func)
    allprod = cur.fetchall()

    func = "select discount, targetaudience, category, subcategory from products where name = "'{}'";".format(name)
    cur.execute(func)
    allprod = cur.fetchall()
    allprod = allprod[0]
    uitsmallen = "select id from products where subcategory = "'{}'"".format("'{}'".format(allprod[3]))
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

    for last in verg:
        cur.execute("INSERT INTO same (product, vergelijkbaar) VALUES (%s, %s)", (name, last))
    print(i)
    if count >= 1000:
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