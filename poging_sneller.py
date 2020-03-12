#https://www.jetbrains.com/help/pycharm/installing-uninstalling-and-upgrading-packages.html
#https://api.mongodb.com/python/current/tutorial.html
#https://www.psycopg.org/docs/usage.html

import psycopg2

conn = psycopg2.connect("dbname=data user=postgres password=postgres")
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS same;")

cur.execute("CREATE TABLE same (id serial PRIMARY KEY, "
            "basisproduct varchar, "
            "vergelijkbaar varchar);")

cur.execute("select name from products;")
total = cur.fetchall()

for i in range(0, len(total)):
    verg = []
    print(i, 'next')
    name = str(total[i])
    name = name.replace('(', '')
    name = name.replace(')', '')
    name = name.replace(',', '')

    name = name[1:len(name) - 1]
    if "'" in name:
        name = name.split("'")
        name = name[0] + "''" + name[1]

    name = "'" + name + "'"

    func = "select discount, targetaudience, category, subcategory from products where name = "'{}'";".format(name)
    cur.execute(func)
    allprod = cur.fetchall()

    for d in range(0, len(total)):
        print(d, 'volgend')
        name2 = str(total[d])
        name2 = name2.replace('(', '')
        name2 = name2.replace(')', '')
        name2 = name2.replace(',', '')

        name2 = name2[1:len(name2) - 1]
        if "'" in name2:
            name2 = name2.split("'")
            name2 = name2[0] + "''" + name2[1]
        name2 = "'" + name2 + "'"

        func2 = "select discount, targetaudience, category, subcategory from products where name = "'{}'";".format(name2)
        cur.execute(func2)
        allgerela = cur.fetchall()
        if allprod == allgerela:
            cur.execute("INSERT INTO same (basisproduct, vergelijkbaar) VALUES (%s, %s)", (name, name2))
    break
            # verg.append(name2)

        # cur.execute("INSERT INTO all_p (_ID, vergelijkbaar) VALUES (%s, %s)",
        #     (i['_id'],
        #      i['na'] if 'vergelijkbaar' in i else None,))
#cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
# Pass data to fill a query placeholders and let Psycopg perform
# the correct conversion (no more SQL injections!)



# a = []
# for i in products:
#     if 'brand' in i:
#         if i['brand'] not in a:
#             cur.execute("INSERT INTO same (_ID, brand) VALUES (%s, %s)",
#                         (i['_id'],
#                          i['brand'] if 'brand' in i else None))
#             a.append(i['brand'])
#ID, category, brand, gender, sub category, sub sub category, color, name, price
"""
# Query the database and obtain data as Python objects
cur.execute("SELECT * FROM test;")
cur.fetchone()
(1, 100, "abc'def")
"""
# Make the changes to the database persistent
conn.commit()

# Close communication with the database
cur.close()
conn.close()