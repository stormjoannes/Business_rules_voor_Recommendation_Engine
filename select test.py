#https://www.jetbrains.com/help/pycharm/installing-uninstalling-and-upgrading-packages.html
#https://api.mongodb.com/python/current/tutorial.html
#https://www.psycopg.org/docs/usage.html

import psycopg2

conn = psycopg2.connect("dbname=data user=postgres password=postgres")
cur = conn.cursor()
cur.execute("select name from products;")
total = cur.fetchall()
for i in range(0, len(total)):
    g = str(total[i])
    g = g.replace('(', '')
    g = g.replace(')', '')
    g = g.replace(',', '')
    print(g)



#cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
# Pass data to fill a query placeholders and let Psycopg perform
# the correct conversion (no more SQL injections!)
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