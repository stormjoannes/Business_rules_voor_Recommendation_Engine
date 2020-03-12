#https://www.jetbrains.com/help/pycharm/installing-uninstalling-and-upgrading-packages.html
#https://api.mongodb.com/python/current/tutorial.html
#https://www.psycopg.org/docs/usage.html

import psycopg2
from pymongo import MongoClient
client = MongoClient('localhost', 27017)

#db = client['test-database']
db = client.huwebshop

col = db.products

products = col.find()

print(db)

conn = psycopg2.connect("dbname=data user=postgres password=postgres")
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS brand_id;")

cur.execute("CREATE TABLE brand_id (id serial PRIMARY KEY, "
            "_ID varchar, "
            "brand varchar);")
#cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
# Pass data to fill a query placeholders and let Psycopg perform
# the correct conversion (no more SQL injections!)
a = []
for i in products:
    if 'brand' in i:
        if i['brand'] not in a:
            cur.execute("INSERT INTO brand_id (_ID, brand) VALUES (%s, %s)",
                        (i['_id'],
                         i['brand'] if 'brand' in i else None))
            a.append(i['brand'])
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