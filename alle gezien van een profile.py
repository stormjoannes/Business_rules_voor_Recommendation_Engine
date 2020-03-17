#https://www.jetbrains.com/help/pycharm/installing-uninstalling-and-upgrading-packages.html
#https://api.mongodb.com/python/current/tutorial.html
#https://www.psycopg.org/docs/usage.html

import psycopg2
from pymongo import MongoClient
client = MongoClient('localhost', 27017)

#db = client['test-database']
db = client.huwebshop

dol = db.products
col = db.profiles

products = dol.find()
profiles = col.find()

conn = psycopg2.connect("dbname=data user=postgres password=postgres")
cur = conn.cursor()

for i in profiles:
    print(i['_id'])
    if 'recommendations' in i:
        gezien = (i['recommendations']['viewed_before'])
        if gezien != None and gezien != '' and gezien != []:
            if len(gezien) > 1:
                ha = gezien[len(gezien) - 1]
            else:
                ha = gezien[0]
            print(ha)
            ha = "'{}'".format(ha)
            tuin = "select name from products where id = "'{}'";".format(ha)
            cur.execute(tuin)
            naam = cur.fetchall()
            if len(naam) == 0:
                continue
            print(naam[0][0])
    else:
        

conn = psycopg2.connect("dbname=voordeelshop user=postgres password=postgres")
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS all_pro;")

cur.execute("CREATE TABLE all_pro (id serial PRIMARY KEY, "
            "_ID varchar, "
            "buids varchar,"
            "order_latest varchar,"
            "recommendations varchar, "
            "previously_recommended varchar);")
#cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
# Pass data to fill a query placeholders and let Psycopg perform
# the correct conversion (no more SQL injections!)
for i in profiles:
    cur.execute("INSERT INTO all_pro (_ID, buids, order_latest,recommendations, previously_recommended) VALUES (%s, %s, %s, %s, %s)",
                (str(i['_id']),
                 i['buids'] if 'buids' in i else None,
                 i['order']['latest'] if 'order_latest' in i else None,
                 i['recommendations']['viewed_before'] if 'recommendations' in i else None,
                 i['previously_recommended'] if 'previously_recommended' in i else None))
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
