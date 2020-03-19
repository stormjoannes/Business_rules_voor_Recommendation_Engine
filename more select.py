#https://www.jetbrains.com/help/pycharm/installing-uninstalling-and-upgrading-packages.html
#https://api.mongodb.com/python/current/tutorial.html
#https://www.psycopg.org/docs/usage.html

import psycopg2
import random

conn = psycopg2.connect("dbname=data user=postgres password=postgres")
cur = conn.cursor()

cur.execute("select subcategory from products where id = '31190'")
allprod = cur.fetchall()
print(allprod)


# Close communication with the database
cur.close()
conn.close()