#https://www.jetbrains.com/help/pycharm/installing-uninstalling-and-upgrading-packages.html
#https://api.mongodb.com/python/current/tutorial.html
#https://www.psycopg.org/docs/usage.html

import psycopg2
import random

conn = psycopg2.connect("dbname=data user=postgres password=postgres")
cur = conn.cursor()

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