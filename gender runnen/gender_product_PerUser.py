import psycopg2
import random

conn = psycopg2.connect("dbname=Onlinestore user=postgres password=postgres")
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS profid_targetaudience;")               #Verwijderd de table die ik ga aanmaken als de table al bestaat, zo voorkom ik errors.

cur.execute("CREATE TABLE profid_targetaudience (id_ varchar PRIMARY KEY, "                #hier create ik mijn table.
            "recommendation varchar);")

cur.execute("select id from profile_recommendations;")
all_profid = cur.fetchall()


cur.execute("select subsubcatrecommend from profile_recommendations;")
all_prodid = cur.fetchall()

for i in range(0, len(all_profid)):
    print("\rcalculating profile {}....".format(i), end='')
    var = all_prodid[i][0]
    var = "'" + str(var) + "'"
    executer = f"select targetaudience from products where id = {var};"
    cur.execute(executer)
    targ_prod = cur.fetchall()
    targ_prod = targ_prod[0][0]
    if targ_prod == None:
        exe = f"select id from products where targetaudience IS NULL LIMIT 10"
        cur.execute(exe)

    else:
        targ_prod = targ_prod.split("'")
        targ_prod = targ_prod[0] + '%'
        targ_prod = "'" + str(targ_prod) + "'"
        exe = f"select id from products where targetaudience LIKE {targ_prod} LIMIT 10"
        cur.execute(exe)

    all_rec = cur.fetchall()
    rand = random.choice(all_rec)
    cur.execute("INSERT INTO profid_targetaudience (id_, recommendation) VALUES (%s, %s)", (all_profid[i], rand))

conn.commit()

# Hier sluit ik de communicatie met de database
cur.close()
conn.close()