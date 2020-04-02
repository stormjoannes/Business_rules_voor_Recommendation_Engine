import psycopg2
import random

conn = psycopg2.connect("dbname=Onlinestore user=postgres password=postgres")
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS profid_targetaudience;")               #Verwijderd de table die ik ga aanmaken als de table al bestaat, zo voorkom ik errors.

cur.execute("CREATE TABLE profid_targetaudience (id_ varchar PRIMARY KEY, "                #hier create ik mijn table.
            "recommendation varchar);")

cur.execute("select id from profile_recommendations;")
all_profid = cur.fetchall()


cur.execute("select profid from profiles_previously_viewed;")
all_profid_viewed = cur.fetchall()
all_profid_viewed = set(all_profid_viewed)
all_profid_viewed = list(all_profid_viewed)

for i in range(0, len(all_profid)):
    if i > 10:
        break
    print("\rcalculating profile {}....".format(i), end='')
    temp_profid = "'" + all_profid[i][0] + "'"
    if all_profid[i] in all_profid_viewed:
        cur.execute(f"select prodid from profiles_previously_viewed where profid = {temp_profid}")
        var = cur.fetchall()
        var = var[0][0]
    else:
        found_profile = verg_profile(temp_profid)
        cur.execute(f"select prodid from profiles_previously_viewed where profid = {found_profile}")
        var = cur.fetchall()
        var = var[0][0]

    var = "'" + str(var) + "'"
    executer = f"select targetaudience from products where id = {var};"
    cur.execute(executer)
    targ_prod = cur.fetchall()
    targ_prod = targ_prod[0]

    if targ_prod[0] == None:
        exe = f"select id from products where targetaudience IS NULL LIMIT 100"
        cur.execute(exe)

    elif "'" in targ_prod[0]:
        targ_prod = targ_prod[0].split("'")
        targ_prod = targ_prod[0] + '%'
        targ_prod = "'" + str(targ_prod) + "'"
        exe = f"select id from products where targetaudience LIKE {targ_prod} LIMIT 100"
        cur.execute(exe)
    else:
        targ_prod = "'" + str(targ_prod[0]) + "'"
        exe = f"select id from products where targetaudience LIKE {targ_prod} LIMIT 100"
        cur.execute(exe)

    all_rec = cur.fetchall()
    rand = random.choice(all_rec)
    cur.execute("INSERT INTO profid_targetaudience (id_, recommendation) VALUES (%s, %s)", (all_profid[i], rand))

def verg_profile(temp_profid):
    cur.execute(f"select os, devicefamily, devicetype from sessions where id = {temp_profid}")
    verge = cur.fetchall()
    os = "'" + verge[0][0] + "'"
    devicefamily = "'" + verge[1][0] + "'"
    devicetype = "'" + verge[2][0] + "'"
    cur.execute(f"select id from sessions where os = {os} and devicefamily = {devicefamily} and devicetype = {devicetype} LIMIT 100")
    all_id = cur.fetchall()
    for x in all_id[0]:
        if x in all_profid_viewed:
            return x


conn.commit()

# Hier sluit ik de communicatie met de database
cur.close()
conn.close()