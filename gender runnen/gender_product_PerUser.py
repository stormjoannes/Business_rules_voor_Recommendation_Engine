import psycopg2
import random

conn = psycopg2.connect("dbname=Onlinestore user=postgres password=postgres")
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS profid_targetaudience;")               #Verwijderd de table die ik ga aanmaken als de table al bestaat, zo voorkom ik errors.

cur.execute("CREATE TABLE profid_targetaudience (id_ varchar PRIMARY KEY, "                #hier create ik mijn table.
            "recommendation varchar);")

cur.execute("select id from profile_recommendations;")
all_profid = cur.fetchall()


cur.execute("select catrecommend, subcatrecommend, subsubcatrecommend from profile_recommendations;")
all_prodid = cur.fetchall()

for i in range( 0, len(all_profid)):
    print("\rcalculating profile {}....".format(i), end='')
    var = all_prodid[i][0]
    var = "'" + str(var) + "'"
    catrecommend = "'" + all_prodid[i][0] + "'"
    subcatrecommend = "'" + all_prodid[i][1] + "'"
    subsubcatrecommend = "'" + all_prodid[i][2] + "'"
    executer = f"select deal, targetaudience from products where id = {catrecommend} or id = {subcatrecommend} or id = {subsubcatrecommend};"
    cur.execute(executer)
    targ_prod_all = cur.fetchall()

    all_audience = []
    all_deal = []
    for q in targ_prod_all:
        all_audience.append(q[1])
        if q[0] != None:
            all_deal.append(q[0])

    views = []
    for index in all_audience:
        hvl = all_audience.count(index)
        views.append(hvl)
    popu = max(views)
    for g in range(0, len(views)):
        if popu == views[g]:
            targ_prod = all_audience[g]
            break

    temp_count = []
    for tem in all_deal:
        how_much = all_deal.count(tem)
        temp_count.append(how_much)
    if len(temp_count) > 0:
        popular = max(temp_count)
        for where in range(0, len(temp_count)):
            if popular == temp_count[where]:
                best_deal = all_deal[where]

    if None == targ_prod:
        if best_deal.count("'") < 2:
            best_deal = "'" + best_deal + "'"
        exe = f"select id from products where targetaudience IS NULL and deal like {best_deal} LIMIT 5"
        cur.execute(exe)

    else:
        targ_prod = targ_prod
        targ_prod = targ_prod.split("'")
        targ_prod = targ_prod[0] + '%'
        if best_deal.count("'") < 2:
            best_deal = "'" + best_deal + "'"
        if targ_prod == 'Unisex%':
            targ_prod = "'" + str(targ_prod) + "'"
            exe = f"select id from products where targetaudience LIKE {targ_prod}LIMIT 5"
            cur.execute(exe)
        else:
            targ_prod = "'" + str(targ_prod) + "'"
            exe = f"select id from products where targetaudience LIKE {targ_prod} and deal like {best_deal} LIMIT 5"
            cur.execute(exe)

        all_rec = cur.fetchall()
        if len(all_rec) == 0:
            exe = f"select id from products where targetaudience LIKE {targ_prod}LIMIT 5"
            cur.execute(exe)
            all_rec = cur.fetchall()
        rand = random.choice(all_rec)
        cur.execute("INSERT INTO profid_targetaudience (id_, recommendation) VALUES (%s, %s)", (all_profid[i], rand))

    conn.commit()

# Hier sluit ik de communicatie met de database
cur.close()
conn.close()