import psycopg2
import random

conn = psycopg2.connect("dbname=Onlinestore user=postgres password=postgres")
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS profid_targetaudience;")  # Verwijderd de table die ik ga aanmaken als de table al bestaat, zo voorkom ik errors.

cur.execute("CREATE TABLE profid_targetaudience (id_ varchar PRIMARY KEY, "  # hier create ik mijn table.
            "recommendation varchar);")

def base():
    all = all_inf()
    all_profid = all[0]
    all_prodid = all[1]
    for i in range(0, len(all_profid)):
        print("\rRendering profile recommendations: {} from 2081649....".format(i), end='')
        executer = """select deal, targetaudience from products where id = %s or id = %s or id = %s;"""
        cur.execute(executer, (all_prodid[i][0], all_prodid[i][1], all_prodid[i][2],))
        targ_prod_all = cur.fetchall()

        all_audience = []
        all_deal = []
        for index in targ_prod_all:
            all_audience.append(index[1])
            if index[0] != None:
                all_deal.append(index[0])
        targ_prod = most_common(all_audience)
        best_deal = most_common(all_deal)

        rand = recommended(targ_prod, best_deal)
        cur.execute("INSERT INTO profid_targetaudience (id_, recommendation) VALUES (%s, %s)", (all_profid[i], rand))

def recommended(targ_prod, best_deal):
    if None == targ_prod or None == best_deal:
        if targ_prod == None and best_deal != None:
            exe = """select id from products where targetaudience IS NULL and deal LIKE %s LIMIT 5"""
            cur.execute(exe, (best_deal,))
        elif best_deal == None and targ_prod != None:
            exe = """select id from products where targetaudience LIKE %s and deal IS NULL LIMIT 5"""
            cur.execute(exe, (targ_prod,))
        else:
            cur.execute("select id from products where targetaudience IS NULL and deal IS NULL LIMIT 5")

    else:
        exe = """select id from products where targetaudience LIKE %s and deal like %s LIMIT 5"""
        cur.execute(exe, (targ_prod, best_deal,))

    all_rec = cur.fetchall()
    if len(all_rec) == 0:
        exe = """select id from products where targetaudience LIKE %s LIMIT 5"""
        cur.execute(exe, (targ_prod,))
        all_rec = cur.fetchall()
    return random.choice(all_rec)

def most_common(list):
    temp_count = []
    for tem in list:
        how_much = list.count(tem)
        temp_count.append(how_much)
    if len(temp_count) > 0:
        popular = max(temp_count)
        for index in range(0, len(temp_count)):
            if popular == temp_count[index]:
                if list[index] != None:
                    if list[index].count("'") > 1:
                        list[index] = list[index][1, len(list[index]) - 1]
                    if "'" in list[index]:
                        list[index] = list[index].split("'")
                        list[index] = str(list[index][0]) + '%'
                return list[index]

def all_inf():
    cur.execute("select id from profile_recommendations;")
    all_profid = cur.fetchall()

    cur.execute("select catrecommend, subcatrecommend, subsubcatrecommend from profile_recommendations;")
    all_prodid = cur.fetchall()
    return all_profid, all_prodid

base()
conn.commit()

# Hier sluit ik de communicatie met de database
cur.close()
conn.close()