#https://www.jetbrains.com/help/pycharm/installing-uninstalling-and-upgrading-packages.html
#https://api.mongodb.com/python/current/tutorial.html
#https://www.psycopg.org/docs/usage.html

import psycopg2

conn = psycopg2.connect("dbname=data user=postgres password=postgres")
cur = conn.cursor()


cur. execute("select * from products")
tot_products = cur.fetchall()

cur.execute("select id from profiles")
tot_id = cur.fetchall()

def base():
    for id in tot_id:
        print(id)
        verz = verzameling(id)
        eind = prod_ophalen(verz, id)
        insert_into_table(id, eind)
        break


def verzameling(id):
        id = "'{}'".format(id[0])
        uitv = "select prodid from profiles_previously_viewed where profid = "'{}'";".format(id)
        cur.execute(uitv)
        bel_prod_id = cur.fetchall()
        return bel_prod_id

def prod_ophalen(verz, id):
    if len(verz) > 0:
        if len(verz) > 1:
            verz = verz[len(verz) - 1]
        else:
            verz = verz[0]
        verz = "'{}'".format(verz[0])
        uitv2 = "select name from products where id = "'{}'";".format(verz)
        cur.execute(uitv2)
        bel_prod_name = cur.fetchall()
        return bel_prod_name[0][0]
    else:
        vergeleken = search_same(id)
        return vergeleken

def search_same(id):
    search_verg = verge_mat(id)
    cur.execute("select profid from profiles_previously_viewed")
    test_id = cur.fetchall()
    for h in test_id:
        verge = verge_mat(h)
        if verge[0] == search_verg[0]:
            test = verzameling(h)
            if len(test) > 0:
                last = prod_ophalen(test, id)
                return last

def verge_mat(id):
    id = "'{}'".format(id[0])
    uitv = "select devicefamily, devicetype from sessions where profid = "'{}'";".format(id)
    cur.execute(uitv)
    tot = cur.fetchall()
    return tot

def insert_into_table(id, prod):
    cur.execute("DROP TABLE IF EXISTS vergelijkbaar_prof;")

    cur.execute("CREATE TABLE vergelijkbaar_prof (id serial PRIMARY KEY, "
                "prof_id varchar, "
                "vergelijkbaar varchar);")

    cur.execute("select name from products;")
    total = cur.fetchall()

    verg = []
    name = prod
    if "'" in name:
        name = name.split("'")
        name = name[0] + "''" + name[1]
    name = "'" + name + "'"

    func = "select discount, targetaudience, category, subcategory from products where name = "'{}'";".format(name)
    cur.execute(func)
    allprod = cur.fetchall()

    for d in range(0, len(total)):
        if name.replace("'", '') in verg:
            verg.remove(name.replace("'", ''))
            
        if len(verg) < 3:
            name2 = str(total[d][0])
            if "'" in name2:
                name2 = name2.split("'")
                name2 = name2[0] + "''" + name2[1]
            name2 = "'" + name2 + "'"

            func2 = "select discount, targetaudience, category, subcategory from products where name = "'{}'";".format(
                name2)
            cur.execute(func2)
            allgerela = cur.fetchall()
            if allprod == allgerela:
                verg.append(name2.replace("'", ''))
        else:
            break

    for last in verg:
        cur.execute("INSERT INTO vergelijkbaar_prof (prof_id, vergelijkbaar) VALUES (%s, %s)", (id, last))


base()

conn.commit()

# Close communication with the database
cur.close()
conn.close()
