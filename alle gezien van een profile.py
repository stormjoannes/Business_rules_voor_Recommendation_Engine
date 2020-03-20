import psycopg2

conn = psycopg2.connect("dbname=data user=postgres password=postgres")
cur = conn.cursor()

cur.execute("select * from products")
tot_products = cur.fetchall()

cur.execute("select id from profiles")
tot_id = cur.fetchall()

cur.execute("DROP TABLE IF EXISTS vergelijkbaar_prof;")

cur.execute("CREATE TABLE vergelijkbaar_prof (id serial PRIMARY KEY, "
            "prof_id varchar, "
            "vergelijkbaar varchar);")

cur.execute("select name from products;")
total = cur.fetchall()


def base():
    hi = 0
    for id in tot_id:
        hi += 1
        print(id)
        verz = verzameling(id)
        eind = prod_ophalen(verz, id)
        insert_into_table(id, eind)
        if hi > 1:
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
    print('koeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeekkkkkkkkkkkkkkkkkkkkkkkkkkkjjjjjjjjjjjjjjjjjjeeeeeeeeeeeeeeeeeeee')
    id = "'{}'".format(id[0])
    uitv = "select segment, devicetype from sessions where profid = "'{}'";".format(id)
    cur.execute(uitv)
    tot = cur.fetchall()
    return tot


def insert_into_table(id, prod):
    filter = ['discount', 'targetaudience', 'category', 'subcategory']
    filt = ''
    for a in range(0, len(filter)):
        if a != len(filter) - 1:
            filt += filter[a] + ", "
        else:
            filt += filter[a]
    verg = []
    name = prod
    if "'" in name:
        name = name.replace("'", "''")
    name = "'" + name + "'"

    func = "select discount, targetaudience, category, subcategory from products where name = "'{}'";".format(name)
    cur.execute(func)
    allprod = cur.fetchall()

    func = f"select {filt} from products where name = "'{}'";".format(name)
    cur.execute(func)
    allprod = cur.fetchall()
    allprod = allprod[0]
    uitk = teller(filter, allprod)

    koek = allprod[uitk[1]]
    if koek == None:
        uitsmallen = f"select id from products where {uitk[0]} = null;"
    else:
        uitsmallen = f"select id from products where {uitk[0]} = "'{}'"".format("'{}'".format(koek.replace("'", '')))
    cur.execute(uitsmallen)
    versmald = cur.fetchall()

    for d in range(0, len(versmald)):
        if name.replace("'", '') in verg:
            verg.remove(name.replace("'", ''))
        if len(verg) < 4:
            name2 = str(versmald[d][0])
            if "'" in name2:
                name2 = name2.split("'")
                name2 = name2[0] + "''" + name2[1]
            name2 = "'" + name2 + "'"

            func2 = "select discount, targetaudience, category, subcategory from products where id = "'{}'";".format(
                name2)
            cur.execute(func2)
            allgerela = cur.fetchall()

            if allprod == allgerela[0]:
                verg.append(name2.replace("'", ''))
        else:
            break

    print(verg)
    for last in verg:
        cur.execute("INSERT INTO vergelijkbaar_prof (prof_id, vergelijkbaar) VALUES (%s, %s)", (id, last))
        conn.commit()

def teller(filter, name):
    minimum = []
    for i in range(0, len(filter)):
        if name[i] == None:
            uitvoeren = f"select id from products where {filter[i]} = null;"
        else:
            naam = name[i]
            if "'" in str(naam):
                naam = name[i].replace("'", "''")
            uitvoeren = f"select id from products where {filter[i]} = "'{}'"".format("'{}'".format(naam))
        cur.execute(uitvoeren)
        aantal = cur.fetchall()
        minimum.append(len(aantal))
    x = min(minimum)
    for y in range(0, len(minimum)):
        if minimum[y] == x:
            min_filt = filter[y]
            return min_filt, y

base()

# Close communication with the database
cur.close()
conn.close()