import psycopg2

conn = psycopg2.connect("dbname=data user=postgres password=postgres")
cur = conn.cursor()

cur.execute("select * from products")
tot_products = cur.fetchall()

cur.execute("select id from profiles")
tot_id = cur.fetchall()

cur.execute("DROP TABLE IF EXISTS vergelijkbaar_prof;")
                                                                                        #Hier drop ik de table zo nodig en maak ik weer een nieuwe aan.
cur.execute("CREATE TABLE vergelijkbaar_prof (id serial PRIMARY KEY, "
            "prof_id varchar, "
            "vergelijkbaar varchar);")

cur.execute("select name from products;")
total = cur.fetchall()


def base():
    "'vanuit hier run ik het begin en zorg ik dat alles op de manier loopt hoe ik het wil hebben'"
    # hi = 0                #hi gebruik ik hier om een cap te geven zodat de code niet blijft runnen.
    for id in tot_id:
        # hi += 1
        verz = verzameling(id)
        eind = prod_ophalen(verz, id)
        insert_into_table(id, eind)
        # if hi > 1:
        #     break


def verzameling(id):
    "'in deze functie haal ik de verzameling producten op waar het desbetreffende profile_id naar heeft gekeken'"
    id = "'{}'".format(id[0])
    uitv = "select prodid from profiles_previously_viewed where profid = "'{}'";".format(id)
    cur.execute(uitv)
    bel_prod_id = cur.fetchall()
    return bel_prod_id


def prod_ophalen(verz, id):
    "'In deze functie bepaal ik welk product en de naam van het product die we gaan kieze nom gerelateerde producten weer te geven.'"
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
    "'in deze functie ga ik kijken wel profiel die al naar producten heeft gekeken op het desbetreffende profiel lijkt, hiermee kan ik toch een recommendation geven terwijl diegene nog nergens naar heeft gekeken'"
    search_verg = verge_mat(id)
    cur.execute("select profid from profiles_previously_viewed")
    test_id = cur.fetchall()
    for tries in test_id:
        verge = verge_mat(tries)
        if verge[0] == search_verg[0]:
            test = verzameling(tries)
            if len(test) > 0:
                last = prod_ophalen(test, id)
                return last


def verge_mat(id):
    "'In deze functie haal ik de eigenschappen van een profile op'"
    id = "'{}'".format(id[0])
    uitv = "select segment, devicetype from sessions where profid = "'{}'";".format(id)
    cur.execute(uitv)
    tot = cur.fetchall()
    return tot


def insert_into_table(id, prod):
    "'In deze functie word de recommended van het product uitgerekend door middel van de de eigenschappen, deze worden gefilterd door de 'filter''"
    filter = ['discount', 'targetaudience', 'category', 'subcategory']
    filt = ''
    for a in range(0, len(filter)):
        if a != len(filter) - 1:
            filt += filter[a] + ", "
        else:
            filt += filter[a]
    verg = []
    name = prod
    if "'" in name:                                                #hier zorg ik dat namen met een ' in de naam geen error opleveren in de query
        name = name.replace("'", "''")
    name = "'" + name + "'"

    func = "select discount, targetaudience, category, subcategory from products where name = "'{}'";".format(name)
    cur.execute(func)
    allprod = cur.fetchall()                #hier haal ik de filter eigenschappen op van het product waar we de recommendations van willen.

    func = f"select {filt} from products where name = "'{}'";".format(name)
    cur.execute(func)
    allprod = cur.fetchall()
    allprod = allprod[0]
    uitk = teller(filter, allprod)

    temp_na = allprod[uitk[1]]
    if temp_na == None:
        uitsmallen = f"select id from products where {uitk[0]} = null;"                                         #Hier zorg ik ervoor dat als er None uitkomt, dat de code het dan verandert naar null zodat pgadmin dit kan lezen.
    else:
        uitsmallen = f"select id from products where {uitk[0]} = "'{}'"".format("'{}'".format(temp_na.replace("'", '')))
    cur.execute(uitsmallen)
    versmald = cur.fetchall()

    for d in range(0, len(versmald)):                               #versmald is de verfijnde lijst van producten en die ga ik langs om de goede recommendation te zoeken
        if name.replace("'", '') in verg:
            verg.remove(name.replace("'", ''))
        if len(verg) < 4:
            name2 = str(versmald[d][0])
            if "'" in name2:
                name2 = name2.split("'")
                name2 = name2[0] + "''" + name2[1]
            name2 = "'" + name2 + "'"

            func2 = "select discount, targetaudience, category, subcategory from products where id = "'{}'";".format(name2)
            cur.execute(func2)
            allgerela = cur.fetchall()

            if allprod == allgerela[0]:
                verg.append(name2.replace("'", ''))
        else:
            break

    for last in verg:
        cur.execute("INSERT INTO vergelijkbaar_prof (prof_id, vergelijkbaar) VALUES (%s, %s)", (id, last))      #Hier zet ik de recommendations bij ieder product in de relationele database.
        conn.commit()               #Hier commit ik de informatie naar de database

def teller(filter, name):
    "'Hierin kijk ik bij welke filter er zo min mogelijk uitkomsten geeft om een for loop in te plaatsen'"
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
    mini = min(minimum)
    for index in range(0, len(minimum)):                #hier zoek ik wat de index is van het minimum getal zodat ik kan zien bij welke filter het hoort.
        if minimum[index] == mini:
            min_filt = filter[index]
            return min_filt, index

base()

# hier sluit ik de communicatie met de database
cur.close()
conn.close()