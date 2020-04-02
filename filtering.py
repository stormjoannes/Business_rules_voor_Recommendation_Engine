import psycopg2
import random

conn = psycopg2.connect("dbname=data user=postgres password=postgres")
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS same;")               #Verwijderd de table die ik ga aanmaken als de table al bestaat, zo voorkom ik errors.

cur.execute("CREATE TABLE same (id serial PRIMARY KEY, "                #hier create ik mijn table.
            "product varchar, "
            "vergelijkbaar varchar);")

cur.execute("select name from products;")                   #hier zet ik in total alle producten die wij in de database hebben.
total = cur.fetchall()

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
    x = min(minimum)
    for y in range(0, len(minimum)):
        if minimum[y] == x:
            min_filt = filter[y]
            return min_filt, y

count = 0
for i in range(0, len(total)):
    # filter = ['targetaudience']
    filter = ['discount', 'targetaudience', 'category', 'subcategory']      #Hier zet ik de filter, je kan de filter gewoon aanpassen en alles zal alsnog perfect lopen.
    filt = ''
    for a in range(0, len(filter)):
        if a != len(filter) - 1:
            filt += filter[a] + ", "
        else:
            filt += filter[a]
        print(filt)
    count += 1
    verg = []
    name = str(total[i][0])
    if "'" in name:
        name = name.replace("'", "''")
    name = "'" + name + "'"
    func = f"select {filt} from products where name = "'{}'";".format(name)         #Hier haal ik alle producten op met dezelfde eigenschappen als het product waarvan we de recommendations willen.
    cur.execute(func)
    allprod = cur.fetchall()
    allprod = allprod[0]
    uitk = teller(filter, allprod)

    temp_na = allprod[uitk[1]]             #Bij deze if, else zorg ik dat de lijst om te doorlopen heel klein word, zo zal de functie erg snel lopen.
    if str(temp_na) == None:
        uitsmallen = f"select id from products where {uitk[0]} = null;"
    else:
        uitsmallen = f"select id from products where {uitk[0]} = "'{}'"".format("'{}'".format(temp_na.replace("'", '')))
    cur.execute(uitsmallen)
    versmald = cur.fetchall()

    for d in range(0, len(versmald)):           #hier loop ik nu met een laatste for loop door de verdunde lijst heen om te zoeken naar goede recommendations
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
            if allprod == allgerela[0]:                 #Als hier de eigenschappen van het product gelijk is aan de eigenschappen van 'het' product, dan word dit product als recommendation opgeslagen in een tijdelijke lijst.
                verg.append(name2.replace("'", ''))
        else:
            break
    for last in verg:           #Hier worden de 4 recommendations van 'het' product in de relationele database gezet.
        cur.execute("INSERT INTO same (product, vergelijkbaar) VALUES (%s, %s)", (name, last))
    if count >= 10:
        break

conn.commit()           #Hier commit ik de informatie naar de database

# Hier sluit ik de communicatie met de database
cur.close()
conn.close()