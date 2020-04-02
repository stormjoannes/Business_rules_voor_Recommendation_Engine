import psycopg2
import random

print('connecting with database..')
conn = psycopg2.connect("dbname=Onlinestore user=postgres password=postgres")
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS profid_targetaudience;")               #Verwijderd de table die ik ga aanmaken als de table al bestaat, zo voorkom ik errors.
print('connected with database')

cur.execute("CREATE TABLE profid_targetaudience (id_ varchar PRIMARY KEY, "                #hier create ik mijn table.
            "recommendation varchar);")

cur.execute("select id from profile_recommendations;")
all_profid = cur.fetchall()
print('got all id')


cur.execute("select profid from profiles_previously_viewed;")
all_profid_viewed = cur.fetchall()
all_profid_viewed = set(all_profid_viewed)
all_profid_viewed = list(all_profid_viewed)
print('print all_profid_viewed')

cur.execute("select id from products where targetaudience IS NULL")
prod_null = cur.fetchall()
print('got all prod_null')

for i in range(0, len(all_profid)):
    if i > 1000:
        break
    print("\rcalculating profile {}....".format(i), end='')
    temp_profid = "'" + all_profid[i][0] + "'"
    cur.execute(f"select prodid from profiles_previously_viewed where profid = {temp_profid}")
    var = cur.fetchall()
    var = var[0]
    # var = ('45218')
    if var[0] == None:
        var = random.choice(prod_null)
    else:
        executer = f"select targetaudience from products where id = %s;"
        cur.execute(executer, (var,))
        targ_prod = cur.fetchall()
        targ_prod = targ_prod[0]
        if targ_prod[0] == None:
            exe = f"select id from products where targetaudience IS NULL LIMIT 10"
            cur.execute(exe)

        else:
            targ_prod = targ_prod[0].split("'")
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