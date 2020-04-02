import psycopg2

print("\n### RecommendedPerUser.py ###\n")

conn = psycopg2.connect("dbname=Onlinestore user=postgres password=postgres")
cur = conn.cursor()

print("Setting up tables...")

cur.execute("DROP TABLE IF EXISTS all_prof_rec")

cur.execute("CREATE TABLE all_prof_rec (id varchar PRIMARY KEY, "                
            "catrecommend varchar, "
            "subcatrecommend varchar, "
            "subsubcatrecommend varchar, "
            "genderrecommend varchar);")

cur.execute("select id from profile_recommendations;")
all_id = cur.fetchall()
print('all_id done')

cur.execute("select catrecommend from profile_recommendations;")
all_catrecommend = cur.fetchall()
print('all_catrecommend done')

cur.execute("select subcatrecommend from profile_recommendations;")
all_subcatrecommend = cur.fetchall()
print('all_subcatrecommend done')

cur.execute("select subsubcatrecommend from profile_recommendations;")
all_subsubcatrecommend = cur.fetchall()
print('all_subsubcatrecommend done')

cur.execute("select recommendation from profid_targetaudience;")
all_genderrecommend = cur.fetchall()
print('all_genderrecommend done')

for tel in range(0, len(all_id)):
    print("\rcalculating recommendations {}....".format(tel), end='')
    cur.execute("INSERT INTO all_prof_rec (id, catrecommend, subcatrecommend, subsubcatrecommend, genderrecommend) VALUES (%s, %s, %s, %s, %s)", (all_id[tel], all_catrecommend[tel], all_subcatrecommend[tel], all_subsubcatrecommend[tel], all_genderrecommend[tel]))

print("Recommendations created for user!")

conn.commit()

cur.close()
conn.close()

# Start het eerstvolgende bestand.
