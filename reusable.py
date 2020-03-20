import psycopg2

conn = psycopg2.connect("dbname=data user=postgres password=postgres")
cur = conn.cursor()

cur.execute("select * from products;")
print(cur.fetchall())

# Close communication with the database
cur.close()
conn.close()