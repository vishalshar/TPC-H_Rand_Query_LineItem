import psycopg2
import time

# Connecting to local postgress database
try:
    conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='admin'")
    cur = conn.cursor()

    starttime = time.time()
    print cur.execute("""EXPLAIN ANALYZE select count(*) from lineitem""")
    rows = cur.fetchall()
    print(time.time() - starttime)


    print "\nShow me the databases:\n"
    print len(rows)
    print rows


    conn.close()


except:
    print "I am unable to connect to the database"
