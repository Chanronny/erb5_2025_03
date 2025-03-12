import psycopg2
conn = psycopg2.connect("host=localhost dbname=bcredb user=ronny")
cur = conn.cursor()
cur.execute("INSERT INTO realtors_realtor VALUES ('Test Realtor1','\jenny.jpg','Test Realtor Desc','4444-6666','t@t.com', False)" )
conn.commit()