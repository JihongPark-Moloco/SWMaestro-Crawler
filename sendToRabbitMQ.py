#!/usr/bin/env python
import pika
import psycopg2 as pg2


def loadUrls():
    conn = None
    try:
        conn = pg2.connect(database="createtrend", user="muna", password="muna112358!", host="222.112.206.190",
                           port="5432")
        cur = conn.cursor()
        # cur.execute("SELECT upload_id from channel;")
        cur.execute(
            # """SELECT video_id FROM video WHERE upload_time BETWEEN CURRENT_TIMESTAMP - interval '3 MONTH' AND now();""")
            """SELECT DISTINCT video_id from video A LEFT JOIN video_views B ON A.idx = B.video_idx WHERE B.video_idx is NULL AND A.forbidden = FALSE;""")
        rows = cur.fetchall()
        newrows = [row[0] for row in rows]
        [print(row) for row in newrows]
        return newrows

    except Exception as e:
        print("postgresql database conn error")
        print(e)
    finally:
        if conn:
            conn.close()


credentials = pika.PlainCredentials('muna', 'muna112358!')
connection = pika.BlockingConnection(pika.ConnectionParameters('13.124.107.195', 5672, '/', credentials))
channel = connection.channel()

urls = loadUrls()
for url in urls:
    channel.basic_publish(exchange='',
                          routing_key='URL',
                          body=url)
print("Sending completed")

connection.close()
