import pika
import psycopg2 as pg2

import New_YouTube_Crawler

# time.sleep(random.random() * 18)

crawler = New_YouTube_Crawler.YouTube_Crawler()

credentials = pika.PlainCredentials("muna", "muna112358!")
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        "13.124.107.195",
        5672,
        "/",
        credentials,
        heartbeat=10,
        blocked_connection_timeout=10,
    )
)

channel = connection.channel()
channel.basic_qos(prefetch_count=1)


def callback(ch, method, properties, body):
    # print(" [x] Received %r" % body.decode())

    conn = pg2.connect(
        database="createtrend",
        user="muna",
        password="muna112358!",
        host="13.124.107.195",
        port="5432",
    )
    conn.autocommit = False
    cur = conn.cursor()

    sql = f"""UPDATE channel SET status = false WHERE channel_id = '{body.decode()}';"""

    cur.execute(sql)
    conn.commit()
    conn.close()

    channel.basic_ack(delivery_tag=method.delivery_tag, multiple=False)


channel.basic_consume(queue="channel_updater_dead", on_message_callback=callback, auto_ack=False)

# print(" [*] Waiting for messages. To exit press CTRL+C")
channel.start_consuming()

# crawler.update_video_info('UU78PMQprrZTbU0IlMDsYZPw', 10)
