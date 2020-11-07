"""
RabbitMQ에 크롤러 동작을 위한 channel_id, upload_id, video_id를 저장하는 동작을 수행합니다.
"""


import pika
import psycopg2 as pg2

def send(t):
    credentials = pika.PlainCredentials('muna', 'muna112358!')
    connection = pika.BlockingConnection(pika.ConnectionParameters('13.124.107.195', 5672, '/', credentials))
    channel = connection.channel()
    conn = None

    try:
        conn = pg2.connect(database="createtrend", user="muna", password="muna112358!", host="13.124.107.195",
                           port="5432")
        cur = conn.cursor()
        # cur.execute("SELECT upload_id from channel;")
        if t == 0:
            cur.execute(
                f"""SELECT channel_id FROM channel WHERE status = TRUE AND hidden_subscriber = false"""  # 채널의 구독자수등의 정보 갱신, Channel_Updater
            )
        elif t == 1:
            cur.execute(
                f"""SELECT upload_id FROM channel WHERE status = TRUE"""  # 채널의 새로운 비디오 갱신, New_Video_Inserter
            )
        elif t == 2:
            cur.execute(
                f"""SELECT video_id FROM video WHERE CURRENT_TIMESTAMP - upload_time <= INTERVAL '1 month' AND status = TRUE AND forbidden = FALSE;"""
                # 비디오 조회수 갱신, APP_proxy
            )
        rows = cur.fetchall()
        newrows = [row[0] for row in rows]
        [print(row) for row in newrows]

    except Exception as e:
        print("postgresql database conn error")
        print(e)
    finally:
        if conn:
            conn.close()

    if t == 0:
        exchange = 'channel_updater_exchange'
        routing_key = 'channel_updater'
    elif t == 1:
        exchange = 'video_inserter_exchange'
        routing_key = 'video_inserter'
    elif t == 2:
        exchange = 'video_crawler_exchange'
        routing_key = 'video_crawler'

    for url in newrows:
        channel.basic_publish(exchange=exchange,
                              routing_key=routing_key,
                              body=url)
    print("Sending completed")

    connection.close()
