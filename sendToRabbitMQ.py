#!/usr/bin/env python
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
        # cur.execute(
        #     # f"""SELECT upload_id FROM channel WHERE status = TRUE"""   # 채널의 새로운 비디오 갱신, New_Video_Inserter
        #     # f"""SELECT channel_id FROM channel WHERE status = TRUE"""   # 채널의 구독자수등의 정보 갱신, Channel_Updater
        #     f"""SELECT video_id FROM video WHERE CURRENT_TIMESTAMP - upload_time <= INTERVAL '1 month' AND status = TRUE AND forbidden = FALSE;"""
        #     # 비디오 조회수 갱신, APP_proxy
        #     # """SELECT channel_id FROM channel WHERE channel_name IS NULL;"""
        #     #            """SELECT upload_id FROM channel WHERE idx IN (66, 98, 218, 211, 181, 197, 30, 17, 169, 171, 14, 138, 99, 60, 206, 122, 94, 43, 38, 115, 55, 85, 200, 182, 134, 112,
        #     # 52, 157, 123, 124, 119, 207, 156, 107, 162, 89, 71, 51, 252, 1588, 76, 220, 263, 26, 130, 12, 163, 172, 148, 153, 164,
        #     # 103, 95, 145, 133, 161, 228, 117, 70, 149, 116, 80, 137, 158, 9, 170, 147, 23, 11, 215, 83, 72, 168, 192, 29, 195, 91,
        #     # 79, 201, 106, 113, 104, 28, 64, 2)"""
        #     #            f"""SELECT video_id FROM video WHERE channel_idx IN (66, 98, 218, 211, 181, 197, 30, 17, 169, 171, 14, 138, 99, 60, 206, 122, 94, 43, 38, 115, 55, 85, 200, 182, 134, 112,
        #     # 52, 157, 123, 124, 119, 207, 156, 107, 162, 89, 71, 51, 252, 1588, 76, 220, 263, 26, 130, 12, 163, 172, 148, 153, 164,
        #     # 103, 95, 145, 133, 161, 228, 117, 70, 149, 116, 80, 137, 158, 9, 170, 147, 23, 11, 215, 83, 72, 168, 192, 29, 195, 91,
        #     # 79, 201, 106, 113, 104, 28, 64, 2) AND status = TRUE AND forbidden = FALSE;"""  # 비디오 조회수 갱신, APP_proxy
        # )
        # """SELECT video_id FROM video WHERE upload_time BETWEEN CURRENT_TIMESTAMP - interval '3 MONTH' AND now();""")
        # """SELECT DISTINCT video_id from video A LEFT JOIN video_views B ON A.idx = B.video_idx WHERE B.video_idx is NULL AND A.forbidden = FALSE;""")
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
