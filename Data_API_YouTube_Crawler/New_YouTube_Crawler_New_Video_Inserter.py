"""
supervisor.py에 호출되는 채널의 신규 영상 정보 수집을 위한 크롤러입니다.
YouTube Data API v3를 통해 수행됩니다.
"""

import sys

import pika

import New_YouTube_Crawler


def do():
    crawler = New_YouTube_Crawler.YouTube_Crawler()
    api_set = sys.argv[1]

    credentials = pika.PlainCredentials("muna", "muna112358!")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            "13.124.107.195", 5672, "/", credentials, heartbeat=10, blocked_connection_timeout=10,
        )
    )

    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body.decode())

        if crawler.update_video_info(body.decode(), interval_day=30, api_set=int(api_set)):
            channel.basic_ack(delivery_tag=method.delivery_tag, multiple=False)
        else:
            channel.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)

    channel.basic_consume(queue="video_inserter", on_message_callback=callback, auto_ack=False)

    print(" [*] Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()

# 테스트를 위한 메인문
if __name__ == "__main__":
    do()
