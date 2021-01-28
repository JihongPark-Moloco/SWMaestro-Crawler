"""
supervisor.py에 호출되는 채널 정보 수집 및 갱신을 위한 크롤러입니다.
YouTube Data API v3를 통해 수행됩니다.
"""

import pika
import sys
import New_YouTube_Crawler

id = #id
pw = #pw
ip = #ip


def do():
    """
    메세지큐로 부터 받은 메세지로 크롤링 동작을 수행합니다.
    파라미터로부터 API key를 결정합니다.
    """
    crawler = New_YouTube_Crawler.YouTube_Crawler()
    api_set = sys.argv[1]

    credentials = pika.PlainCredentials(id, pw)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            ip,
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
        print(" [x] Received %r" % body.decode())

        if crawler.update_channel_info(body.decode(), api_set=int(api_set)):
            channel.basic_ack(delivery_tag=method.delivery_tag, multiple=False)
        else:
            channel.basic_nack(
                delivery_tag=method.delivery_tag, multiple=False, requeue=False
            )

    channel.basic_consume(queue="channel_updater", on_message_callback=callback, auto_ack=False)

    print(" [*] Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()

# 테스트용 메인 함수
if __name__ == "__main__":
    do()
