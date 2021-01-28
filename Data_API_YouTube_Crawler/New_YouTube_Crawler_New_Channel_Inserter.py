"""
신규 채널 정보 입력에 사용되는 소스입니다.
기존 DB에 존재하지 않는 채널을 신규로 등록할때 사용합니다.
YouTube Data API v3를 통해 수행됩니다.
"""

import pika

import New_YouTube_Crawler

ip = #ip
id = #id
pw = #pw

crawler = New_YouTube_Crawler.YouTube_Crawler()

credentials = pika.PlainCredentials(id, pw)
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        ip, 5672, "/", credentials, heartbeat=10, blocked_connection_timeout=10,
    )
)

channel = connection.channel()
channel.basic_qos(prefetch_count=1)


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body.decode())

    if crawler.insert_channel_info(body.decode()):
        channel.basic_ack(delivery_tag=method.delivery_tag, multiple=False)
    else:
        channel.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)


channel.basic_consume(queue="URL2", on_message_callback=callback, auto_ack=False)

print(" [*] Waiting for messages. To exit press CTRL+C")
channel.start_consuming()
