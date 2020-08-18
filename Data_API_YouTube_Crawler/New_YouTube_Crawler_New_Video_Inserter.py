import pika
import New_YouTube_Crawler
import requests

# time.sleep(random.random() * 18)

crawler = New_YouTube_Crawler.YouTube_Crawler()

proxies = {
    "http": "http://Paper9795:!As515951@us-wa.proxymesh.com:31280",
    "https": "http://Paper9795:!As515951@us-wa.proxymesh.com:31280",
}

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
    print(" [x] Received %r" % body.decode())

    if crawler.update_video_info(body.decode()):
        channel.basic_ack(delivery_tag=method.delivery_tag, multiple=False)
    else:
        channel.basic_nack(
            delivery_tag=method.delivery_tag, multiple=False, requeue=False
        )


channel.basic_consume(queue="URL2", on_message_callback=callback, auto_ack=False)

print(" [*] Waiting for messages. To exit press CTRL+C")
channel.start_consuming()

# crawler.update_video_info('UU78PMQprrZTbU0IlMDsYZPw', 10)
