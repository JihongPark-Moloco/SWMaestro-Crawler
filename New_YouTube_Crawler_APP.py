import New_YouTube_Crawler
import psycopg2 as pg2
import pika

crawler = New_YouTube_Crawler.YouTube_Crawler()
# crawler.make_driver_ready()

credentials = pika.PlainCredentials('muna', 'muna112358!')
connection = pika.BlockingConnection(pika.ConnectionParameters('13.124.107.195', 5672, '/',
                                                               credentials, heartbeat=0,
                                                               blocked_connection_timeout=None))
channel = connection.channel()
channel.basic_qos(prefetch_count=1)


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body.decode())

    if crawler.update_video_and_comment(body.decode()):
        channel.basic_ack(delivery_tag=method.delivery_tag, multiple=False)
    else:
        channel.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)


channel.basic_consume(queue='URL', on_message_callback=callback, auto_ack=False)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
