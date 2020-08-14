import psycopg2 as pg2
import pika
import time
import random
import view_count_crawler
import requests

# time.sleep(random.random() * 18)

# crawler = New_YouTube_Crawler.YouTube_Crawler()
# crawler.make_driver_ready()

proxies = {'http': 'http://Paper9795:!As515951@us-wa.proxymesh.com:31280',
           'https': 'http://Paper9795:!As515951@us-wa.proxymesh.com:31280'}

credentials = pika.PlainCredentials('muna', 'muna112358!')
connection = pika.BlockingConnection(pika.ConnectionParameters('13.124.107.195', 5672, '/',
                                                               credentials, heartbeat=10,
                                                               blocked_connection_timeout=10))
channel = connection.channel()
channel.basic_qos(prefetch_count=1)

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36"
cookies = requests.cookies.create_cookie(domain='.youtube.com', name='PREF', value='gl=US&hl=en')
session = requests.Session()
session.headers['User-Agent'] = USER_AGENT
session.cookies.set_cookie(cookies)


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body.decode())

    if view_count_crawler.do(body.decode(), session, proxies):
        channel.basic_ack(delivery_tag=method.delivery_tag, multiple=False)
    else:
        channel.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)


channel.basic_consume(queue='URL2', on_message_callback=callback, auto_ack=False)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()

# crawler.update_video_info('UU78PMQprrZTbU0IlMDsYZPw', 10)
