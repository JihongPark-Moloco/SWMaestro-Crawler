#!/usr/bin/env python

import pika
import threading
import time
import youtube_crawler
credentials = pika.PlainCredentials('muna','muna112358!')
connection = pika.BlockingConnection(pika.ConnectionParameters('13.124.107.195',5672,'/',credentials))
channel = connection.channel()
channel.queue_declare(queue='URLS')
channel.queue_declare(queue='dataframes')
def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    dataframe_list=youtube_crawler.main(body)
    channel.basic_publish(exchange ='',
                      routing_key = 'dataframes',
                      body=dataframe_list)
    

channel.basic_consume(
    queue='URLS', on_message_callback=callback, auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()

