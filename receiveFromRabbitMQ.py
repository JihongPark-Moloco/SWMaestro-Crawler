#!/usr/bin/env python3

import pika
import YouTube_Crawler

credentials = pika.PlainCredentials('muna', 'muna112358!')
# connection = pika.BlockingConnection(pika.ConnectionParameters('13.124.107.195', 5672, '/',
#                                                                credentials, heartbeat=0,
#                                                                blocked_connection_timeout=None))
# channel = connection.channel()
# channel.basic_qos(prefetch_count=1)
#
#
# def callback(ch, method, properties, body):
#     print(" [x] Received %r" % body.decode())
#     if YouTube_Crawler.main(body.decode()):
#         # 크롤러가 정상 종료 되었을때 ack를 보냅니다. 이때 메세지큐에서 해당 주소가 삭제됩니다.
#         print('success')
#     else:
#         # 크롤러가 에러를 띄우면 negative ack를 보냅니다.
#         # URL 큐는 큐의 x-dead-letter 설정에 따라 해당 메세지를 URL_dead 큐로 보냅니다.
#         channel.basic_publish(exchange='URL_exchange',
#                               routing_key='URL_dead',
#                               body=body.decode())
#     # if YouTube_Crawler.main(body.decode()):
#     #     return
#     # else:
#     #     channel.basic_publish(exchange ='', routing_key = 'URL_dead', body=body.decode())
#
#
# # auto_ack를 False로 수정했습니다.
# # auto_ack가 True일 경우 메세지를 꺼내오는 순간에 메세지 큐에서 해당 메세지는 삭제됩니다.
# # 만약 해당 주소를 크롤러가 받아와서 도는 도중에 크롤러가 중간에 에러를 띄우고
# # 프로세스가 중간에 죽어버릴 경우 정상적으로 처리가 안되었지만 메세지 큐에는 해당 주소가 없는 상황이 발생합니다.
# channel.basic_get(queue='URL', on_message_callback=callback, auto_ack=False)
#
# print(' [*] Waiting for messages. To exit press CTRL+C')
# channel.start_consuming()


while True:
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('13.124.107.195', 5672, '/',
                                                                       credentials, heartbeat=0,
                                                                       blocked_connection_timeout=None))
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)

        method_frame, header_frame, body = channel.basic_get(queue='URL', auto_ack=True)

        connection.close(reply_code=200, reply_text='Normal shutdown')
    except Exception as e:
        print("connection Error:", e)
        break

    print(" [x] Received %r" % body.decode())

    if YouTube_Crawler.main(body.decode()):
        print('success')
    else:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('13.124.107.195', 5672, '/',
                                                                           credentials, heartbeat=0,
                                                                           blocked_connection_timeout=None))
            channel = connection.channel()
            channel.basic_qos(prefetch_count=1)

            channel.basic_publish(exchange='URL_exchange',
                                  routing_key='URL_dead',
                                  body=body.decode())

            connection.close(reply_code=200, reply_text='Normal shutdown')
        except Exception as e:
            print("connection Error:", e)
            break
