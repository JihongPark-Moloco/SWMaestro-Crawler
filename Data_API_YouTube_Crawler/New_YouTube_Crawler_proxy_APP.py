"""
supervisor.py에 호출되는 영상 정보 수집 크롤러입니다.
Rotating Proxy 서비스와 requests 라이브러리를 통해 동작합니다.
"""

import sys

import pika
import requests

import view_count_crawler

credentials = pika.PlainCredentials("muna", "muna112358!")

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36"
cookies = requests.cookies.create_cookie(domain=".youtube.com", name="PREF", value="gl=US&hl=en")


def do():
    # 5035 ~ 5049
    port = "50" + sys.argv[1]

    if not (5035 <= int(port) <= 5049):
        print("Not Acceptable Port Number", port)
        return

    ip = "69.46.80.226:" + port
    proxies = {"http": "http://" + ip, "https": "http://" + ip}

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            "13.124.107.195", 5672, "/", credentials, heartbeat=10, blocked_connection_timeout=10,
        )
    )
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    session.cookies.set_cookie(cookies)

    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body.decode(), ip)

        if view_count_crawler.do(body.decode(), session, proxies):
            channel.basic_ack(delivery_tag=method.delivery_tag, multiple=False)
        else:
            channel.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)

    channel.basic_consume(queue="video_crawler", on_message_callback=callback, auto_ack=False)

    print(" [*] Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()


# 테스트 동작을 위한 메인문
if __name__ == "__main__":
    do()
