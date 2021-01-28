"""
전체 크롤러 동작을 자동으로 수행합니다.
channel_updater
new_video_inserter
video_crawler
순서로 동작합니다.

# channel_updater
  대상 채널의 다음의 정보를 수집합니다.
  - status: 채널 활성화 여부
  - channel_views: 채널 전체 조회수
  - 기타 채널정보

# new_video_inserter
  대상 채널에서 새롭데 등록된 영상 정보를 가져옵니다.

# video_crawler
  대상 영상들의 조회수, 좋아요수, 싫어요수, 댓글의 정보를 수집합니다.
"""


import json
import os
import subprocess
import time
import sys

import psutil
import requests

import sendToRabbitMQ

id = #id
pw = #pw
ip = #ip


# 0: channel updater Crawler 수행
# 채널 정보를 업데이트합니다.
# 3개의 subprocess를 구동해 동작합니다.
# 각각의 프로세스는 서로 다른 API key 값을 가지고 구동합니다.
print("#### Start send to rabbit, upload channel_id ####")
sendToRabbitMQ.send(0)
print("#### Done send to rabbit, upload channel_id ####")

print("#### Start process to crawler channel updater ####")
pro_1 = subprocess.Popen(
    "python New_YouTube_Crawler_Channel_Updater.py " + str(0),
    stdout=subprocess.PIPE,
    stdin=subprocess.PIPE,
    stderr=subprocess.PIPE,
    cwd=r"D:\Cloud\Project\PyCharm\muna-3\Data_API_YouTube_Crawler",
)
pro_2 = subprocess.Popen(
    "python New_YouTube_Crawler_Channel_Updater.py " + str(1),
    stdout=subprocess.PIPE,
    stdin=subprocess.PIPE,
    stderr=subprocess.PIPE,
    cwd=r"D:\Cloud\Project\PyCharm\muna-3\Data_API_YouTube_Crawler",
)
pro_3 = subprocess.Popen(
    "python New_YouTube_Crawler_Channel_Updater.py " + str(2),
    stdout=subprocess.PIPE,
    stdin=subprocess.PIPE,
    stderr=subprocess.PIPE,
    cwd=r"D:\Cloud\Project\PyCharm\muna-3\Data_API_YouTube_Crawler",
)

# 메세지큐를 검사해 남은 메세지수가 0이면 종료합니다.
while True:
    time.sleep(10)
    session = requests.Session()
    session.auth = (id, pw)
    res = session.get(f"http://{ip}:30000/api/queues/%2f/channel_updater")
    res = json.loads(res.text)
    if res["backing_queue_status"]["len"] == 0:
        pro_1.kill()
        pro_2.kill()
        pro_3.kill()
        # 비활성화된 채널 정보를 DB에 반영하기 위한 소스입니다.
        # pro_1 = subprocess.Popen(
        #     "python New_YouTube_Crawler_Channel_Updater_Error.py ",
        #     stdout=subprocess.PIPE,
        #     stdin=subprocess.PIPE,
        #     stderr=subprocess.PIPE,
        #     cwd=r"D:\Cloud\Project\PyCharm\muna-3\Data_API_YouTube_Crawler",
        # )
        # time.sleep(10)
        # pro_1.kill()
        break
    else:
        time.sleep(50)
print("#### Done process to crawler channel updater ####")

# 1: video inserter Crawler 수행
# 채널의 신규 영상 정보를 수집합니다.
# 3개의 subprocess를 구동해 동작합니다.
# 각각의 프로세스는 서로 다른 API key 값을 가지고 구동합니다.

print("#### Start send to rabbit, upload upload_id ####")
sendToRabbitMQ.send(1)
print("#### Done send to rabbit, upload upload_id ####")

print("#### Start process to crawler new video inserter ####")
pro_1 = subprocess.Popen(
    "python New_YouTube_Crawler_New_Video_Inserter.py " + str(0),
    stdout=subprocess.PIPE,
    stdin=subprocess.PIPE,
    stderr=subprocess.PIPE,
    cwd=r"D:\Cloud\Project\PyCharm\muna-3\Data_API_YouTube_Crawler",
)
pro_2 = subprocess.Popen(
    "python New_YouTube_Crawler_New_Video_Inserter.py " + str(1),
    stdout=subprocess.PIPE,
    stdin=subprocess.PIPE,
    stderr=subprocess.PIPE,
    cwd=r"D:\Cloud\Project\PyCharm\muna-3\Data_API_YouTube_Crawler",
)
pro_3 = subprocess.Popen(
    "python New_YouTube_Crawler_New_Video_Inserter.py " + str(2),
    stdout=subprocess.PIPE,
    stdin=subprocess.PIPE,
    stderr=subprocess.PIPE,
    cwd=r"D:\Cloud\Project\PyCharm\muna-3\Data_API_YouTube_Crawler",
)

# 메세지큐를 검사해 남은 메세지수가 0이면 종료합니다.
while True:
    time.sleep(10)
    session = requests.Session()
    session.auth = (id, pw)
    res = session.get(f"http://{ip}:30000/api/queues/%2f/video_inserter")
    res = json.loads(res.text)
    if res["backing_queue_status"]["len"] == 0:
        pro_1.kill()
        pro_2.kill()
        pro_3.kill()
        break
print("#### Done process to crawler new video inserter ####")


print("#### Start send to rabbit, upload video_id ####")
sendToRabbitMQ.send(2)  # 2: video crawler
print("#### Done send to rabbit, upload video_id ####")

port_list = [35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49]
port_dict = {}

print("#### Start process to video cralwer ####")

# 지정된 프록시 포트로 크롤링 동작을 수행하는 subprocess를 생성합니다.
for port_num in port_list:
    pro = subprocess.Popen(
        "python New_YouTube_Crawler_proxy_APP.py " + str(port_num),
        stdout=subprocess.PIPE,
        stdin=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=r"D:\Cloud\Project\PyCharm\muna-3\Data_API_YouTube_Crawler",
    )
    port_dict[port_num] = pro.pid

while True:
    time.sleep(5)
    os.system("cls")

    # 각 프로세스의 생존을 검사하고 죽은 프로세르를 재실행합니다.
    for port_num in port_list:
        if psutil.pid_exists(port_dict[port_num]):
            print(f"port {port_num}: True")
        else:
            print(f"port {port_num}: False")
            pro = subprocess.Popen(
                "python New_YouTube_Crawler_proxy_APP.py " + str(port_num),
                stdout=subprocess.PIPE,
                stdin=subprocess.PIPE,
                cwd=r"D:\Cloud\Project\PyCharm\muna-3\Data_API_YouTube_Crawler",
            )
            port_dict[port_num] = pro.pid

    session = requests.Session()
    session.auth = (id, pw)
    res = session.get(f"http://{ip:30000/api/queues/%2f/video_crawler")
    res = json.loads(res.text)

    # 남은 메세지수가 0이면 종료합니다.
    if res["backing_queue_status"]["len"] == 0:
        pro = subprocess.Popen(
            "python New_YouTube_Crawler_proxy_APP_Error.py",
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
            cwd=r"D:\Cloud\Project\PyCharm\muna-3\Data_API_YouTube_Crawler",
        )
        time.sleep(10)
        break
print("#### Done process to video cralwer ####")
print("#### Done All Porcess:", time.ctime(), "####")
sys.exit(0)
