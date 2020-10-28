import json
import os
import subprocess
import time
import sys

import psutil
import requests

import sendToRabbitMQ

# The os.setsid() is passed in the argument preexec_fn so
# it's run after the fork() and before  exec() to run the shell.
# pro = subprocess.Popen(
#     "python New_YouTube_Crawler_proxy_APP.py 35",
#     stdout=subprocess.PIPE,
#     stdin=subprocess.PIPE,
#     cwd=r"D:\Cloud\Project\PyCharm\muna-3\Data_API_YouTube_Crawler",
# )


sendToRabbitMQ.send(0)  # 0: channel updater
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

while True:
    time.sleep(10)
    session = requests.Session()
    session.auth = ('muna', 'muna112358!')
    res = session.get(r'http://13.124.107.195:30000/api/queues/%2f/channel_updater')
    res = json.loads(res.text)
    if res['backing_queue_status']['len'] == 0:
        pro_1.kill()
        pro_2.kill()
        pro_3.kill()
        pro_1 = subprocess.Popen(
            "python New_YouTube_Crawler_Channel_Updater_Error.py ",
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=r"D:\Cloud\Project\PyCharm\muna-3\Data_API_YouTube_Crawler",
        )
        time.sleep(10)
        pro_1.kill()
        break
    else:
        time.sleep(50)

sendToRabbitMQ.send(1)  # 1: video inserter
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

while True:
    time.sleep(10)
    session = requests.Session()
    session.auth = ('muna', 'muna112358!')
    res = session.get(r'http://13.124.107.195:30000/api/queues/%2f/video_inserter')
    res = json.loads(res.text)
    if res['backing_queue_status']['len'] == 0:
        pro_1.kill()
        pro_2.kill()
        # pro_3.kill()
        break

sendToRabbitMQ.send(2)  # 2: video crawler

port_list = [35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49]
port_dict = {}

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
    os.system('cls')

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
    session.auth = ('muna', 'muna112358!')
    res = session.get(r'http://13.124.107.195:30000/api/queues/%2f/video_crawler')
    res = json.loads(res.text)

    if res['backing_queue_status']['len'] == 0:
        pro = subprocess.Popen(
            "python New_YouTube_Crawler_proxy_APP_Error.py",
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
            cwd=r"D:\Cloud\Project\PyCharm\muna-3\Data_API_YouTube_Crawler",
        )
        time.sleep(10)
        break

print('Done:', time.ctime())
sys.exit(0)