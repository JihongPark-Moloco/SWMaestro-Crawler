#!/usr/bin/env python3

from selenium.webdriver.common.keys import Keys
from selenium.webdriver import Chrome
from selenium.webdriver import ChromeOptions
from bs4 import BeautifulSoup
import time
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait
import psycopg2 as pg2
import re
import pika
import json

channel_savedata = pd.DataFrame(columns=['channel_name',
                                         'channel_url',
                                         'subscriber_num',
                                         'check_time'])

driver = None
link = None


def log(text):
    global link

    current_time = time.strftime('%Y.%m.%d %H:%M:%S', time.localtime(time.time()))

    credentials = pika.PlainCredentials('muna', 'muna112358!')
    connection = pika.BlockingConnection(pika.ConnectionParameters('13.124.107.195', 5672, '/', credentials))
    channel = connection.channel()

    channel.basic_publish(exchange='',
                          routing_key='test_URL',
                          body=json.dumps({'time': current_time,
                                           'link': link[link.rfind("/") + 1:],
                                           'log': text}))

    print(current_time, link, text)

    connection.close()


def getDriver():
    global driver
    options = ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    # options.add_argument("disable-gpu")
    driver = Chrome(executable_path=r"/home/ubuntu/Crawler/chromedriver",
                    options=options)  # ,chrome_options=options
    driver.set_window_size(1920, 1080)


def openWindow(link):
    # driver.maximize_window()
    global driver
    driver.get(link)
    print("A")
    driver.implicitly_wait(5)
    driver.delete_cookie('PREF')
    driver.add_cookie({'domain': '.youtube.com', 'httpOnly': False,
                       'name': 'PREF',
                       'value': 'gl=US&hl=en',
                       'path': '/'})
    driver.get(link)
    driver.implicitly_wait(5)


def getChannelInfo(link):
    print(link)
    global channel_savedata
    global driver

    try:
        driver.find_elements_by_xpath(
            '/html/body/ytd-app/ytd-popup-container/iron-dropdown/div/yt-bubble-hint-renderer/div[2]/div[2]/yt-button-renderer/a/paper-button/yt-formatted-string')[
            0].click()
    except:
        pass

    driver.find_elements_by_xpath('//*[@id="tabsContent"]//*[contains(., "About")]')[0].click()
    WebDriverWait(driver, 3).until(lambda x: x.find_element_by_id("description-container"))
    html = BeautifulSoup(driver.page_source, 'html.parser')
    channel_title = html.find("yt-formatted-string", {"id": "text", "class": "ytd-channel-name"}).getText()

    try:
        channel_subscriber_count = html.find("yt-formatted-string", {"id": "subscriber-count"}).getText()
    except:
        log("this channel has no subscriber")
        channel_subscriber_count = -1

    channel_savedata = pd.concat([channel_savedata, pd.DataFrame([{'channel_name': channel_title,
                                                                   'channel_url': link,
                                                                   'subscriber_num': channel_subscriber_count,
                                                                   'check_time': time.time()}])],
                                 ignore_index=True)


def pre_process(text):
    temp = bytearray(text.encode('UTF-8'))
    temp.replace(b'\x00', b'')
    temp = temp.decode('utf-8', 'ignore')
    return re.sub("'", "''", temp)


def toSql():
    global channel_savedata, link

    conn = pg2.connect(database="createtrend", user="muna", password="muna112358!", host="13.124.107.195", port="5432")
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # 채널 정보 저장 sql
        for index, row in channel_savedata.iterrows():
            cur.execute(f"""SELECT idx FROM channel WHERE channel_url='{row["channel_url"]}';""")
            channel_idx = cur.fetchall()[0][0]
            sql = \
                f"""UPDATE channel
                    SET channel_name        = '{pre_process(row["channel_name"])}',
                        channel_description = '{pre_process(row["channel_description"])}',
                        channel_start_date  = to_date('{row["channel_start_date"]}', 'Mon DD, YYYY')
                    WHERE idx = {channel_idx};

                    INSERT INTO channel_subscriber (channel_idx, subscriber_num, check_time)
                    VALUES ({channel_idx}, '{row["subscriber_num"]}', to_timestamp({row["check_time"]}) + interval '9 hour');"""
            cur.execute(sql)
    except Exception as e:
        log(e)
        channel_savedata.to_csv(f'./logs/{link[link.rfind("/") + 1:]}_channel_savedata.csv')
        raise Exception('channel sql error')

    conn.commit()


def main(LINK):
    global driver, link, channel_savedata

    channel_savedata.drop(channel_savedata.index, inplace=True)

    driver = None
    link = None

    try:
        link = LINK
        getDriver()
        openWindow(link)
        getChannelInfo(link)
        toSql()
        driver.quit()
        return True
    except Exception as e:
        log(f'Crawler Error on {LINK} : ' + str(e))
        driver.quit()
        return False


if __name__ == '__main__':
    main('https://www.youtube.com/c/BJ%EC%8C%88%EC%9A%A9')
