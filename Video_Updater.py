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

video_savedata = pd.DataFrame(columns=['video_name',
                                       'video_description',
                                       'video_url',
                                       'upload_time',
                                       'likes',
                                       'dislikes',
                                       'check_time',
                                       'views'])

comment_savedata = pd.DataFrame(columns=['video_url',
                                         'comment_content',
                                         'likes',
                                         'check_time',
                                         'write_time'])

driver = None
logf = None
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
    driver.get(link + "/videos")
    driver.implicitly_wait(5)


def scrollDownComment(start_url):
    body = driver.find_element_by_tag_name('body')

    body.send_keys(Keys.PAGE_DOWN)
    time.sleep(1)
    body.send_keys(Keys.PAGE_DOWN)
    time.sleep(1)
    body.send_keys(Keys.END)

    try:
        WebDriverWait(driver, 3).until(lambda x: x.find_element_by_xpath('//*[@id="content-text"]'))
    except:
        log('no comment')
        log({start_url})
        return

    def check_comment_number(driver):
        lists = driver.find_elements_by_xpath(
            '''/html/body/ytd-app/div[@id='content']/ytd-page-manager[@id='page-manager']/ytd-watch-flexy[@class='style-scope ytd-page-manager hide-skeleton']/div[@id='columns']/div[@id='primary']/div[@id='primary-inner']/ytd-comments[@id='comments']/ytd-item-section-renderer[@id='sections']/div[@id='contents']/ytd-comment-thread-renderer[@class='style-scope ytd-item-section-renderer']''')
        return len(lists)

    number = 0
    while number < 100:
        before_number = number
        number = check_comment_number(driver)

        if before_number == number:
            break

        body.send_keys(Keys.END)
        time.sleep(1)

    # def check_scrolled(driver):
    #     nonlocal current_height
    #     return driver.execute_script("return document.querySelector('#primary').scrollHeight;") != current_height
    #
    # end_time = time.time() + 3
    #
    # while time.time() < end_time:
    #     end_time = time.time() + 3
    #     current_height = driver.execute_script("return document.querySelector('#primary').scrollHeight;")
    #     body.send_keys(Keys.END)
    #     try:
    #         WebDriverWait(driver, 4).until(check_scrolled)
    #     except:
    #         pass


def saveData(start_url):
    global comment_savedata
    global video_savedata
    global returnData

    html_s0 = driver.page_source
    html_s = BeautifulSoup(html_s0, 'html.parser')

    views = html_s.find('span', {'class': 'view-count'}).getText()
    likes = html_s.find_all('yt-formatted-string',
                            {'id': 'text', 'class': 'style-scope ytd-toggle-button-renderer style-text'})[
        0].getText()
    dislikes = html_s.find_all('yt-formatted-string',
                               {'id': 'text', 'class': 'style-scope ytd-toggle-button-renderer style-text'})[
        1].getText()

    if "ike" in likes:
        likes = -1
    if "ike" in dislikes:
        dislikes = -1

    video_savedata = pd.concat([video_savedata, pd.DataFrame([{'video_url': start_url,
                                                               'likes': likes,
                                                               'dislikes': dislikes,
                                                               'check_time': time.time(),
                                                               'views': views}])], ignore_index=True)

    # 모든 댓글 수집하기
    comment0 = html_s.find_all('div', {'id': "body", 'class': 'style-scope ytd-comment-renderer'})
    for i in range(len(comment0)):
        # 댓글
        try:
            comment_content = comment0[i].find('yt-formatted-string',
                                               {'id': 'content-text',
                                                'class': 'style-scope ytd-comment-renderer'}).text
            comment_content = re.sub('\n', ' ', comment_content)
        except Exception as e:
            log(f'video({start_url}) comment#{i} raise comment_content except')
            log(e)
            continue

        try:
            likenum_text = comment0[i].find('span', {'id': 'vote-count-left'}).text
            goods = "".join(re.findall('[0-9]', likenum_text))
        except Exception as e:
            goods = 0
            log(f'video({start_url}) comment#{i} likenum makes except')
            log(e)

        comment_savedata = pd.concat([comment_savedata, pd.DataFrame([{'video_url': start_url,
                                                                       'comment_content': comment_content,
                                                                       'likes': goods,
                                                                       'check_time': time.time()}])], ignore_index=True)


def pre_process(text):
    temp = bytearray(text.encode('UTF-8'))
    temp.replace(b'\x00', b'')
    temp = temp.decode('utf-8', 'ignore')
    return re.sub("'", "''", temp)


def toSql():
    global video_savedata, comment_savedata, link

    conn = pg2.connect(database="createtrend", user="muna", password="muna112358!", host="13.124.107.195", port="5432")
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # 비디오 정보 저장
        for index, row in video_savedata.iterrows():
            views = re.sub(",", "", (row['views'])[:-5])

            if 'No' in views:
                views = 0

            if 'ago' in row["upload_time"]:
                sql = f"""INSERT INTO video (video_name, video_description, video_url, upload_time, channel_idx)
                          VALUES ('{pre_process(row["video_name"])}', '{pre_process(row["video_description"])}', '{row["video_url"]}', to_timestamp({time.time()}) + interval '9 hour' - interval '{row["upload_time"][:-4]}', '{channel_idx}')
                          RETURNING idx"""
            else:
                sql = f"""INSERT INTO video (video_name, video_description, video_url, upload_time, channel_idx)
                          VALUES ('{pre_process(row["video_name"])}', '{pre_process(row["video_description"])}', '{row["video_url"]}', to_timestamp('{row["upload_time"]}', 'Mon DD, YYYY'), '{channel_idx}')
                          RETURNING idx"""
            cur.execute(sql)
            video_idx = cur.fetchall()[0][0]

            sql = f"""INSERT INTO video_likes (video_idx, likes, check_time, dislikes) 
                      VALUES ('{video_idx}', '{row["likes"]}', to_timestamp({row["check_time"]}) + interval '9 hour', '{row["dislikes"]}');
                      INSERT INTO video_views (video_idx, views, check_time) 
                      VALUES ('{video_idx}', '{views}', to_timestamp({row["check_time"]}) + interval '9 hour');"""
            cur.execute(sql)
    except Exception as e:
        log(e)
        video_savedata.to_csv(f'./logs/{link[link.rfind("/") + 1:]}_video_savedata.csv')
        raise Exception('video sql error')

    try:
        # 댓글 정보 저장
        for index, row in comment_savedata.iterrows():
            write_time = row["write_time"]

            interval_time = write_time[:write_time.find(' ago')]

            sql = f"""SELECT idx FROM video WHERE video_url='{row["video_url"]}'"""
            cur.execute(sql)
            video_idx = cur.fetchall()[0][0]

            sql = f"""INSERT INTO comment (video_idx, comment_content, write_time) 
                      VALUES ('{video_idx}', '{pre_process(row["comment_content"])}', to_timestamp({row["check_time"]}) + interval '9 hour' - interval '{interval_time}')
                      RETURNING idx;"""
            cur.execute(sql)
            comment_idx = cur.fetchall()[0][0]

            sql = f"""INSERT INTO comment_likes (comment_idx, likes, check_time) 
                      VALUES ('{comment_idx}', '{row["likes"]}', to_timestamp({row["check_time"]}) + interval '9 hour');"""
            cur.execute(sql)
    except Exception as e:
        log(e)
        comment_savedata.to_csv(f'./logs/{link[link.rfind("/") + 1:]}_comment_savedata.csv')
        raise Exception('comment sql error')

    conn.commit()


def main(LINK):
    global driver, link, video_savedata, comment_savedata

    video_savedata.drop(video_savedata.index, inplace=True)
    comment_savedata.drop(comment_savedata.index, inplace=True)

    driver = None
    link = None

    try:
        link = LINK
        getDriver()
        openWindow(link)
        toSql()
        driver.quit()
        return True
    except Exception as e:
        log(f'Crawler Error on {LINK} : ' + str(e))
        driver.quit()
        return False


if __name__ == '__main__':
    main('https://www.youtube.com/c/BJ%EC%8C%88%EC%9A%A9')
