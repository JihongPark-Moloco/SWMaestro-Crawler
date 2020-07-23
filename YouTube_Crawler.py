#!/usr/bin/python3

from selenium.webdriver.common.keys import Keys
from selenium.webdriver import Chrome
from selenium.webdriver import ChromeOptions
from bs4 import BeautifulSoup
import time
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait
import psycopg2 as pg2
import re

channel_savedata = pd.DataFrame(columns=['channel_name',
                                         'channel_description',
                                         'channel_url',
                                         'subscriber_num',
                                         'channel_start_date',
                                         'check_time'])

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
    global logf
    global link

    if logf == None:
        logf = open(f'{link[link.rfind("/") + 1:]}.log', 'w')

    logf.write(text)


def getDriver():
    global driver
    options = ChromeOptions()
    options.add_argument('headless')
    options.add_argument('--window-size=1920,1080')
    # options.add_argument("disable-gpu")
    driver = Chrome(executable_path="chromedriver_84_win.exe", options=options)  # ,chrome_options=options
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


def getChannelInfo(link):
    print(link)
    global channel_savedata
    global driver

    driver.find_elements_by_xpath('//*[@id="tabsContent"]/paper-tab[6]')[0].click()
    WebDriverWait(driver, 3).until(lambda x: x.find_element_by_id("description-container"))
    html = BeautifulSoup(driver.page_source, 'html.parser')
    channel_title = html.find("yt-formatted-string", {"id": "text", "class": "ytd-channel-name"}).getText()
    print(channel_title)
    try:
        channel_description = html.find("yt-formatted-string", {"id": "description"}).getText()
        channel_description = re.sub('\n', ' ', channel_description)
    except:
        log("channel description is empty")
    try:
        channel_start_date = re.search("[A-Z]{1}[a-z]{2} [0-9]{1,2}[,] [0-9]{4}",
                                       str(html.find("div", {"id": "right-column"}))).group(0)
    except:
        channel_start_date = "아직몰라"
    print(channel_start_date)

    try:
        channel_subscriber_count = html.find("yt-formatted-string", {"id": "subscriber-count"}).getText()
    except:
        log("this channel has no subscriber")
        channel_subscriber_count = -1

    channel_savedata = pd.concat([channel_savedata, pd.DataFrame([{'channel_name': channel_title,
                                                                   'channel_url': link,
                                                                   'channel_description': channel_description,
                                                                   'channel_start_date': channel_start_date,
                                                                   'subscriber_num': channel_subscriber_count,
                                                                   'check_time': time.time()}])],
                                 ignore_index=True)
    print(channel_savedata)


def scrollDownVideo():
    global driver
    driver.find_elements_by_xpath('//*[@id="tabsContent"]/paper-tab[2]')[0].click()
    body = driver.find_element_by_tag_name('body')
    WebDriverWait(driver, 3).until(lambda x: x.find_element_by_xpath('// *[ @ id = "dismissable"]'))

    # 동영상 모두 스크롤 다운
    def check_scrolled(driver):
        nonlocal current_height
        return driver.execute_script(
            'return document.querySelector("#page-manager > ytd-browse:nth-child(1)").scrollHeight;') != current_height

    end_time = time.time() + 3

    while time.time() < end_time:
        end_time = time.time() + 3
        current_height = driver.execute_script(
            'return document.querySelector("#page-manager > ytd-browse:nth-child(1)").scrollHeight;')
        body.send_keys(Keys.END)
        try:
            WebDriverWait(driver, 4).until(check_scrolled)
        except:
            pass


def scrollDownComment():
    body = driver.find_element_by_tag_name('body')

    body.send_keys(Keys.PAGE_DOWN)
    time.sleep(1)
    body.send_keys(Keys.PAGE_DOWN)
    time.sleep(1)
    body.send_keys(Keys.END)

    WebDriverWait(driver, 3).until(lambda x: x.find_element_by_xpath('//*[@id="content-text"]'))

    def check_scrolled(driver):
        nonlocal current_height
        return driver.execute_script("return document.querySelector('#primary').scrollHeight;") != current_height

    end_time = time.time() + 3

    while time.time() < end_time:
        end_time = time.time() + 3
        current_height = driver.execute_script("return document.querySelector('#primary').scrollHeight;")
        body.send_keys(Keys.END)
        try:
            WebDriverWait(driver, 4).until(check_scrolled)
        except:
            pass


def showReply():
    # 답글보기 클릭하기
    scroll_down_btns = driver.find_elements_by_xpath('//ytd-button-renderer[@id="more-replies"]/a/paper-button')
    for i in scroll_down_btns:
        i.send_keys(Keys.ENTER)
    # 답글 더보기 클릭하기
    while 1:
        continuations = driver.find_elements_by_xpath('//*[@id="continuation"]/yt-next-continuation/paper-button')
        if not len(continuations):
            break
        for i in continuations:
            try:
                i.send_keys(Keys.ENTER)
            except:
                continue
        driver.implicitly_wait(1)


def getVideoLinks():
    html0 = driver.page_source
    html = BeautifulSoup(html0, 'html.parser')
    video_list0 = html.find('div', {'class': 'style-scope ytd-grid-renderer'})
    video_list2 = video_list0.find_all('a', {'id': 'video-title'})
    base_url = 'http://www.youtube.com'  # 영상별url이 유튜브 기본 url을 포함하지 않기 때문에 꼭 확인해줘야 함
    main_url = []
    for i in range(len(video_list2)):
        url = base_url + video_list2[i]['href']
        main_url.append(url)
    return main_url


def saveData(start_url):
    global comment_savedata
    global video_savedata
    global returnData
    html_s0 = driver.page_source
    html_s = BeautifulSoup(html_s0, 'html.parser')

    # 모든 댓글 수집하기
    comment0 = html_s.find_all('div', {'id': "body", 'class': 'style-scope ytd-comment-renderer'})
    for i in range(len(comment0)):
        # 댓글
        try:
            comment_content = comment0[i].find('yt-formatted-string',
                                               {'id': 'content-text',
                                                'class': 'style-scope ytd-comment-renderer'}).text
            comment_content = re.sub('\n', ' ', comment_content)
        except:
            log(f'video({start_url}) comment#{i} raise comment_content except')
            continue
        try:
            write_date = comment0[i].find('a', {'class': 'yt-simple-endpoint style-scope yt-formatted-string'}).text
        except:
            log(f'video({start_url}) comment#{i} has no write date except')
            write_date = "null"
        try:
            likenum_text = comment0[i].find('span', {'id': 'vote-count-left'}).text
            goods = "".join(re.findall('[0-9]', likenum_text))
        except:
            goods = 0
            log(f'video({start_url}) comment#{i} likenum makes except')

        comment_savedata = pd.concat([comment_savedata, pd.DataFrame([{'video_url': start_url,
                                                                       'comment_content': comment_content,
                                                                       'likes': goods,
                                                                       'check_time': time.time(),
                                                                       'write_time': write_date}])], ignore_index=True)

    name = driver.find_elements_by_xpath('//*[@id="container"]/h1/yt-formatted-string')[0].text
    start_date = driver.find_elements_by_xpath('//*[@id="date"]/yt-formatted-string')[0].text

    views = html_s.find('span', {'class': 'view-count'}).getText()
    likes = html_s.find_all('yt-formatted-string',
                            {'id': 'text', 'class': 'style-scope ytd-toggle-button-renderer style-text'})[
        0].getText()
    dislikes = html_s.find_all('yt-formatted-string',
                               {'id': 'text', 'class': 'style-scope ytd-toggle-button-renderer style-text'})[
        1].getText()

    try:
        driver.find_element_by_tag_name('body').send_keys(Keys.HOME)
        time.sleep(1.0)
        driver.find_element_by_tag_name('body').send_keys(Keys.PAGE_DOWN)
        time.sleep(0.5)
        driver.find_element_by_xpath('//*[@id="more"]/yt-formatted-string').click()
    except:
        print("do not find a show button")
        print(start_url)
        # print(html_s0)

    try:
        description = driver.find_element_by_xpath('//*[@id="description"]/yt-formatted-string').text
        description = re.sub('\n', ' ', description)
    except:
        log(f'video({start_url}) makes no description exception')
        description = "null"

    video_savedata = pd.concat([video_savedata, pd.DataFrame([{'video_name': name,
                                                               'video_description': description,
                                                               'video_url': start_url,
                                                               'upload_time': start_date,
                                                               'likes': likes,
                                                               'dislikes': dislikes,
                                                               'check_time': time.time(),
                                                               'views': views}])], ignore_index=True)


def startCrawling(links):
    global comment_savedata
    global video_savedata
    global channel_savedata
    for number_of_url in range(len(links)):  # 리스트로 만들어져 있는 url중 한개의 url을 이용 range(len(links))
        start_url = links[number_of_url]
        driver.get(start_url)
        scrollDownComment()
        # showReply()
        saveData(start_url)


def pre_process(text):
    return re.sub("'", "''", text)


def toSql():
    global channel_savedata, video_savedata, comment_savedata

    channel_savedata.to_csv('channel_savedata.csv')
    video_savedata.to_csv('video_savedata')
    comment_savedata.to_csv('comment_savedata.csv')

    conn = pg2.connect(database="createtrend", user="muna", password="muna112358!", host="13.124.107.195", port="5432")
    conn.autocommit = False
    cur = conn.cursor()

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

    # 비디오 정보 저장
    for index, row in video_savedata.iterrows():
        views = re.sub(",", "", (row['views'])[:-5])

        sql = f"""INSERT INTO video (video_name, video_description, video_url, upload_time, channel_idx)
                  VALUES ('{pre_process(row["video_name"])}', '{pre_process(row["video_description"])}', '{row["video_url"]}', to_timestamp('{row["upload_time"]}', 'Mon DD, YYYY'), '{channel_idx}')
                  RETURNING idx"""
        cur.execute(sql)
        video_idx = cur.fetchall()[0][0]

        sql = f"""INSERT INTO video_likes (video_idx, likes, check_time, dislikes) 
                  VALUES ('{video_idx}', '{row["likes"]}', to_timestamp({row["check_time"]}) + interval '9 hour', {row["dislikes"]});
                  INSERT INTO video_views (video_idx, views, check_time) 
                  VALUES ('{video_idx}', '{views}', to_timestamp({row["check_time"]}) + interval '9 hour');"""
        cur.execute(sql)

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

    conn.commit()


def main(LINK):
    try:
        global link
        link = LINK
        getDriver()
        openWindow(link)
        getChannelInfo(link)
        scrollDownVideo()
        links = getVideoLinks()
        startCrawling(links)
        driver.quit()
        toSql()
        return True
    except:
        log(f'Crawler Error on {LINK}')
        return False


if __name__ == '__main__':
    main('https://www.youtube.com/channel/UCvS-d8Ntsny2H8UeINNsQhw')
