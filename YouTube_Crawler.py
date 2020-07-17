#!/usr/bin/env python

from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import Chrome
from selenium.webdriver import ChromeOptions
from bs4 import BeautifulSoup
import time
import re
import datetime as dt
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import re

channel_savedata = pd.DataFrame({'channel_name': [],
                                 'channel_description': [],
                                 'channel_addr':[],
                                 'channel_owner':[],
                                 'need_process':[],
                                 'subscriber_num':[],
                                 'channel_start_date': []})

video_hits_savedata=pd.DataFrame({'video_idx':[],
                                  'hits':[],
                                  'check_date':[]})
video_likes_savedata=pd.DataFrame({'video_idx':[],
                                   'likes':[],
                                   'check_date'})                                              
video_savedata = pd.DataFrame({'video_name': [],
                               'video_description': [],
                               'video_addr': [],
                               'channel_idx':[],
                               'need_process':[],
                               'upload_date': []})
comment_savedata = pd.DataFrame({'comment_content': [],
                                 'video_idx':[],
                                 'need_process':[]
                                 'write_date': []})
comment_likes_savedata = pd.DataFrame({'comment_idx':[],
                                       'likes':[],
                                       'check_date':[]})
topComment_savedata = pd.DataFrame({''})
driver = None


def getCrawlingLink():
    return input("링크를 입력해주세요.")


def getDriver():
    global driver
    # options = ChromeOptions()
    # options.add_argument('headless')
    # options.add_argument('window-size=1920x1080')
    # options.add_argument("disable-gpu")
    driver = Chrome(executable_path="/Users/jun/juny/swm/crawler/muna-crawler/chromedriver")  # ,chrome_options=options


def openWindow(link):
    # driver.maximize_window()
    driver.get(link)
    driver.implicitly_wait(2)


def getChannelInfo(link):
    global channel_savedata

    driver.find_elements_by_xpath('//*[@id="tabsContent"]/paper-tab[6]')[0].click()
    driver.implicitly_wait(2)
    html = BeautifulSoup(driver.page_source, 'html.parser')
    channel_title = html.find("yt-formatted-string", {"id": "text", "class": "ytd-channel-name"}).getText()
    channel_description = html.find("yt-formatted-string", {"id": "description"}).getText()
    channel_start_date = re.search("[0-9]{4}[.] *[0-9]{1,2}[.] *[0-9]{1,2}[.] *",
                                   str(html.find("div", {"id": "right-column"}))).group(0)

    try:
        channel_subscriber_count = html.find("yt-formatted-string", {"id": "subscriber-count"}).getText()
    except:
        channel_subscriber_count = -1

    insert_data = pd.DataFrame(
        {'channel_name': [channel_title],'channel_addr':[link] 'channel_description': [channel_description],
         'channel_owner':[],'channel_start_date': [channel_start_date],'subscriber_num':[channel_subscriber_count],'need_process':[]})

    channel_savedata = channel_savedata.append(insert_data)


def scrollDownVideo():
    driver.find_elements_by_xpath('//*[@id="tabsContent"]/paper-tab[2]')[0].click()
    body = driver.find_element_by_tag_name('body')
    driver.implicitly_wait(2)

    # 동영상 모두 스크롤 다운
    SCROLL_PAUSE_TIME = 0.3
    body.send_keys(Keys.END)
    driver.implicitly_wait(2)
    last_height = driver.execute_script(
        'return document.querySelector("#page-manager > ytd-browse:nth-child(1)").scrollHeight;')

    while True:
        # Scroll down to bottom
        body.send_keys(Keys.END)
        time.sleep(SCROLL_PAUSE_TIME)

        # Wait to load page
        driver.implicitly_wait(2)

        # Calculate new scroll height and compare with last scroll height
        new_height = driver.execute_script(
            'return document.querySelector("#page-manager > ytd-browse:nth-child(1)").scrollHeight;')

        if new_height == last_height:
            break
        last_height = new_height


def scrollDownComment():
    body = driver.find_element_by_tag_name('body')
    SCROLL_PAUSE_TIME = 0.3
    # Get scroll height
    # 댓글을 나오게 하기위에 임의적으로 키다운 한번.
    driver.implicitly_wait(2)
    body.send_keys(Keys.END)
    driver.implicitly_wait(2)

    try:
        driver.find_elements_by_xpath(
            '/html/body/ytd-app/div/ytd-page-manager/ytd-watch-flexy/div[4]/div[1]/div/div[6]/div[3]/ytd-video-secondary-info-renderer/div/ytd-expander/paper-button[2]')[
            0].send_keys(Keys.ENTER)
    except:
        print("간략히")

    last_height = driver.execute_script("return document.querySelector('#primary').scrollHeight;")
    while True:
        # Scroll down to bottom
        body.send_keys(Keys.END)
        # Wait to load page
        driver.implicitly_wait(2)
        # Calculate new scroll height and compare with last scroll height
        new_height = driver.execute_script("return document.querySelector('#primary').scrollHeight;")
        if new_height == last_height:
            break
        last_height = new_height
    # 참조 : https://stackoverflow.com/a/28928684/1316860
    driver.implicitly_wait(2)


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
    html_s0 = driver.page_source
    html_s = BeautifulSoup(html_s0, 'html.parser')
    # 모든 댓글 수집하기
    comment0 = html_s.find_all('div', {'id': "body", 'class': 'style-scope ytd-comment-renderer'})
    for i in range(len(comment0)):
        # 댓글
        try:
            comment_content = comment0[i].find('yt-formatted-string',
                                               {'id': 'content-text', 'class': 'style-scope ytd-comment-renderer'}).text
        except:
            continue
        try:
            write_date = comment0[i].find('a', {'class': 'yt-simple-endpoint style-scope yt-formatted-string'}).text
        except:
            write_date = "null"
        try:
            likenum_text = comment0[i].find('span', {'id': 'vote-count-left'}).text
            goods = "".join(re.findall('[0-9]', likenum_text))
        except:
            goods = 0
        insert_data = pd.DataFrame({'comment_content': [comment_content],
                                    'goods': [goods],
                                    'write_date': [write_date]})
        comment_savedata = comment_savedata.append(insert_data)

    name = driver.find_elements_by_xpath('//*[@id="container"]/h1/yt-formatted-string')[0].text
    view = driver.find_elements_by_xpath('//*[@id="count"]/yt-view-count-renderer/span[1]')[0].text
    start_date = driver.find_elements_by_xpath('//*[@id="date"]/yt-formatted-string')[0].text
    try:
        thumbnail = driver.find_elements_by_xpath('//*[@id="description"]/yt-formatted-string/span[3]')[0].text
    except:
        thumbnail = "null"

    video_url = start_url
    insert_data = pd.DataFrame(
        {'video_name': [name], 'hits': [view], 'upload_date': [start_date], 'video_description': [thumbnail],
         'video_addr': [video_url]})
    video_savedata = video_savedata.append(insert_data)


def startCrawling(links):
    global comment_savedata
    global video_savedata
    for number_of_url in range(len(links)):  # 리스트로 만들어져 있는 url중 한개의 url을 이용 range(len(links))
        start_url = links[number_of_url]
        driver.get(start_url)
        scrollDownComment()
        driver.implicitly_wait(2)
        # showReply()
        saveData(start_url)
    comment_savedata.to_csv("comment_log.csv", index=False)
    video_savedata.to_csv("video_log.csv", index=False)
    channel_savedata.to_csv("channel_log.csv", index=False)


def toSql():
    global comment_savedata
    global video_savedata
    engine = create_engine(
        "postgresql://muna:muna112358!@ec2-13-124-107-195.ap-northeast-2.compute.amazonaws.com:5432/test")
    channel_savedata.to_sql(name='video',
                          con=engine,
                          if_exists='append',
                          index=False,
                          dtype={
                              'channel_name':sqlalchemy.types.VARCHAR(45),
                              'channel_description': sqlalchemy.types.VARCHAR(50),
                              'channel_addr': sqlalchemy.types.VARCHAR(500),
                              'channel_owner': sqlalchemy.types.VARCHAR(100),
                              'subscriber_num':sqlalchemy.types.INTEGER(),
                              'channel_start_date': sqlalchemy.DateTime(),
                              'need_process':sqlalchemy.types.BOOLEAN(),
                              'check_date':sqlalchemy.DateTime()
                          })

    video_savedata.to_sql(name='video',
                          con=engine,
                          if_exists='append',
                          index=False,
                          dtype={
                              'video_name': sqlalchemy.types.VARCHAR(50),
                              'video_description': sqlalchemy.types.VARCHAR(500),
                              'channel_idx': sqlalchemy.types.INTEGER(),
                              'video_addr': sqlalchemy.types.VARCHAR(100),
                              'upload_date': sqlalchemy.DateTime(),
                              'need_process':sqlalchemy.types.BOOLEAN()

                          })


def main():
    link = getCrawlingLink()
    getDriver()
    openWindow(link)
    getChannelInfo(link)
    scrollDownVideo()
    links = getVideoLinks()
    startCrawling(links)
    driver.quit()
    toSql()


if __name__ == "__main__":
    main()
