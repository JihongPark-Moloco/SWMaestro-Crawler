#!/usr/bin/env python

from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import Chrome
from selenium.webdriver import ChromeOptions
from bs4 import BeautifulSoup
import time
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait
from sqlalchemy import create_engine
import sqlalchemy
import re

channel_savedata = pd.DataFrame(columns=['channel_name',
                                         'channel_description',
                                         'channel_url',
                                         'subscriber_num',
                                         'channel_start_date'])

video_savedata = pd.DataFrame(columns=['video_name',
                                       'video_description',
                                       'video_url',
                                       'upload_date',
                                       'likes',
                                       'dislikes',
                                       'check_date',
                                       'views'])

comment_savedata = pd.DataFrame(columns=['video_url',
                                         'comment_content',
                                         'likes',
                                         'check_date',
                                         'write_date'])

driver = None
logf = None
link = None


def log(text):
    global logf
    global link

    if logf == None:
        logf = open(f'{link[link.rfind("/") + 1:]}.log', 'w')

    logf.write(text)


def getCrawlingLink():
    return input("링크를 입력해주세요.")


def getDriver():
    global driver
    # options = ChromeOptions()
    # options.add_argument('headless')
    # options.add_argument('window-size=1920x1080')
    # options.add_argument("disable-gpu")
    driver = Chrome(executable_path="chromedriver_84_win.exe")  # ,chrome_options=options


def openWindow(link):
    # driver.maximize_window()
    driver.get(link)
    driver.implicitly_wait(5)


def getChannelInfo(link):
    global channel_savedata

    driver.find_elements_by_xpath('//*[@id="tabsContent"]/paper-tab[6]')[0].click()
    WebDriverWait(driver, 3).until(lambda x: x.find_element_by_id("description-container"))
    html = BeautifulSoup(driver.page_source, 'html.parser')
    channel_title = html.find("yt-formatted-string", {"id": "text", "class": "ytd-channel-name"}).getText()
    try:
        channel_description = html.find("yt-formatted-string", {"id": "description"}).getText()
        channel_description = re.sub('\n', ' ', channel_description)
    except:
        log("channel description is empty")

    channel_start_date = re.search("[0-9]{4}[.] *[0-9]{1,2}[.] *[0-9]{1,2}[.] *",
                                   str(html.find("div", {"id": "right-column"}))).group(0)

    try:
        channel_subscriber_count = html.find("yt-formatted-string", {"id": "subscriber-count"}).getText()
    except:
        log("this channel has no subscriber")
        channel_subscriber_count = -1

    channel_savedata = pd.concat([channel_savedata, pd.DataFrame([{'channel_name': channel_title,
                                                                   'channel_url': link,
                                                                   'channel_description': channel_description,
                                                                   'channel_start_date': channel_start_date,
                                                                   'subscriber_num': channel_subscriber_count}])],
                                 ignore_index=True)


def scrollDownVideo():
    driver.find_elements_by_xpath('//*[@id="tabsContent"]/paper-tab[2]')[0].click()
    body = driver.find_element_by_tag_name('body')
    WebDriverWait(driver, 3).until(lambda x: x.find_element_by_xpath('// *[ @ id = "dismissable"]'))

    # 동영상 모두 스크롤 다운
    # SCROLL_PAUSE_TIME = 0.3
    def check_scrolled(driver):
        nonlocal current_height
        return driver.execute_script(
            'return document.querySelector("#page-manager > ytd-browse:nth-child(1)").scrollHeight;') != current_height

    end_time = time.time() + 2

    while time.time() < end_time:
        end_time = time.time() + 2
        current_height = driver.execute_script(
            'return document.querySelector("#page-manager > ytd-browse:nth-child(1)").scrollHeight;')
        body.send_keys(Keys.END)
        try:
            WebDriverWait(driver, 3).until(check_scrolled)
        except:
            pass


def scrollDownComment():
    body = driver.find_element_by_tag_name('body')

    body.send_keys(Keys.END)
    time.sleep(0.5)
    body.send_keys(Keys.END)
    time.sleep(0.5)
    body.send_keys(Keys.END)
    time.sleep(0.5)
    body.send_keys(Keys.END)

    WebDriverWait(driver, 3).until(lambda x: x.find_element_by_xpath('//*[@id="content-text"]'))

    def check_scrolled(driver):
        nonlocal current_height
        return driver.execute_script("return document.querySelector('#primary').scrollHeight;") != current_height

    end_time = time.time() + 2

    while time.time() < end_time:
        end_time = time.time() + 2
        current_height = driver.execute_script("return document.querySelector('#primary').scrollHeight;")
        body.send_keys(Keys.END)
        try:
            WebDriverWait(driver, 3).until(check_scrolled)
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
                                                                       'check_date': time.time(),
                                                                       'write_date': write_date}])], ignore_index=True)

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
        driver.find_element_by_xpath('/html/body/ytd-app/div/ytd-page-manager/ytd-watch-flexy/div[4]/div[1]/div/div[8]/div[3]/ytd-video-secondary-info-renderer/div/ytd-expander/paper-button[2]/yt-formatted-string').click()
    except:
        pass

    try:
        description = driver.find_element_by_xpath('//*[@id="description"]/yt-formatted-string').text
        description = re.sub('\n', ' ', description)
    except:
        log(f'video({start_url}) makes no description exception')
        description = "null"

    video_savedata = pd.concat([video_savedata, pd.DataFrame([{'video_name': name,
                                                               'video_description': description,
                                                               'video_url': start_url,
                                                               'upload_date': start_date,
                                                               'likes': likes,
                                                               'dislikes': dislikes,
                                                               'check_date': time.time(),
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
    comment_savedata.to_csv("comment_log.csv", index=False)
    video_savedata.to_csv("video_log.csv", index=False)
    channel_savedata.to_csv("channel_log.csv", index=False)

    #
    # def toSql():
    #     global comment_savedata
    #     global video_savedata
    #     engine = create_engine(
    #         "postgresql://muna:muna112358!@ec2-13-124-107-195.ap-northeast-2.compute.amazonaws.com:5432/test")
    #     channel_savedata.to_sql(name='video',
    #                             con=engine,
    #                             if_exists='append',
    #                             index=False,
    #                             dtype={
    #                                 'channel_name': sqlalchemy.types.VARCHAR(45),
    #                                 'channel_description': sqlalchemy.types.VARCHAR(50),
    #                                 'channel_addr': sqlalchemy.types.VARCHAR(500),
    #                                 'channel_owner': sqlalchemy.types.VARCHAR(100),
    #                                 'subscriber_num': sqlalchemy.types.INTEGER(),
    #                                 'channel_start_date': sqlalchemy.DateTime(),
    #                                 'need_process': sqlalchemy.types.BOOLEAN(),
    #                                 'check_date': sqlalchemy.DateTime()
    #                             })
    #
    #     video_savedata.to_sql(name='video',
    #                           con=engine,
    #                           if_exists='append',
    #                           index=False,
    #                           dtype={
    #                               'video_name': sqlalchemy.types.VARCHAR(50),
    #                               'video_description': sqlalchemy.types.VARCHAR(500),
    #                               'channel_idx': sqlalchemy.types.INTEGER(),
    #                               'video_addr': sqlalchemy.types.VARCHAR(100),
    #                               'upload_date': sqlalchemy.DateTime(),
    #                               'need_process': sqlalchemy.types.BOOLEAN()
    #
    #                           })


def main():
    # link = getCrawlingLink()
    global link
    link = "https://www.youtube.com/channel/UCvS-d8Ntsny2H8UeINNsQhw"
    getDriver()
    openWindow(link)
    getChannelInfo(link)
    scrollDownVideo()
    links = getVideoLinks()
    startCrawling(links)
    driver.quit()
    # toSql()


main()
# if __name__ == "__main__":
#     main()
