"""
Selenium 기반의 유튜브 크롤러입니다.
과도한 CPU 접유율과 불안정성으로 현재는 사용하지 않습니다.
"""

import re
import time
import traceback

import pandas as pd
import psycopg2 as pg2
from bs4 import BeautifulSoup
from selenium.webdriver import Chrome
from selenium.webdriver import ChromeOptions
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait

channel_savedata = pd.DataFrame(
    columns=[
        "channel_name",
        "channel_description",
        "channel_url",
        "subscriber_num",
        "channel_start_date",
        "check_time",
    ]
)

video_savedata = pd.DataFrame(
    columns=[
        "video_name",
        "video_description",
        "video_url",
        "upload_time",
        "likes",
        "dislikes",
        "check_time",
        "views",
    ]
)

comment_savedata = pd.DataFrame(
    columns=["video_url", "comment_content", "likes", "check_time", "write_time"]
)

driver = None
logf = None
link = None
too_old_switch = False


def log(text):
    global logf
    global link

    if logf == None:
        logf = open(f'./logs/{link[link.rfind("/") + 1:]}.log', "w")

    logf.write(str(text) + "\n")


def getDriver():
    global driver
    options = ChromeOptions()
    # options.add_argument('--headless')
    # options.add_argument('--no-sandbox')
    options.add_argument("--enable-automation")
    # options.headless = True
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-features=VizDisplayCompositor")
    # options.add_argument('--disable-dev-shm-usage')
    # options.add_argument("disable-gpu")

    # driver = Chrome(executable_path=r"/home/ubuntu/Crawler/chromedriver",
    driver = Chrome(
        executable_path=r"D:\Cloud\Project\PyCharm\muna-3\Selenium_YouTube_Crawler\chromedriver.exe",
        options=options,
    )  # ,chrome_options=options
    driver.set_window_size(1920, 1080)


def openWindow(link):
    # driver.maximize_window()
    global driver
    driver.get(link)
    print("A")
    driver.implicitly_wait(5)
    driver.delete_cookie("PREF")
    driver.add_cookie(
        {
            "domain": ".youtube.com",
            "httpOnly": False,
            "name": "PREF",
            "value": "gl=US&hl=en",
            "path": "/",
        }
    )
    driver.get(link)
    driver.implicitly_wait(5)


def getChannelInfo(link):
    print(link)
    global channel_savedata
    global driver

    try:
        driver.find_elements_by_xpath(
            "/html/body/ytd-app/ytd-popup-container/iron-dropdown/div/yt-bubble-hint-renderer/div[2]/div[2]/yt-button-renderer/a/paper-button/yt-formatted-string"
        )[0].click()
    except:
        pass

    driver.find_elements_by_xpath('//*[@id="tabsContent"]//*[contains(., "About")]')[0].click()
    WebDriverWait(driver, 7).until(lambda x: x.find_element_by_id("description-container"))
    html = BeautifulSoup(driver.page_source, "html.parser")
    channel_title = html.find(
        "yt-formatted-string", {"id": "text", "class": "ytd-channel-name"}
    ).getText()
    print(channel_title)
    try:
        channel_description = html.find("yt-formatted-string", {"id": "description"}).getText()
        channel_description = re.sub("\n", " ", channel_description)
    except:
        # log("channel description is empty")
        pass
    try:
        channel_start_date = re.search(
            "[A-Z]{1}[a-z]{2} [0-9]{1,2}[,] [0-9]{4}", str(html.find("div", {"id": "right-column"}))
        ).group(0)
    except:
        # log("channel description is empty")
        pass
    print(channel_start_date)

    try:
        channel_subscriber_count = html.find(
            "yt-formatted-string", {"id": "subscriber-count"}
        ).getText()
    except:
        # log("this channel has no subscriber")
        channel_subscriber_count = -1

    channel_savedata = pd.concat(
        [
            channel_savedata,
            pd.DataFrame(
                [
                    {
                        "channel_name": channel_title,
                        "channel_url": link,
                        "channel_description": channel_description,
                        "channel_start_date": channel_start_date,
                        "subscriber_num": channel_subscriber_count,
                        "check_time": time.time(),
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    # print(channel_savedata)


def scrollDownVideo():
    global driver, too_old_switch
    driver.find_elements_by_xpath('//*[@id="tabsContent"]/paper-tab[2]')[0].click()

    WebDriverWait(driver, 7).until(
        lambda x: x.find_element_by_xpath(
            """/html/body/ytd-app/div[@id='content']/ytd-page-manager[@id='page-manager']/ytd-browse[@class='style-scope ytd-page-manager'][1]/ytd-two-column-browse-results-renderer[@class='style-scope ytd-browse grid grid-6-columns']/div[@id='primary']/ytd-section-list-renderer[@class='style-scope ytd-two-column-browse-results-renderer']/div[@id='contents']/ytd-item-section-renderer[@class='style-scope ytd-section-list-renderer']/div[@id='contents']"""
        )
    )
    body = driver.find_element_by_tag_name("body")

    try:
        driver.find_elements_by_xpath(
            """/html/body/ytd-app/div[@id='content']/ytd-page-manager[@id='page-manager']/ytd-browse[@class='style-scope ytd-page-manager']/ytd-two-column-browse-results-renderer[@class='style-scope ytd-browse grid grid-6-columns']/div[@id='primary']/ytd-section-list-renderer[@class='style-scope ytd-two-column-browse-results-renderer']/div[@id='contents']/ytd-item-section-renderer[@class='style-scope ytd-section-list-renderer'][3]/div[@id='contents']/ytd-shelf-renderer[@class='style-scope ytd-item-section-renderer']/div[@id='dismissable']/div[@id='contents']/ytd-grid-renderer[@class='style-scope ytd-shelf-renderer']/yt-formatted-string[@id='view-all']/a[@class='yt-simple-endpoint style-scope yt-formatted-string']"""
        )[0].click()
        WebDriverWait(driver, 7).until(
            lambda x: x.find_element_by_xpath(
                """/html/body/ytd-app/div[@id='content']/ytd-page-manager[@id='page-manager']/ytd-browse[@class='style-scope ytd-page-manager'][1]/ytd-two-column-browse-results-renderer[@class='style-scope ytd-browse grid grid-6-columns']/div[@id='primary']/ytd-section-list-renderer[@class='style-scope ytd-two-column-browse-results-renderer']/div[@id='contents']/ytd-item-section-renderer[@class='style-scope ytd-section-list-renderer']/div[@id='contents']"""
            )
        )
    except:
        pass

    lists = driver.find_elements_by_xpath(
        """/html/body/ytd-app/div[@id='content']/ytd-page-manager[@id='page-manager']/ytd-browse[@class='style-scope ytd-page-manager']/ytd-two-column-browse-results-renderer[@class='style-scope ytd-browse grid grid-6-columns']/div[@id='primary']/ytd-section-list-renderer[@class='style-scope ytd-two-column-browse-results-renderer']/div[@id='contents']/ytd-item-section-renderer[@class='style-scope ytd-section-list-renderer']/div[@id='contents']/ytd-grid-renderer[@class='style-scope ytd-item-section-renderer']/div[@id='items']/ytd-grid-video-renderer[@class='style-scope ytd-grid-renderer']"""
    )

    for ll in lists[0].text.split("\n"):
        if "ago" in ll:
            first_video_upload_date = ll

    if "first_video_upload_date" not in locals():
        for ll in lists[1].text.split("\n"):
            if "ago" in ll:
                first_video_upload_date = ll

    if "year" in first_video_upload_date or (
        "month" in first_video_upload_date and int(first_video_upload_date.split(" ")[0]) > 4
    ):
        too_old_switch = True
        # log('latest video uploaded a month ago')
        return

    def check_last_video_upload_date(driver):
        lists = driver.find_elements_by_xpath(
            """/html/body/ytd-app/div[@id='content']/ytd-page-manager[@id='page-manager']/ytd-browse[@class='style-scope ytd-page-manager']/ytd-two-column-browse-results-renderer[@class='style-scope ytd-browse grid grid-6-columns']/div[@id='primary']/ytd-section-list-renderer[@class='style-scope ytd-two-column-browse-results-renderer']/div[@id='contents']/ytd-item-section-renderer[@class='style-scope ytd-section-list-renderer']/div[@id='contents']/ytd-grid-renderer[@class='style-scope ytd-item-section-renderer']/div[@id='items']/ytd-grid-video-renderer[@class='style-scope ytd-grid-renderer']"""
        )

        for ll in lists[-1].text.split("\n"):
            if "ago" in ll:
                last_video_upload_date = ll
        try:
            if "year" in last_video_upload_date or (
                "month" in last_video_upload_date and int(last_video_upload_date.split(" ")[0]) > 4
            ):
                return True
            else:
                return False
        except:
            return True

    while True:
        if check_last_video_upload_date(driver):
            break
        body.send_keys(Keys.END)
        time.sleep(1)

    # end_time = time.time() + 3
    #
    # while time.time() < end_time:
    #     end_time = time.time() + 3
    #     current_height = driver.execute_script(
    #         'return document.querySelector("#page-manager > ytd-browse:nth-child(1)").scrollHeight;')
    #     body.send_keys(Keys.END)
    #     try:
    #         WebDriverWait(driver, 4).until(check_scrolled)
    #     except:
    #         pass


def scrollDownComment(start_url):
    body = driver.find_element_by_tag_name("body")

    body.send_keys(Keys.PAGE_DOWN)
    time.sleep(1)
    body.send_keys(Keys.PAGE_DOWN)
    time.sleep(1)
    body.send_keys(Keys.END)

    try:
        WebDriverWait(driver, 3).until(lambda x: x.find_element_by_xpath('//*[@id="content-text"]'))
    except:
        # log('no comment')
        # log({start_url})
        return

    def check_comment_number(driver):
        lists = driver.find_elements_by_xpath(
            """/html/body/ytd-app/div[@id='content']/ytd-page-manager[@id='page-manager']/ytd-watch-flexy[@class='style-scope ytd-page-manager hide-skeleton']/div[@id='columns']/div[@id='primary']/div[@id='primary-inner']/ytd-comments[@id='comments']/ytd-item-section-renderer[@id='sections']/div[@id='contents']/ytd-comment-thread-renderer[@class='style-scope ytd-item-section-renderer']"""
        )
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


def showReply():
    # 답글보기 클릭하기
    scroll_down_btns = driver.find_elements_by_xpath(
        '//ytd-button-renderer[@id="more-replies"]/a/paper-button'
    )
    for i in scroll_down_btns:
        i.send_keys(Keys.ENTER)
    # 답글 더보기 클릭하기
    while 1:
        continuations = driver.find_elements_by_xpath(
            '//*[@id="continuation"]/yt-next-continuation/paper-button'
        )
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
    html = BeautifulSoup(html0, "html.parser")
    video_list0 = html.find("div", {"class": "style-scope ytd-grid-renderer"})
    video_list2 = video_list0.find_all("a", {"id": "video-title"})
    base_url = "http://www.youtube.com"  # 영상별url이 유튜브 기본 url을 포함하지 않기 때문에 꼭 확인해줘야 함
    main_url = []
    for i in range(len(video_list2)):
        url = base_url + video_list2[i]["href"]
        main_url.append(url)
    return main_url


def saveData(start_url):
    global comment_savedata
    global video_savedata
    global returnData

    html_s0 = driver.page_source
    html_s = BeautifulSoup(html_s0, "html.parser")

    name = driver.find_elements_by_xpath('//*[@id="container"]/h1/yt-formatted-string')[0].text
    start_date = driver.find_elements_by_xpath('//*[@id="date"]/yt-formatted-string')[0].text

    if "watching" in start_date:
        return

    if "Streamed live" in start_date:
        if "ago" in start_date:
            start_date = start_date[start_date.find("live ") + 5 :]
        else:
            start_date = start_date[start_date.find("on ") + 3 :]

    if "Premiered" in start_date:
        start_date = start_date[start_date.find("Premiered ") + 10 :]

    if "Premieres" in start_date:
        start_date = start_date[start_date.find("Premieres ") + 10 :]

    views = html_s.find("span", {"class": "view-count"}).getText()
    likes = html_s.find_all(
        "yt-formatted-string",
        {"id": "text", "class": "style-scope ytd-toggle-button-renderer style-text"},
    )[0].getText()
    dislikes = html_s.find_all(
        "yt-formatted-string",
        {"id": "text", "class": "style-scope ytd-toggle-button-renderer style-text"},
    )[1].getText()

    if "ike" in likes:
        likes = -1
    if "ike" in dislikes:
        dislikes = -1

    try:
        driver.find_element_by_tag_name("body").send_keys(Keys.HOME)
        time.sleep(1.0)
        driver.find_element_by_tag_name("body").send_keys(Keys.PAGE_DOWN)
        time.sleep(0.5)
        driver.find_element_by_xpath('//*[@id="more"]/yt-formatted-string').click()
    except:
        pass
        # log("do not find a show button")
        # log(start_url)
        # print(html_s0)

    try:
        description = driver.find_element_by_xpath(
            '//*[@id="description"]/yt-formatted-string'
        ).text
        description = re.sub("\n", " ", description)
    except Exception as e:
        # log(f'video({start_url}) makes no description exception')
        # log(e)
        description = "null"

    video_savedata = pd.concat(
        [
            video_savedata,
            pd.DataFrame(
                [
                    {
                        "video_name": name,
                        "video_description": description,
                        "video_url": start_url,
                        "upload_time": start_date,
                        "likes": likes,
                        "dislikes": dislikes,
                        "check_time": time.time(),
                        "views": views,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )

    # 모든 댓글 수집하기
    comment0 = html_s.find_all("div", {"id": "body", "class": "style-scope ytd-comment-renderer"})
    for i in range(len(comment0)):
        # 댓글
        try:
            comment_content = (
                comment0[i]
                .find(
                    "yt-formatted-string",
                    {"id": "content-text", "class": "style-scope ytd-comment-renderer"},
                )
                .text
            )
            comment_content = re.sub("\n", " ", comment_content)

            if len(comment_content) >= 7900:
                comment_content = comment_content[:7900] + "..."

        except Exception as e:
            # log(f'video({start_url}) comment#{i} raise comment_content except')
            # log(e)
            continue
        try:
            write_date = (
                comment0[i]
                .find("a", {"class": "yt-simple-endpoint style-scope yt-formatted-string"})
                .text
            )
        except Exception as e:
            # log(f'video({start_url}) comment#{i} has no write date except')
            # log(e)
            write_date = "null"
        try:
            likenum_text = comment0[i].find("span", {"id": "vote-count-left"}).text
            goods = "".join(re.findall("[0-9]", likenum_text))
        except Exception as e:
            goods = 0
            # log(f'video({start_url}) comment#{i} likenum makes except')
            # log(e)

        comment_savedata = pd.concat(
            [
                comment_savedata,
                pd.DataFrame(
                    [
                        {
                            "video_url": start_url,
                            "comment_content": comment_content,
                            "likes": goods,
                            "check_time": time.time(),
                            "write_time": write_date,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )


def startCrawling(links):
    global comment_savedata
    global video_savedata
    global channel_savedata
    for number_of_url in range(len(links)):  # 리스트로 만들어져 있는 url중 한개의 url을 이용 range(len(links))
        start_url = links[number_of_url]
        driver.get(start_url)
        try:
            WebDriverWait(driver, 5).until(
                lambda x: x.find_elements_by_xpath('//*[@id="date"]/yt-formatted-string')
            )
        except:
            pass
        try:
            if (
                "streaming"
                in driver.find_elements_by_xpath('//*[@id="date"]/yt-formatted-string')[0].text
            ):
                continue
        except:
            pass

        scrollDownComment(start_url)
        # showReply()
        saveData(start_url)


def pre_process(text):
    temp = bytearray(text.encode("UTF-8"))
    temp.replace(b"\x00", b"")
    temp = temp.decode("utf-8", "ignore")
    re.sub('"', " ", temp)
    return re.sub("'", "''", temp)


def toSql():
    global channel_savedata, video_savedata, comment_savedata, link

    # channel_savedata.to_csv('channel_savedata.csv')
    # video_savedata.to_csv('video_savedata')
    # comment_savedata.to_csv('comment_savedata.csv')

    conn = pg2.connect(
        database="createtrend", user="muna", password="muna112358!", host="54.180.25.4", port="5432"
    )
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # 채널 정보 저장 sql
        for index, row in channel_savedata.iterrows():
            cur.execute(f"""SELECT idx FROM channel WHERE channel_url='{row["channel_url"]}';""")
            channel_idx = cur.fetchall()[0][0]
            sql = f"""UPDATE channel
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
        raise Exception("channel sql error")

    try:
        # 비디오 정보 저장
        for index, row in video_savedata.iterrows():
            views = re.sub(",", "", (row["views"])[:-5])

            if "No" in views:
                views = 0

            if "ago" in row["upload_time"]:
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
        raise Exception("video sql error")

    try:
        # 댓글 정보 저장
        for index, row in comment_savedata.iterrows():
            write_time = row["write_time"]

            interval_time = write_time[: write_time.find(" ago")]

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
        raise Exception("comment sql error")

    conn.commit()
    conn.close()


def main(LINK):
    global driver, logf, link, channel_savedata, video_savedata, comment_savedata, too_old_switch

    channel_savedata.drop(channel_savedata.index, inplace=True)
    video_savedata.drop(video_savedata.index, inplace=True)
    comment_savedata.drop(comment_savedata.index, inplace=True)

    driver = None
    logf = None
    link = None
    too_old_switch = False

    try:
        link = LINK
        getDriver()
        openWindow(link)
        getChannelInfo(link)
        scrollDownVideo()
        if not too_old_switch:
            links = getVideoLinks()
            startCrawling(links)
            print("ABC")
        toSql()
        driver.quit()
        return True
    except Exception as e:
        error = traceback.format_exc()
        log(f"Crawler Error on {LINK}")
        log(e)
        log(error)
        log(driver.current_url)
        log(driver.page_source)
        logf.close()
        driver.quit()
        return False


if __name__ == "__main__":
    main(
        "https://www.youtube.com/c/%EB%AF%B8%EC%BD%A5%EC%9D%98%EB%8B%A4%EB%82%98%EC%99%80%EB%A6%AC%EB%B7%B0"
    )
