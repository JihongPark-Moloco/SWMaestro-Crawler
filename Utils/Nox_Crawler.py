"""
NoxInfluencer에서 각 카테고리별 채널 정보를 수집하는 크롤러입니다.
"""

import time

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

# NoxInfluencer 카테고리 ID
ll = [
    "nonprofits%20%26%20activism",
    "pets%20%26%20animals",
    "comedy",
    "news%20%26%20politics",
    "autos%20%26%20vehicles",
    "howto%20%26%20style",
    "science%20%26%20technology",
    "education",
    "people%20%26%20blogs",
    "sports",
    "entertainment",
    "travel%20%26%20events",
    "music",
    "gaming",
    "film%20%26%20animation",
]

for name in ll:
    file_name = name.replace("%20%26%20", "-") + ".csv"

    def do(URL):
        """
        입력한 URL에서 채널목록을 수집해 반환합니다.
        :param URL: target url
        :return: None, global DataFrame에 추가합니다.
        """
        global df

        # 채널목록이 모두 로드되도록 최하단까지 스크롤합니다.
        driver.get(URL)
        driver.implicitly_wait(10)
        body = driver.find_element_by_tag_name("body")
        body.send_keys(Keys.END)
        time.sleep(1)
        body.send_keys(Keys.END)
        time.sleep(1)
        body.send_keys(Keys.END)
        time.sleep(1)
        body.send_keys(Keys.END)
        time.sleep(1)
        body.send_keys(Keys.END)
        time.sleep(1)
        body.send_keys(Keys.END)
        time.sleep(1)
        body.send_keys(Keys.END)
        time.sleep(1)
        body.send_keys(Keys.END)
        time.sleep(1)
        body.send_keys(Keys.END)
        time.sleep(1)
        body.send_keys(Keys.END)
        time.sleep(1)
        body.send_keys(Keys.END)
        time.sleep(1)
        body.send_keys(Keys.END)
        time.sleep(1)
        body.send_keys(Keys.END)
        time.sleep(1)
        body.send_keys(Keys.END)
        time.sleep(1)
        body.send_keys(Keys.END)
        time.sleep(1)
        body.send_keys(Keys.END)
        time.sleep(1)
        body.send_keys(Keys.END)
        time.sleep(1)

        html = BeautifulSoup(driver.page_source, "html.parser")
        channel_list = html.find_all("tr", {"class": "item"})

        # 로드된 채널목록을 수집합니다.
        for channel in channel_list:
            channel_url = channel.find("td", {"class": "profile"}).find("a")["href"]
            channel_id = channel_url.split("/")[-1]

            df = pd.concat(
                [df, pd.DataFrame([[channel_id]], columns=["channel_id"])], ignore_index=True,
            )

    options = webdriver.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome("../Selenium_YouTube_Crawler/chromedriver", options=options)
    df = pd.DataFrame(columns=["channel_id"])

    # 녹스 점수순으로 상위 500명
    do(
        f"https://kr.noxinfluencer.com/youtube-channel-rank/top-500-kr-{name}-youtuber-sorted-by-noxscore-weekly"
    )
    # 평균 조회수순으로 상위 500명
    do(
        f"https://kr.noxinfluencer.com/youtube-channel-rank/top-500-kr-{name}-youtuber-sorted-by-avgview-weekly"
    )
    # 채널 구독자수 성장순으로 상위 500명
    do(
        f"https://kr.noxinfluencer.com/youtube-channel-rank/top-500-kr-{name}-youtuber-sorted-by-growth-weekly"
    )
    # 채널 조회수 순으로 상위 500명
    do(
        f"https://kr.noxinfluencer.com/youtube-channel-rank/top-500-kr-{name}-youtuber-sorted-by-views-monthly"
    )

    df.to_csv(file_name, encoding="UTF-8", index=False)

    driver.close()
