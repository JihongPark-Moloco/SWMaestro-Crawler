from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.common.keys import Keys
import time
import pandas as pd
import psycopg2 as pg2

# button_xpath = '//*[@id="ETC"]'
file_name = 'channels/FILM-ANIMATION'


def do(URL):
    global df

    driver.get(URL)
    driver.implicitly_wait(10)
    body = driver.find_element_by_tag_name('body')
    body.send_keys(Keys.END)
    time.sleep(1)
    body.send_keys(Keys.END)
    time.sleep(1)
    body.send_keys(Keys.END)
    time.sleep(1)

    html = BeautifulSoup(driver.page_source, 'html.parser')
    channel_list = html.find_all('tr', {'class': 'item'})

    for channel in channel_list:
        channel_url = channel.find('td', {'class': 'profile'}).find('a')['href']
        channel_id = channel_url.split('/')[-1]

        df = pd.concat([df, pd.DataFrame([[channel_id]], columns=["channel_id"])], ignore_index=True)


options = webdriver.ChromeOptions()
options.add_argument("--window-size=1920,1080")
driver = webdriver.Chrome("chromedriver", options=options)
df = pd.DataFrame(columns=["channel_id"])

do(
    'https://kr.noxinfluencer.com/youtube-channel-rank/top-100-kr-comedy-youtuber-sorted-by-noxscore-weekly'
)
do(
    'https://kr.noxinfluencer.com/youtube-channel-rank/top-100-kr-comedy-youtuber-sorted-by-avgview-weekly'
)
do(
    'https://kr.noxinfluencer.com/youtube-channel-rank/top-100-kr-comedy-youtuber-sorted-by-growth-weekly'
)
do(
    'https://kr.noxinfluencer.com/youtube-channel-rank/top-100-kr-comedy-youtuber-sorted-by-views-monthly'
)

marker = 'comedy'

print(len(df))
df = df.drop_duplicates(keep='first')
print(len(df))

conn = pg2.connect(database="createtrend", user="muna", password="muna112358!", host="222.112.206.190",
                   port="5432")
conn.autocommit = False
cur = conn.cursor()
for index, row in df.iterrows():
    sql = \
        f"""INSERT INTO channel (channel_id, temp)
            VALUES ('{row['channel_id']}', '{marker}');"""
    cur.execute(sql)
conn.commit()
conn.close()

# df.to_csv(file_name, encoding="UTF-8", index=False)

driver.close()
