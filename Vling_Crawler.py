from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.common.keys import Keys
import time
import pandas as pd

button_xpath = '//*[@id="ETC"]'
file_name = 'channels/ETC'

options = webdriver.ChromeOptions()
options.add_argument("--window-size=1920,1080")
driver = webdriver.Chrome("chromedriver_84_win.exe", options=options)
driver.get("http://www.vling.net/")
driver.implicitly_wait(10)

assert "블링" in driver.title

driver.find_element_by_xpath('//*[@id="root"]/div/div/header/div/div/div/div[1]/div[2]/a[1]').click()
driver.find_element_by_xpath('//*[@id="username"]').send_keys("qkrwlghddlek@naver.com")
driver.find_element_by_xpath('//*[@id="password"]').send_keys("/a515951")
driver.find_element_by_xpath('/html/body/div/div/div/div[1]/div/div[2]/div[2]/div[7]/button').click()

time.sleep(1)

try:
    driver.find_element_by_xpath(button_xpath).click()
except:
    try:
        driver.find_element_by_xpath('//*[@id="search-next-btn"]').click()
        driver.find_element_by_xpath(button_xpath).click()
    except:
        driver.find_element_by_xpath('//*[@id="search-next-btn"]').click()
        driver.find_element_by_xpath(button_xpath).click()

time.sleep(1)
driver.find_element_by_xpath('/html/body/div/div/div/div[3]/div[2]/button').click()

time.sleep(1)

# Get scroll height
last_height = driver.execute_script("return document.body.scrollHeight")

while True:
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(1)

    driver.execute_script("window.scrollTo(0, document.body.scrollHeight-50);")
    time.sleep(1)

    new_height = driver.execute_script("return document.body.scrollHeight")

    if new_height == last_height:
        break

    last_height = new_height

html = BeautifulSoup(driver.page_source, 'html.parser')
channel_list = html.find_all('div', {'class': 'portlet-wrap channel-list-portle-wrap'})

df = pd.DataFrame(
    columns=["channel_title", "channel_link", "channel_subscriber", "channel_subscriber_plus", "channel_view_count",
             "channel_view_count_plus"])

for channel in channel_list:
    channel_info = channel.find_all('div', {'class': 'channel-list-status-count'})
    channel_info_plus = channel.find_all('div', {'class': 'channel-list-sattus-count-bottom-plus'})

    channel_subscriber = channel_info[0].getText()
    channel_subscriber_plus = channel_info_plus[0].getText()
    channel_view_count = channel_info[1].getText()
    channel_view_count_plus = channel_info_plus[1].getText()

    try:
        channel_title = channel.find('div', {'class': 'channel-list-title'}).getText()
    except:
        channel_title = channel.find('div', {'class': 'channel-list-title-with-new-mark'}).getText()
    channel_link = channel.find('div', {'class': 'channel-list-content-link'}).find('a')['href']

    df = pd.concat(
        [df, pd.DataFrame([[
            channel_title, channel_link, channel_subscriber, channel_subscriber_plus, channel_view_count,
            channel_view_count_plus]],
            columns=["channel_title", "channel_link", "channel_subscriber", "channel_subscriber_plus",
                     "channel_view_count",
                     "channel_view_count_plus"])], ignore_index=True)

df.to_csv(file_name, encoding="UTF-8")

driver.close()
