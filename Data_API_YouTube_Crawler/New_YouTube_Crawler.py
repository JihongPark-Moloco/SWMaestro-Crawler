"""
크롤러 동작을 구현한 크롤러 클래스 소스입니다.
"""

import re
import time
import traceback

import psycopg2 as pg2
import requests
from selenium.webdriver import Chrome
from selenium.webdriver import ChromeOptions

from Data_API_YouTube_Crawler import New_YouTube_Crawler_Comment


class YouTube_Crawler:
    api_key = None
    kwonjun_api_key = None
    kyungsu_api_key = None
    is_driver = False
    IP = #IP
    database = #database
    user = #user
    password = #password

    def __init__(self, api_key=None):
        if api_key is not None:
            self.api_key = api_key

    def make_driver_ready(self):
        options = ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--enable-automation")
        # options.headless = True
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-features=VizDisplayCompositor")
        # options.add_argument('--disable-dev-shm-usage')
        # options.add_argument("disable-gpu")

        self.driver = Chrome(
            executable_path=r"/home/ubuntu/Crawler/chromedriver",
            # self.driver = Chrome(executable_path=r"chromedriver",
            options=options,
        )  # ,chrome_options=options
        self.driver.set_window_size(1920, 1080)
        self.driver.get("https://www.youtube.com/")
        self.driver.implicitly_wait(5)
        self.driver.delete_cookie("PREF")
        self.driver.add_cookie(
            {
                "domain": ".youtube.com",
                "httpOnly": False,
                "name": "PREF",
                "value": "gl=US&hl=en",
                "path": "/",
            }
        )
        self.driver.get("https://www.youtube.com/")
        self.driver.implicitly_wait(5)
        self.is_driver = True

    def pre_process_sql(self, text):
        # temp = bytearray(text.encode('UTF-8'))
        # temp.replace(b'\x00', b'')
        # temp = temp.decode('utf-8', 'ignore')
        # re.sub("\"", " ", temp)
        return re.sub("'", "''", text)

    def pre_process_comment(self, text):
        temp = bytearray(text.encode("UTF-8"))
        temp.replace(b"\x00", b"")
        text = temp.decode("utf-8", "ignore")
        # re.sub("\"", " ", temp)
        return re.sub("'", "''", text)

    def update_video_and_comment(self, video_id):
        if New_YouTube_Crawler_Comment.main(video_id):
            return True
        else:
            return False

    def update_channel_info(self, channel_id, api_set=0):
        if api_set == 0:
            api_key = self.api_key
        elif api_set == 1:
            api_key = self.kwonjun_api_key
        else:
            api_key = self.kyungsu_api_key

        try:
            time.sleep(0.2)
            url = f"""https://www.googleapis.com/youtube/v3/channels?part=statistics&maxResults=50&id={channel_id}&key={api_key}"""

            response = requests.get(url)
            if response.status_code != 200:
                # print("response error: ", url)
                return False

            result = response.json()
            item = dict(*result["items"])

            try:
                check = item["statistics"]["subscriberCount"]
            except:
                conn = pg2.connect(
                    database = self.database,
                    user = self.user,
                    password = self.password,
                    host = self.IP,
                    port = "5432",
                )
                conn.autocommit = False
                cur = conn.cursor()
                sql = f"""UPDATE channel SET hidden_subscriber = true WHERE channel_id = '{channel_id}';"""
                cur.execute(sql)
                conn.commit()
                conn.close()
                return True

            conn = pg2.connect(
                database = self.database,
                user = self.user,
                password = self.password,
                host = self.IP,
                port = "5432",
            )
            conn.autocommit = False
            cur = conn.cursor()

            sql = f"""INSERT INTO channel_subscriber (channel_idx, subscriber_num, check_time)
                    VALUES ((SELECT idx from channel where channel.channel_id='{channel_id}'), '{item['statistics']['subscriberCount']}', to_timestamp({time.time()}));
                    INSERT INTO channel_views (channel_idx, view_count, check_time)
                    VALUES ((SELECT idx from channel where channel.channel_id='{channel_id}'), '{item['statistics']['viewCount']}', to_timestamp({time.time()}));"""

            cur.execute(sql)
            conn.commit()
            conn.close()

            return True
        except Exception as e:
            # print(traceback.format_exc())
            # print("ERROR", e)
            return False

    def insert_channel_info(self, channel_id):
        try:
            url = f"""https://www.googleapis.com/youtube/v3/channels?part=id,snippet,contentDetails,statistics,topicDetails&maxResults=50&id={channel_id}&key={self.api_key}"""

            response = requests.get(url)
            if response.status_code != 200:
                # print("response error: ", url)
                return False

            result = response.json()
            item = dict(*result["items"])

            conn = pg2.connect(
                database = database,
                user = user,
                password = password,
                host = self.IP,
                port="5432",
            )
            conn.autocommit = False
            cur = conn.cursor()

            sql = f"""UPDATE channel
                        SET channel_name        = '{self.pre_process_sql(item['snippet']["title"])}',
                            channel_description = '{self.pre_process_sql(item['snippet']['description'])}',
                            channel_start_date  = to_date('{item['snippet']['publishedAt']}', 'YYYY-MM-DD'),
                            upload_id = '{item['contentDetails']['relatedPlaylists']['uploads']}',
                            hidden_subscriber = {item['statistics']['hiddenSubscriberCount']},
                            thumbnail_url = '{item['snippet']['thumbnails']['default']['url']}'
                        WHERE channel_id = '{channel_id}';
        
                    INSERT INTO channel_subscriber (channel_idx, subscriber_num, check_time)
                    VALUES ((SELECT idx from channel where channel.channel_id='{channel_id}'), '{item['statistics']['subscriberCount']}', to_timestamp({time.time()}));"""

            cur.execute(sql)
            conn.commit()
            conn.close()

            return True
        except Exception as e:
            # print(traceback.format_exc())
            # print("ERROR", e)
            return False

    def update_video_info(self, upload_id, interval_day=30, api_set=0):
        if api_set == 0:
            api_key = self.api_key
        elif api_set == 1:
            api_key = self.kwonjun_api_key
        else:
            api_key = self.kyungsu_api_key

        try:
            next_page_token = None
            keep_going = True

            conn = pg2.connect(
                database = self.database,
                user = self.user,
                password = self.password,
                host = self.IP,
                port = "5432",
            )
            conn.autocommit = False
            cur = conn.cursor()

            while keep_going:
                if next_page_token is None:
                    url = f"""https://www.googleapis.com/youtube/v3/playlistItems?part=id,snippet,contentDetails,status&maxResults=50&playlistId={upload_id}&key={api_key}"""
                else:
                    url = f"""https://www.googleapis.com/youtube/v3/playlistItems?part=id,snippet,contentDetails,status&maxResults=50&pageToken={next_page_token}&playlistId={upload_id}&key={api_key}"""

                response = requests.get(url)
                if response.status_code != 200:
                    pass
                    # # print("response error: ", url)
                result = response.json()

                try:
                    next_page_token = result["nextPageToken"]
                except:
                    next_page_token = None
                    keep_going = False

                for items in result["items"]:
                    item = dict(items)

                    try:
                        upload_time = time.strptime(
                            item["contentDetails"]["videoPublishedAt"], "%Y-%m-%dT%H:%M:%SZ",
                        )
                    except:
                        upload_time = time.strptime(
                            item["snippet"]["publishedAt"], "%Y-%m-%dT%H:%M:%SZ"
                        )
                        # # print(upload_time)
                        sql = f"""INSERT INTO video (channel_idx, video_id, upload_time, status)
                                VALUES ((SELECT idx from channel where upload_id = '{upload_id}'),
                                        '{item['contentDetails']['videoId']}', to_timestamp('{item['snippet']['publishedAt']}', 'YYYY-MM-DDTHH24:MI:SSZ'), FALSE)
                                ON CONFLICT DO NOTHING;"""
                        cur.execute(sql)
                        # # print("Disabled Video", item["contentDetails"]["videoId"])
                        if (time.mktime(time.localtime()) - time.mktime(upload_time)) / (
                            60 * 60 * 24
                        ) <= interval_day:
                            pass
                        else:
                            keep_going = False
                            break
                        continue

                    # 90일 이내의 영상
                    # 2020-07-31T12:05:06Z
                    if (time.mktime(time.localtime()) - time.mktime(upload_time)) / (
                        60 * 60 * 24
                    ) <= interval_day:
                        sql = f"""SELECT insert_video('{self.pre_process_sql(item['snippet']['title'])}', '{self.pre_process_sql(item['snippet']['description'])}', 
                                        '{item['contentDetails']['videoId']}', '{item['contentDetails']['videoPublishedAt']}', 
                                        '{upload_id}', '{item['snippet']['thumbnails']['high']['url']}')"""
                        cur.execute(sql)
                        success = cur.fetchone()[0]
                        if not success:
                            keep_going = False
                            break
                    else:
                        keep_going = False
                        break

            conn.commit()
            conn.close()

            return True
        except Exception as e:
            # print(traceback.format_exc())
            # print("ERROR", e)

            return False

    def __del__(self):
        if self.is_driver:
            self.driver.close()
