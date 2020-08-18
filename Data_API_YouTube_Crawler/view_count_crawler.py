import requests
import json
import re
import traceback
import psycopg2 as pg2
import time


def do(video_id, session, proxies=None):
    try:
        start_time = time.time()
        IP = "13.124.107.195"

        conn = pg2.connect(
            database="createtrend",
            user="muna",
            password="muna112358!",
            host=IP,
            port="5432",
        )
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute(f"SELECT idx FROM video WHERE video_id='{video_id}';")
        video_idx = cur.fetchall()[0][0]

        res = session.get(
            f"https://www.youtube.com/watch?v={video_id}", proxies=proxies
        )

        if r'"reason":"Premieres' in res.text:
            print("premier video")
            conn.close()
            return True

        if (
            r'"playabilityStatus":{"status":"ERROR"' in res.text
            or r'"playabilityStatus":{"status":"LOGIN_REQUIRED","messages":["This is a private video.'
            in res.text
            or r'"status":"UNPLAYABLE"' in res.text
        ):
            print("video has error")
            sql = f"UPDATE video SET status = FALSE WHERE video_id='{video_id}';"
            cur.execute(sql)
            conn.commit()
            conn.close()
            return True

        if r"Sorry for the interruption." in res.text:
            print("notwork volume error")
            conn.commit()
            conn.close()
            return False

        view = res.text.split(
            r'{"videoViewCountRenderer":{"viewCount":{"simpleText":"'
        )[1].split(" ")[0]
        view_count = int(re.sub(",", "", view))

        tags = []
        for tag in res.text.split(r'<meta property="og:video:tag" content="')[1:]:
            tag = tag.split('">')[0]
            tags.append(f"({video_idx}, '{tag[:99]}'),")

        sql = f"""INSERT INTO video_views (video_idx, views, check_time)
                VALUES ((SELECT idx FROM video WHERE video_id = '{video_id}'),'{view_count}', CURRENT_TIMESTAMP);"""

        if len(tags) >= 1:
            sql += " ".join(
                [
                    "INSERT INTO video_keyword_new (video_idx, keyword) VALUES",
                    "".join(tags)[:-1],
                    "ON CONFLICT DO NOTHING;",
                ]
            )

        cur.execute(sql)
        conn.commit()
        conn.close()

        print(f"{video_id}, {view_count}")

        # interval_time = start_time - time.time()
        # if interval_time <= 1:
        #     time.sleep(1 - interval_time)

        return True
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        print(traceback.format_exc())
        print("Error:", str(e))
        with open("../file.txt", "w", encoding="UTF-8") as f:
            f.write(res.text)

        # interval_time = start_time - time.time()
        # if interval_time <= 1:
        #     time.sleep(1 - interval_time)

        return False


# proxies = {"http": "http://5.79.73.131:13040", "https": "http://5.79.73.131:13040"}
# USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36"
# cookies = requests.cookies.create_cookie(
#     domain=".youtube.com", name="PREF", value="gl=US&hl=en"
# )
# session = requests.Session()
# session.headers["User-Agent"] = USER_AGENT
# session.cookies.set_cookie(cookies)
# do("lL7VznCnsMI", session, proxies)
