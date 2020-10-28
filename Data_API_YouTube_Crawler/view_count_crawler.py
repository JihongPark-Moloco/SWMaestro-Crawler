import re
import sys
import time
import traceback
import psycopg2 as pg2


def do(video_id, session, proxies=None):
    try:
        start_time = time.time()
        IP = "13.124.107.195"

        conn = pg2.connect(
            database="createtrend", user="muna", password="muna112358!", host=IP, port="5432",
        )
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute(f"SELECT idx FROM video WHERE video_id='{video_id}';")
        video_idx = cur.fetchall()[0][0]

        try:
            res = session.get(f"https://www.youtube.com/watch?v={video_id}", proxies=proxies)
        except:
            sys.exit()

        if r'"reason":"Premieres' in res.text:
            # print("premier video")
            conn.close()
            return True

        if (
            r'"playabilityStatus":{"status":"ERROR"' in res.text
            or r'"playabilityStatus":{"status":"LOGIN_REQUIRED","messages":["This is a private video.'
            in res.text
            or r'"status":"UNPLAYABLE"' in res.text
        ):
            # print("video has error")
            sql = f"UPDATE video SET status = FALSE WHERE video_id='{video_id}';"
            cur.execute(sql)
            conn.commit()
            conn.close()
            return True

        if r"Sorry for the interruption." in res.text:
            # print("network volume error")
            conn.commit()
            conn.close()
            sys.exit()
            # raise Exception('Network Volume Error')
            return False
        try:
            if "watching now" not in res.text:
                view = res.text.split(r'{"videoViewCountRenderer":{"viewCount":{"simpleText":"')[
                    1
                ].split(" ")[0]
            else:
                view = "0"
        except:
            # print(res.text)
            sys.exit()

        try:
            view_count = int(re.sub(",", "", view))
        except:
            view_count = 0

        try:
            if r'{"iconType":"LIKE"},"defaultText":{"simpleText":"Like"},"toggledText":{"simpleText":"Like"}' in res.text:
                likes = 0
            else:
                likes = res.text.split(
                    r'{"iconType":"LIKE"},"defaultText":{"accessibility":{"accessibilityData":{"label":"'
                )[1].split(" ")[0]
                if 'No' in likes:
                    likes = 0
                else:
                    likes = int(re.sub(",", "", likes))

            if r'{"iconType":"DISLIKE"},"defaultText":{"simpleText":"Dislike"},"toggledText":{"simpleText":"Dislike"}' in res.text:
                dislikes = 0
            else:
                dislikes = res.text.split(
                    r'{"iconType":"DISLIKE"},"defaultText":{"accessibility":{"accessibilityData":{"label":"'
                )[1].split(" ")[0]
                if 'No' in dislikes:
                    dislikes = 0
                else:
                    dislikes = int(re.sub(",", "", dislikes))

            sql = f"""INSERT INTO video_likes (video_idx, likes, check_time, dislikes)
                     VALUES ((SELECT idx FROM video WHERE video_id = '{video_id}'),'{likes}', CURRENT_TIMESTAMP, '{dislikes}');"""

            cur.execute(sql)

        except Exception as e:
            # print(res.text)
            # print(e)
            # traceback.print_exc()
            conn.commit()
            conn.close()
            sys.exit()
            return False

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

        # print(f"{video_id}, {view_count}")

        # interval_time = start_time - time.time()
        # if interval_time <= 1:
        #     time.sleep(1 - interval_time)

        return True
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        # print(traceback.format_exc())
        # print("Error:", str(e))
        # with open("../file.txt", "w", encoding="UTF-8") as f:
        #     f.write(res.text)

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
