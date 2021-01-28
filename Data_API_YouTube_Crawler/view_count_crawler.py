"""
New_YouTube_Crawler_proxy_APP에서 호출되는 영상 조회수 수집 크롤러입니다.
"""

import re
import sys
import traceback

import psycopg2 as pg2

database = #database
user = #uesr
password = #password
IP = "13.124.107.195"
id = #id
pw = #pw


def do(video_id, session, proxies=None):
    """
    영상 조회수를 수집해 DB에 반영합니다.
    :param video_id: 영상 고유 ID 값
    :param session: requests 동작시 사용할 session 값
    :param proxies: 프록시 수행을 위한 설정 값
    :return: None
    """
    try:
        conn = pg2.connect(
            database = database,
            user = user, 
            password = password,
            host=IP,
            port="5432",
        )
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute(f"SELECT idx FROM video WHERE video_id='{video_id}';")
        video_idx = cur.fetchall()[0][0]

        try:
            res = session.get(f"https://www.youtube.com/watch?v={video_id}", proxies=proxies)
        except:
            sys.exit()

        # 공개 예정 영상
        if r'"reason":"Premieres' in res.text:
            print("premier video")
            conn.close()
            return True

        # 비공개 처리된 영상
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

        # 네트워크 트래픽 초과 에러
        if r"Sorry for the interruption." in res.text:
            print("network volume error")
            conn.commit()
            conn.close()
            sys.exit()
            raise Exception("Network Volume Error")
            return False
        
        # 실시간 방송중인 영상
        try:
            if "watching now" not in res.text:
                view = res.text.split(r'{"videoViewCountRenderer":{"viewCount":{"simpleText":"')[
                    1
                ].split(" ")[0]
            else:
                view = "0"
        except:
            sys.exit()

        try:
            view_count = int(re.sub(",", "", view))
        except:
            view_count = 0

        # 좋아요와 싫어요수 수집
        try:
            if (
                r'{"iconType":"LIKE"},"defaultText":{"simpleText":"Like"},"toggledText":{"simpleText":"Like"}'
                in res.text
            ):
                likes = 0
            else:
                likes = res.text.split(
                    r'{"iconType":"LIKE"},"defaultText":{"accessibility":{"accessibilityData":{"label":"'
                )[1].split(" ")[0]
                if "No" in likes:
                    likes = 0
                else:
                    likes = int(re.sub(",", "", likes))

            if (
                r'{"iconType":"DISLIKE"},"defaultText":{"simpleText":"Dislike"},"toggledText":{"simpleText":"Dislike"}'
                in res.text
            ):
                dislikes = 0
            else:
                dislikes = res.text.split(
                    r'{"iconType":"DISLIKE"},"defaultText":{"accessibility":{"accessibilityData":{"label":"'
                )[1].split(" ")[0]
                if "No" in dislikes:
                    dislikes = 0
                else:
                    dislikes = int(re.sub(",", "", dislikes))

            sql = f"""INSERT INTO video_likes (video_idx, likes, check_time, dislikes)
                     VALUES ((SELECT idx FROM video WHERE video_id = '{video_id}'),'{likes}', CURRENT_TIMESTAMP, '{dislikes}');"""

            cur.execute(sql)

        except Exception as e:
            traceback.print_exc()
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

        return True
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        print(traceback.format_exc())
        with open("../file.txt", "w", encoding="UTF-8") as f:
            f.write(res.text)

        return False
