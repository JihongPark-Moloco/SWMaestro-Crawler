import requests
import json
import re
import traceback
import psycopg2 as pg2


def do(video_id):
    try:
        IP = '13.124.107.195'

        USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36"

        cookies = requests.cookies.create_cookie(domain='.youtube.com', name='PREF', value='gl=US&hl=en')

        session = requests.Session()
        session.headers['User-Agent'] = USER_AGENT
        session.cookies.set_cookie(cookies)

        res = session.get(f'https://www.youtube.com/watch?v={video_id}')
        result = json.loads(res.text.split(r'window["ytInitialData"] = ')[1].split(';')[0])
        view = \
            result['contents']['twoColumnWatchNextResults']['results']['results']['contents'][0][
                'videoPrimaryInfoRenderer'][
                'viewCount']['videoViewCountRenderer']['viewCount']['simpleText'].split(' ')[0]
        view_count = int(re.sub(',', '', view))

        conn = pg2.connect(database="createtrend", user="muna", password="muna112358!", host=IP,
                           port="5432")
        conn.autocommit = False
        cur = conn.cursor()
        sql = \
            f"""INSERT INTO video_views (video_idx, views, check_time)
                VALUES ((SELECT idx FROM video WHERE video_id = '{video_id}'),'{view_count}', CURRENT_TIMESTAMP);"""

        cur.execute(sql)
        conn.commit()
        conn.close()

        print(f'{video_id}, {view_count}')

        return True
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        print(traceback.format_exc())
        print('Error:', str(e))

        return False
