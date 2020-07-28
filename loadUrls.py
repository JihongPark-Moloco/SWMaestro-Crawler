#!/usr/bin/env python
import psycopg2 as pg2


def loadUrls():
    conn = None
    try:
        conn = pg2.connect(database="createtrend", user="muna", password="muna112358!", host="13.124.107.195",
                           port="5432")
        cur = conn.cursor()
        cur.execute("SELECT channel_url from channel where need_process = True and channel_name is null")
        rows = cur.fetchall()
        newrows = [row[0] for row in rows]
        [print(row) for row in newrows]
        return newrows

    except Exception as e:
        print("postgresql database conn error")
        print(e)
    finally:
        if conn:
            conn.close()
