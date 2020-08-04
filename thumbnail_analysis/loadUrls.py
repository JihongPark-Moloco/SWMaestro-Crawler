#!/usr/bin/env python
import psycopg2 as pg2


def loadUrls():
    conn = None
    try:
        conn = pg2.connect(database="createtrend", user="muna", password="muna112358!", host="222.112.206.190",
                           port="5432")
        cur = conn.cursor()
        cur.execute("SELECT thumbnail_url from video where thumbnail_processed = False")
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
