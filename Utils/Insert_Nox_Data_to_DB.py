"""
Nox_Crawler로 수집한 정보를 DB에 입력합니다.
"""

import pandas as pd
import psycopg2 as pg2

# NoxInfluencer 사이트의 카테고리 목록
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

original_df = pd.read_csv("channel.csv", header=None)
original_channels = set(original_df[0].values)

all_counts = 0

for name in ll:
    converted_name = name.replace("%20%26%20", "-")
    file_name = converted_name + ".csv"
    df = pd.read_csv(file_name)

    plus_channels = set(df["channel_id"].values)

    conn = pg2.connect(
        database=None,
        user=None,
        password=None,
        host=None,
        port=None,
    )
    conn.autocommit = False
    cur = conn.cursor()
    sql = f"""SELECT count(*) FROM channel WHERE temp='{converted_name}'"""
    cur.execute(sql)
    counts = cur.fetchall()[0][0]

    for channel_id in (plus_channels - original_channels):
        sql = f"""INSERT INTO channel (channel_id, temp) VALUES ('{channel_id}', '{converted_name}');"""
        cur.execute(sql)
    conn.commit()
    conn.close()

    print(f"#################")
    print(file_name)
    print(f"current {converted_name} channel num: {counts}")
    print(f"plus channels num: {len(plus_channels)}")
    print(f"duplicate channels num: {len(original_channels & plus_channels)}")
    print(f"insertable channels num: {len(plus_channels) - len(original_channels & plus_channels)}")
    print(f"after insert channels num: {counts + len(plus_channels) - len(original_channels & plus_channels)}")

    all_counts += counts

print(all_counts)
