# -*- coding:utf-8 -*-
"""
@Time : 2021/9/16 14:50
@Author : Domionlu@gmail.com
@File : coingecko
"""
# -*- coding:utf-8 -*-
from pycoingecko import CoinGeckoAPI

import pandas as pd
import time
from loguru import logger as log
import pymysql
import datetime, dateutil.parser
import traceback

import configparser

def get_config():
    # 获取本爬虫的配制文件
    cfg=configparser.ConfigParser()
    cfg.read("./config.ini")
    return cfg


cg = CoinGeckoAPI()

config=get_config()["DB"]
db = pymysql.connect(host=config["host"],user=config["user"],password=config["password"],database=config["database"])
cursor = db.cursor()
lenght=15


usd=["usdt","usdc","tusd","busd","cusdc","ust","dai"]

def run():
    coins = cg.get_coins_markets(vs_currency="usd")
    dt = pd.DataFrame(coins)
    cap = dt["market_cap"].sum()
    sql_symbol = "select * from markets where symbol=%s and dt<%s"
    values = []
    for i in coins:
        symbol = i["symbol"]
        if symbol not in usd:
            day_time = dateutil.parser.parse(dateutil.parser.parse(i["last_updated"]).strftime("%Y-%m-%d"))
            i["rank"] = i["market_cap"] / cap
            values.append((symbol, day_time, i["current_price"], i["market_cap"], i['market_cap_rank'], i["rank"],
                           i['total_volume'], i['high_24h'], i["low_24h"],i["price_change_percentage_24h"], dateutil.parser.parse(i["last_updated"])))
    sql_insert = "insert into markets(symbol,dt,price,market_cap,market_cap_rank,cap_rank,volume,high,low,rise,last_updated) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    day_time = values[0][1]
    sql = "select count(dt) from markets where dt=%s"
    cursor.execute(sql, [day_time])
    dt = cursor.fetchall()
    if len(dt) > 0:
        sql = "delete from markets where dt=%s"
        cursor.execute(sql, [day_time])
        db.commit()
    cursor.executemany(sql_insert, values)
    db.commit()
    log.info("更新数据成功！")
    highlow(day_time,cap)


def highlow(day_time,cap):
    refdate=day_time-datetime.timedelta(days=lenght)
    sql = "SELECT symbol,max(high),min(low) FROM markets WHERE dt  >=%s and dt <%s group by symbol"
    cursor.execute(sql,[refdate, day_time])
    ref_data = cursor.fetchall()
    hl={}
    for i in ref_data:
        hl[i[0]]=i
    highs=0
    lows=0
    sql = "select symbol,high,low from markets where dt=%s"
    cursor.execute(sql, [day_time])
    dt = cursor.fetchall()
    for i in list(dt):
        if i[0] in hl:
            high = hl[i[0]][1]
            low = hl[i[0]][2]
            if i[1] > high:
                highs += 1
            if i[2] < low:
                lows += 1
    sql="INSERT INTO highlows(datetime,market_cap,highs,lows) VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE market_cap=VALUES(market_cap),highs=VALUES(highs),lows=VALUES(lows)"
    cursor.execute(sql,[day_time,cap,highs,lows])
    db.commit()
    log.info("更新前高前低数据成功！")


def his():
    sql="select dt,sum(market_cap) from markets group by dt"
    cursor.execute(sql)
    dt=cursor.fetchall()
    for i in list(dt):
        highlow(i[0],i[1])
        log.info(f"更新hl{i[0]}")
#
# def top(data):
#     tops=[]
#     volumes=[]
#     ranks=[]
#     for i in data:
#         symbol=i["symbol"]
#         c=db.find(table,{"symbol":symbol}).to_pd()
#         rank=c["rank"]
#         ref_rank=rank.shift(7)
#         tops=(ref_rank-rank)/ref_rank
#         volume=c["volume"]
#         ref_volume=volume.shift(7)
#         volume=(ref_volume-volume)/ref_volume
#         item={"rank":rank[-1],"symbol":symbol,"price":i["price"],"volume":volume[-1]}
#         tops.append(item)
#
#     db.delete("tops")
#     db.delete("ranks")
#     db.delete("volumes")
#     db.insert_many("tops",tops)
#     db.insert_many("ranks",ranks)
#     db.insert_many("volumes",volumes)


def get_categories():
    data=cg.get_coin_history_by_id("chainlink","30-12-2017")
    # data=cg.get_search_trending()
    # data=cg.get_coin_by_id("chainlink")
    print(data)
if __name__ == "__main__":
    # get_categories()
    while True:
        try:
            run()
            # his()
        except Exception as e:
            traceback.print_exc()
            log.error(e)
        time.sleep(60)