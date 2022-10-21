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
from clickhouse_driver import Client
import datetime, dateutil.parser
import traceback
from dynaconf import Dynaconf


cg = CoinGeckoAPI()
config=Dynaconf(settings_files=['settings.yaml', '.secrets.yaml'])
db =Client(host=config["host"],user=config["user"],password=config["password"],database=config["db"])

lenght=15


usd=["usdt","usdc","tusd","busd","cusdc","ust","dai"]

def run():
    coins = cg.get_coins_markets(vs_currency="usd")
    dt = pd.DataFrame(coins)
    cap = dt["market_cap"].sum()
    values = []
    for i in coins:
        symbol = i["symbol"]
        if symbol not in usd:
            day_time =dateutil.parser.parse(dateutil.parser.parse(i["last_updated"]).strftime("%Y-%m-%d"))
            i["rank"] = i["market_cap"] / cap
            values.append((symbol, day_time, i["current_price"], i["market_cap"], i['market_cap_rank'], i["rank"],
                           i['total_volume'], i['high_24h'], i["low_24h"],dateutil.parser.parse(i["last_updated"]),i["price_change_percentage_24h"]))
    sql_insert = "insert into markets values"
    day_time = values[0][1]
    sql = "select count(dt) from markets where dt=toDateTime(%(date)s)"
    dt=db.execute(sql, {'date':day_time})
    if len(dt) > 0:
        sql = "alter table markets delete  where dt=toDateTime(%(date)s)"
        db.execute(sql, {'date':day_time})
    db.execute(sql_insert, values,types_check=True)

    log.info("更新数据成功！")
    highlow(day_time,cap)


def highlow(day_time,cap):
    refdate=day_time-datetime.timedelta(days=lenght)
    sql = "SELECT symbol,max(high),min(low) FROM markets WHERE dt  >=toDateTime(%(st)s) and dt <toDateTime(%(ed)s) group by symbol"
    ref_data=db.execute(sql,{"st":refdate, "ed":day_time})
    hl={}
    for i in ref_data:
        hl[i[0]]=i
    highs=0
    lows=0
    sql = "select symbol,high,low from markets where dt=toDateTime(%(date)s)"
    dt=db.execute(sql, {'date':day_time})
    for i in list(dt):
        if i[0] in hl:
            high = hl[i[0]][1]
            low = hl[i[0]][2]
            if i[1] > high:
                highs += 1
            if i[2] < low:
                lows += 1
    sql='select dt from highlows where dt=toDateTime(%(date)s)'
    dt=db.execute(sql,{'date':day_time})
    if len(dt)>0:
        sql = "alter table highlows delete  where dt=toDateTime(%(date)s)"
        db.execute(sql, {'date':day_time})
    sql="INSERT INTO highlows  VALUES"
    db.execute(sql,[[day_time,cap,highs,lows]],types_check=True)
    log.info("更新前高前低数据成功！")


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