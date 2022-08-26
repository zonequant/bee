# -*- coding:utf-8 -*-
"""
@Time : 2022/4/26 16:18
@Author : Domionlu@gmail.com
@File : liquidation
"""
# -*- coding:utf-8 -*-
from aq.broker.binance import Binance
from aq.common.const import *
from aq.engine.eventengine import EventManger
from aq.common.config import config
from aq.common.tools import *
import pymysql
import traceback
db = pymysql.connect(host=config.mongo.host,user=config.mongo.user,password=config.mongo.password,database=config.mongo.database)
cursor = db.cursor()

def on_liquidation(data):
    try:
        sql_insert = "insert into liquidation(broker,symbol,side,volume,price,dt) values (%s,%s,%s,%s,%s,%s)"
        values=["Binance",data["s"],data["S"],data["q"],data["p"],ts_to_datetime_str(data["T"])]
        cursor.execute(sql_insert, values)
        db.commit()
    except:
        traceback.print_exc()
    

def run():
    ev = EventManger.get_instance()
    bn = Binance(market_type=USDT_SWAP)
    bn.market.add_feed({LIQUIDATIONS:on_liquidation})
    ev.start()
if __name__ == "__main__":
    # get_categories()
    run()