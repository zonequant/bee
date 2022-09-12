#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/9/5 22:42
# @Author  : Dominolu
# @File    : trades.py
# @Software: PyCharm
import asyncio
import traceback

import aiohttp

from bee.aiowebsocket import Websocket
from dynaconf import Dynaconf
import time
from loguru import logger as log
import pymysql
from bee.tools import *
import dateutil.parser
from dateutil import tz,parser

class Ftx_liqudation(Websocket):
    url="wss://ftx.com/ws/"
    db=None

    def __init__(self):
        super(Ftx_liqudation, self).__init__()
        self.confg=Dynaconf(settings_files=['settings.yaml', '.secrets.yaml'])
        self.create_pool()

    def create_pool(self):
        host=self.confg["host"]
        user=self.confg["user"]
        pwd=self.confg['password']
        db=self.confg['db']
        self.db=pymysql.connect(host=host, user=user, password=pwd, database=db)

    def start(self):
        """
        """
        self.init(self.url, self.connected_callback, self.process_callback, self.process_binary_callback)
        self._loop.create_task(self._connect())


    async def ping(self):
        while True:
            try:
                await self.send({'op': 'ping'})
                await asyncio.sleep(15)
            except:
                log.error("send ping error!")


    async def connected_callback(self):
        url="https://ftx.com/api/markets"
        async with aiohttp.request("GET", url) as r:
            reponse = await r.json()
            data=reponse["result"]
            for i in data:
                if i["type"]=="future":
                    symbol=i["name"]
                    await self.send({'op': 'subscribe', 'channel': 'trades', 'market': symbol})
        # await self.send({'op': 'subscribe', 'channel': 'trades'})
        self._loop.create_task(self.ping())


    async def process_callback(self,data):
        sql_trader = "insert into trades(broker,symbol,side,price,volume,amount,ts) values (%s,%s,%s,%s,%s,%s,%s)"
        sql_liq = "insert into liquidation(broker,symbol,side,price,volume,ts) values (%s,%s,%s,%s,%s,%s)"
        # print(data)
        if "channel" in data:
            symbol = data.get("market")
            data=data.get("data",None)
            if data:
                traders=[]
                liqs=[]
                for i in data:
                    liquidation=i.get("liquidation")
                    side = i["side"]
                    price = float(i["price"])
                    volume = float(i["size"])
                    amount=volume*price
                    ts =parser.parse(i["time"])
                    tz_sh = tz.gettz('Asia/Shanghai')
                    ts=ts.astimezone(tz_sh)
                    ts=ts.strftime("%Y-%m-%d %H:%M:%S.%f")
                    # traders.append(("ftx",symbol,side,price,volume,amount,ts))
                    if liquidation:
                        liqs.append(("ftx",symbol,side,price,volume,ts))
                # if len(traders)>0:
                    # self.execute_db(sql_trader,traders)
                if len(liqs)>0:
                    self.execute_db(sql_liq,liqs)


    def execute_db(self, sql,param=None):
        """更新/新增/删除"""
        try:
            # 检查连接是否断开，如果断开就进行重连
            self.db.ping(reconnect=True)
            with self.db.cursor() as cursor:
                if param:
                    cursor.executemany(sql, param)
                else:
                    cursor.execute(sql)
                self.db.commit()

        except Exception as e:
            log.debug(sql)
            log.debug(param)
            traceback.print_exc()
            # 回滚所有更改
            self.db.rollback()

if __name__ == '__main__':

    spider=Ftx_liqudation()
    spider.start()
    while True:
        time.sleep(0.1)