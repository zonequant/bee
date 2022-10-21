#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/9/4 00:30
# @Author  : Dominolu
# @File    : liquidation.py
# @Software: PyChar
import time
import asyncio

import aioredis
import pymysql
from bee.aiowebsocket import Websocket
from loguru import logger as log
import aiomysql
from bee.tools import *
from bee.sqlorm import *
from dynaconf import Dynaconf
from clickhouse_driver import Client


class Bybit_liqudation(Websocket):
    url="wss://stream.bybit.com/realtime"
    # url="wss://stream-testnet.bybit.com/realtime"
    subscribe='{"op":"subscribe","args":["liquidation"]}'
    db=None

    def __init__(self):
        super(Bybit_liqudation, self).__init__()
        self.confg=Dynaconf(settings_files=['settings.yaml', '.secrets.yaml'])
        self.create_pool()

    def create_pool(self):
        host=self.confg["host"]
        user=self.confg["user"]
        pwd=self.confg['password']
        db=self.confg['db']
        self.db = Client(host=host, user=user, password=pwd, database=db)

    def start(self):
        """
        """
        self.init(self.url, self.connected_callback, self.process_callback, self.process_binary_callback)
        self._loop.create_task(self._connect())
        self._loop.create_task(self.ping())



    async def connected_callback(self):
        await self.send('{"op":"subscribe","args":["liquidation"]}')

    async def process_callback(self,data):
        if "topic" in data:
            print(data["data"])
            data=data["data"]
            sql_insert = "insert into liq"
            symbol=data["symbol"]
            side = data["side"]
            price = float(data["price"])
            volume = float(data["qty"])/price
            ts = ts_to_datetime(data["time"])
            data=("bybit",symbol,side,price,volume,ts)
            self.execute_db(sql_insert,data)

    def execute_db(self, sql,param=None):
        """更新/新增/删除"""
        try:
            # 检查连接是否断开，如果断开就进行重连
            self.db.execute(sql, [param], types_check=True)
        except Exception as e:
            log.debug(sql)
            log.debug(param)
            traceback.print_exc()


    async def ping(self):

        while True:
            await self.send('{"op":"ping"}')
            # log.info("ping ok")
            await asyncio.sleep(20)

if __name__ == '__main__':

    spider=Bybit_liqudation()
    spider.start()
    while True:
        time.sleep(0.1)
#
# async def onmessage(data):
#     channel = data.get("ch")
#     if not channel:
#         if data.get("ping"):
#             hb_msg = {"pong": data.get("ping")}
#             await ws.send(hb_msg)
#         return
#     else:
#         log.info(data)
#
# #
# # async def binary_onmessage(msg):
# #     data = json.loads(gzip.decompress(msg).decode())
# #     await onmessage(data)
#
# async def conn():
#     print("连接成功")
#     await ws.send('{"op":"subscribe","args":["liquidation"]}')
#
# async def main():
#     url = "wss://stream.bybit.com/realtime"
#     ws = Websocket()
#     ws.init(url, connected_callback=conn, process_callback=onmessage)
#     await ws.conn()