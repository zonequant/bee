# -*- coding:utf-8 -*-
"""
@Time : 2022/4/26 16:18
@Author : Domionlu@gmail.com
@File : liquidation
"""
# -*- coding:utf-8 -*-
from bee.aiowebsocket import Websocket
from loguru import logger as log
from bee.tools import *
from bee.sqlorm import *
import pymysql
import traceback
from dynaconf import Dynaconf
import time
import asyncio
from aiohttp.http import WSMsgType
from clickhouse_driver import Client

class Binance_liqudation(Websocket):
    url="wss://fstream.binance.com/ws"
    # url="wss://stream-testnet.bybit.com/realtime"
    subscribe='{"op":"subscribe","args":["liquidation"]}'
    db=None

    def __init__(self):
        super(Binance_liqudation, self).__init__()
        self.confg=Dynaconf(settings_files=['settings.yaml', '.secrets.yaml'])
        self.create_pool()

    def create_pool(self):
        host=self.confg["host"]
        user=self.confg["user"]
        pwd=self.confg['password']
        db=self.confg['db']
        self.db=Client(host=host, user=user, password=pwd, database=db)

    def start(self):
        """
        """
        self.init(self.url, self.connected_callback, self.process_callback, self.process_binary_callback)
        self._loop.create_task(self._connect())
        self._loop.create_task(self.ping())


    async def connected_callback(self):
        await self.send('{"method":"SUBSCRIBE","params":["!forceOrder@arr"],"id":1024}')

    async def process_callback(self,data):
        try:
            e=data.get("e",None)
            if "forceOrder" ==e:
                data=data["o"]
                sql_insert = "insert into liq values"
                values = ["binance", data["s"], data["S"], data["p"], data["q"], ts_to_datetime(data["T"])]
                self.db.execute(sql_insert, [values])
                # self.execute_db(sql_insert, values)
        except:
            log.error(traceback.format_exc())


    def execute_db(self, sql,param=None):
        """更新/新增/删除"""
        self.db.execute(sql,param)
        # try:
        #     # 检查连接是否断开，如果断开就进行重连
        #     self.db.ping(reconnect=True)
        #     with self.db.cursor() as cursor:
        #         if param:
        #             cursor.execute(sql, param)
        #         else:
        #             cursor.execute(sql)
        #         self.db.commit()
        #     # log.debug(sql)
        #     # log.debug(param)
        # except Exception as e:
        #     traceback.print_exc()
        #     # 回滚所有更改
        #     self.db.rollback()

    async def ping(self):
        while True:
            await self.send(WSMsgType.PONG)
            # log.info("ping ok")
            await asyncio.sleep(600)

if __name__ == '__main__':

    spider=Binance_liqudation()
    spider.start()
    while True:
        time.sleep(0.1)
#
# def on_liquidation(data):
#     try:
#         sql_insert = "insert into liquidation(broker,symbol,side,volume,price,ts) values (%s,%s,%s,%s,%s,%s)"
#         values=["Binance",data["s"],data["S"],data["q"],data["p"],ts_to_datetime_str(data["T"])]
#         cursor.execute(sql_insert, values)
#         db.commit()
#     except:
        # traceback.print_exc()
#
#
# def run():
#     ev = EventManger.get_instance()
#     bn = Binance(market_type=USDT_SWAP)
#     bn.market.add_feed({LIQUIDATIONS:on_liquidation})
#     ev.start()
# if __name__ == "__main__":
#     # get_categories()
#     run()