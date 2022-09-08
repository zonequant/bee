#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/8/12 22:21
# @Author  : Dominolu
# @File    : liquidation.py
# @Software: PyCharm
import asyncio
import json
import time
import traceback
from bee.tools import *
from loguru import logger as log
import requests
from bee.spider import Spider
from datetime import datetime

class Okex(Spider):
    start_urls=["https://www.okx.com/api/v5/public/instruments?instType=SWAP"]
    name="okex"
    request_param = {"limit": 50} #访问频率 毫秒/次
    markets={}
    period=60 #多少周期开始一次更新 秒

    def __init__(self):
        super(Okex, self).__init__()
        reponse=requests.get(self.start_urls[0])
        data=reponse.json()
        data = data["data"]
        for i in data:
            symbol = i["uly"]
            if i["ctValCcy"] != "USD":
                ctVal = i["ctVal"]
                self.markets[symbol] = float(ctVal)



    async def parse_liquidation(self,data):
        # todo 解析数据生成items
        response = data.get("response")
        data=json.loads(response)
        data=data["data"][0]
        symbol=data["uly"]
        details=data["details"]
        items=[]
        for i in details:
            side=i.get("side")
            sz=float(i.get("sz"))
            price = float(i.get("bkPx"))
            sz=self.markets[symbol]*sz
            ts=datetime.fromtimestamp(int(i.get('ts'))/1000)
            item={"broker":self.name,"symbol":symbol,"price":price,"side":side.upper(),"volume":sz,"ts":ts}
            items.append(item)
        if len(items)>0:
            ts=items[-1]["ts"]
            ts=int(ts.timestamp()*1000)+100
            self.next(symbol, ts)
        else:
            ts=get_timestamp_ms()
            self.next(symbol, ts,True)
        return items

    def next(self, symbol,ts,delay=False):
        """
        处理解析数据后的逻辑，生成新请求&存储数据
        """
        url = "https://www.okx.com/api/v5/public/liquidation-orders"
        param = {"uly": symbol, "instType": "SWAP", "state": "filled", "before": ts}
        if delay:
            next_ts=get_timestamp_ms()+self.period*1000
        else:
            next_ts = get_timestamp_ms()+1000
        self.request_delay(next_ts,url=url, data=param,method="GET", callback="parse_liquidation")
        # log.info(f"推入延时队列{next_ts}-{param}")

    async def pipe_item(self, items):
        # todo 保存数据库
        await self.db.process_item("liquidation",items)

    async def parse(self, data):
        """
        断点续传
        """
        try:
            await self.reset_queue()
            response = data.get("response")
            url="https://www.okx.com/api/v5/public/liquidation-orders"
            data=json.loads(response)
            data=data["data"]
            # symbol="BTC-USDT"
            # param = await self.get_param(symbol)
            # await self.request(url, data=param, callback="parse_liquidation")
            for i in data:
                symbol=i["uly"]
                if i["ctValCcy"]!="USD":
                    param = await self.get_param(symbol)
                    await self.request(url,data=param,callback="parse_liquidation")
                    # log.info(f"new request {url}-{param}")
        except:
            log.error(traceback.format_exc())

    async def reset_queue(self):
        # todo 检查任务队列中是否存在对应的下载任务
        key = self.name + ":in"
        await self.queue.delete(key)

    async def get_param(self,symbol):
        # todo 从数据中获取最近需要下载的参数
        sql="select max(ts) from liquidation where symbol=%s and broker=%s"
        ds=await self.db.select(sql,[symbol,self.name])
        rs=ds[0]
        if rs[0]==None:
            param = {"uly": symbol,"instType":"SWAP","state":"filled", "before": 0}
        else:
            t=int(rs[0].timestamp()*1000)+100
            param={"uly":symbol,"instType":"SWAP","state":"filled","before":t}
        return param



if __name__ == '__main__':

    spider=Okex()
    spider.run()