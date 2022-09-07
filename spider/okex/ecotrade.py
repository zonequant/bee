#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/9/6 10:30
# @Author  : Dominolu
# @File    : ecotrade.py
# @Software: PyCharm

import json
import time
import traceback
from bee.tools import *
from loguru import logger as log

from bee.spider import Spider
from datetime import datetime


class Okex_trade(Spider):
    start_urls=["https://www.okx.com/priapi/v5/ecotrade/public/popular-star-rate?t=1640970000000&size=1000&num=1"]
    name="okex_trade"

    async def parse_trade_records(self,data):
        "历史记录"

    async def parse_pos(self,data):
        "当前仓位"

    async def parse_pnl(self,data):
        "历史收益率"

    async def parse(self, data):
        """
        断点续传
        """
        user_list = []
        try:
            await self.reset_queue()
            response = data.get("response")
            data=json.loads(response)
            ranks=data["data"][0]["ranks"]
            if len(ranks) == 0:
                return
            else:
                for item in ranks:
                    nick_name = item["nickName"]  # 昵称
                    uniquename = item["uniqueName"]  # 用户标识
                    yieldrate = item["yieldRate"]  # 收益率
                    shortlever = item["shortLever"]  # 空头持仓杠杆
                    longlever = item["longLever"]  # 多头持仓杠杆
                    user_list.append({
                        "nick_name": nick_name,
                        "unique_name": uniquename,
                        "yield_rate": yieldrate,
                        "short_lever": shortlever,
                        "long_lever": longlever
                    })
                return user_list

            # symbol="BTC-USDT"
            # param = await self.get_param(symbol)
            # await self.request(url, data=param, callback="parse_liquidation")
            url = "https://www.okx.com/api/v5/public/liquidation-orders"
            for i in ranks:
                symbol=i["uly"]
                if i["ctValCcy"]!="USD":
                    await self.request(url,data=param,callback="parse_liquidation")
        except:
            log.error(traceback.format_exc())


    async def reset_queue(self):
        # todo 检查任务队列中是否存在对应的下载任务
        key = self.name + ":in"
        await self.queue.delete(key)