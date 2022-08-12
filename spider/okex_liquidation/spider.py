#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/8/12 22:21
# @Author  : Dominolu
# @File    : spider.py
# @Software: PyCharm
import json



from bee.spider import Spider


class Okex(Spider):
    start_urls=["https://www.okx.com/api/v5/public/instruments?instType=SWAP"]

    def parse_liquidation(self,response):
        # todo 解析数据生成items
        data=json.loads(response.text)
        data=data["data"]
        symbol=data["uly"]
        details=data["details"]
        items=[]
        for i in details:
            side=i.get("side")
            sz=i.get("sz")
            ts=i.get('tz')
            item={"symbol":symbol,"side":side,"sz":sz,"ts":ts}
            items.append(item)
        return items

    def pipe_item(self, items):
        # todo 保存数据库
        # self.db.insert(items)
        pass


    def parse(self, response):
        """
        断点续传
        """
        url="https://www.okx.com/api/v5/public/liquidation-orders"
        data=json.loads(response.text)
        data=data["data"]
        for i in data:
            symbol=i["uly"]
            if self.check_symbol(symbol):
                param=self.get_param(symbol)
                self.request(url,data=param,callback="parse_liquidation")

    def check_symbol(self,symbol):
        # todo 检查任务队列中是否存在对应的下载任务
        pass

    def get_param(self,symbol):
        # todo 从数据中获取最近需要下载的参数
        pass