#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/8/10 23:50
# @Author  : Dominolu
# @File    : spider.py
# @Software: PyCharm

import json
import time
import asyncio
import traceback

from loguru import logger as log
import configparser
import aioredis
class Spider(object):
    name = ""
    start_urls=[]

    def __init__(self, queue):
        # todo 初始化配置文件与redis
        self.config=self.get_config()
        self.queue=aioredis.from_url(self.config.REDIS,decode_responses=True)


    def get_config(self):
        # 获取本爬虫的配制文件
        cfg=configparser.ConfigParser()
        cfg.read("./spide.cfg")
        return cfg

    async def start_requests(self):
        """
        初始化爬虫任务
        检查队列
        断点继爬
        """
        for url in self.start_urls:
            await self.request(url)

    async def request(self, url, data=None, method="GET", callback="parse"):
        request = {
            "url": url,
            "method": method,
            "data": data,
            "callback": callback
        }
        key = self.name + ":in"
        await self.queue.lpush(key, json.dumps(request))

    async def start(self):
        """
        从队列中获取爬取结果
        对结果进行解析后，进入item处理
        item处理完成之后，进入pipe持久化
        """
        key = self.name + ":out"
        while True:
            data = await self.queue.lpop(key)
            if data:
                job = json.loads(data)
                response = await self.process_response(job)
                item = await self.process_item(response)
                await self.pipe_item(item)
            else:
                await asyncio.sleep(0.1)

    async def pipe_item(self, items):
        """
        持久化操作
        """
        pass

    async def process_item(self, response):
        """
        处理解析数据后的逻辑，生成新请求&存储数据
        """
        return response

    async def process_failed_response(self, job):
        return job

    async def parse(self, response):
        return response

    async def process_response(self, job):
        """
        处理response结果，解析数据进入下一步处理
        """
        try:
            request = job.get("request")
            response = job.get("response")
            if response.status in (200,201):
                func = request.get("callback")
                func = getattr(self, func)
                if func:
                    data = func(response)
                else:
                    data = self.parse(response)
                return data
            else:
                await self.process_failed_response(job)
        except:
            log.error(traceback.format_exc())
