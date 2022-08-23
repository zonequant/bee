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
from bee.mysqlpipeline import Mysqlpipline
from bee.tools import *
def get_config():
    # 获取本爬虫的配制文件
    cfg=configparser.ConfigParser()
    cfg.read("./config.ini")
    return cfg


class Spider(object):
    name = ""
    start_urls=[]
    request_param={"limit":1}
    delay_queue=dict()
    def __init__(self):
        # todo 初始化配置文件与redis
        self.loop = asyncio.get_event_loop()
        self.config= get_config()
        url=self.config["REDIS"].get("url")
        self.queue=aioredis.from_url(url,decode_responses=True)
        self.db=Mysqlpipline(self.loop,self.config["DB"])

    async def start_requests(self):
        """
        初始化爬虫任务
        检查队列
        断点继爬
        """
        await self.regist()
        for url in self.start_urls:
            await self.request(url)

    async def request(self, url, data=None, method="GET", callback="parse"):
        try:
            request = {
                "url": url,
                "method": method,
                "data": data,
                "callback": callback
            }
            key = self.name + ":in"
            await self.queue.lpush(key, json.dumps(request))
        except:
            log.error(data)

    def request_delay(self,t,**kwargs):
        job=self.delay_queue.get(t,None)
        if job:
            job.append(kwargs)
        else:
            self.delay_queue[t]=[kwargs]

    def run(self):
        self.loop.run_until_complete(self.start())

    async def start(self):
        """
        从队列中获取爬取结果
        对结果进行解析后，进入item处理
        item处理完成之后，进入pipe持久化
        """
        await self.start_requests()
        key = self.name + ":out"
        while True:
            data = await self.queue.lpop(key)
            if data:
                job = json.loads(data)
                log.debug(f"New response:{job['url']}")
                item = await self.process_response(job)
                if item:
                    await self.pipe_item(item)
            else:
                await asyncio.sleep(0.1)
            await self.next_request()


    async def next_request(self):
        t=sorted(self.delay_queue.keys())
        if len(t)>0:
            first=t[0]
            if first<get_timestamp_ms():
                jobs=self.delay_queue.pop(first)
                for i in jobs:
                    await self.request(i["url"],i["data"],i["method"],i["callback"])

    async  def regist(self):
        prex_name="bee:worker"
        prex_job_name="bee:worker:job:"
        worker=await self.queue.hgetall(prex_name)
        if worker:
            try:
                for k, v in worker.items():
                    key=prex_job_name+k
                    await self.queue.hset(key,self.name,json.dumps(self.request_param))
            except:
                log.debug(traceback.print_exc())

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
            status = job.get("status")
            if status in (200,201):
                func = job.get("callback")
                func = getattr(self, func)
                if func:
                    data = await func(job)
                else:
                    data = await self.parse(job)
                return data
            else:
                await self.process_failed_response(job)
        except:
            log.error(traceback.format_exc())
