#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/7/31 11:55
# @Author  : Dominolu
# @File    : worker.py
# @Software: PyCharm

import asyncio
import traceback
import socket
import aiohttp
import aioredis
from loguru import logger as log
from datetime import datetime
import json
from bee.config import cfg
from bee.tools import *
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'
}


class Woker(object):
    update_rate=5
    def __init__(self):
        self.signal = False
        self.queue=aioredis.from_url(cfg.REDIS,decode_responses=True)
        self.session=aiohttp.ClientSession()
        self.rateLimit={}
        self.id=get_hostname()
        self.count=0
        self.last_update=0

    async def update(self):
        if get_timestamp()-self.last_update>self.update_rate:
            data={"time":get_timestamp(),
                  "IP":get_host_ip()}
            self.queue.set(self.id,json.dumps(data))
            self.last_update=get_timestamp()

    async def fetch(self,req):
        '''
        aiohttp获取网页源码
        '''
        # async with sem:
        method=req["method"]
        url=req["url"]
        param=req["param"]
        try:
            if method=="GET":
                async with self.session.get(url,param=param, headers=headers) as resp:
                    if resp.status in [200, 201]:
                        req.data = await resp.text()
                        return req
            else:
                async with self.session.post(url, body=param, headers=headers) as resp:
                    if resp.status in [200, 201]:
                        req.data =await resp.text()
                        return req
        except Exception as e:
            log.error(traceback.format_exc())

    def delay(self,key):
        now = datetime.now()
        timestamp = datetime.timestamp(now)
        if key in self.rateLimit:
            elapsed = timestamp -self.rateLimit[key].get("last_Request",0)
            rateLimit=self.rateLimit[key].get("limit",0)
            if elapsed < rateLimit:
                return False
            else:
                return True
        else:
            return True

    async def request(self,key):
        """
        根据项目名称，获取对应的任务队列
        异步爬取后存到对应的队列中，由Spider管理解析数据并完成下一步任务
        """
        in_key=key+":in"
        out_key=key+":out"
        try:
            if self.delay(key):     #控制爬取频率
                job=json.loads((await self.queue.lpop(in_key)))
                log.debug(f"New Task:{key}-{job.url}")
                reponse=await self.fetch(job)
                if reponse:
                    self.rateLimit[key]["last_Request"]=int(datetime.timestamp(datetime.now()))  #爬取后更新最后一次访问时间
                    await self.queue.lpush(out_key,json.dumps(reponse))
                    self.count=self.count+1
                    log.debug(f"Task:{key}-{job.url} is complete.")
                else:
                    log.debug(f"Task:{key}-{job.url} is download error.")
        except:
            log.error(traceback.format_exc())

    def stop(self):
        self.signal=False

    async def start(self):
        self.signal=True
        await self.run()

    async def run(self):
        """
        根据Worker_name从redis获取对应的项目列表
        根据task_name 从redis 队珍中获取爬虫任务
        """
        log.info(f"{self.id}-Spide worder is started.")
        while self.signal:
            tasks = json.loads(await self.queue.get(self.id))
            try:
                for k,v in tasks.items():
                    self.rateLimit[k]["limit"]=int(v.get("limit",0))
                    await self.request(k)
            except:
                log.debug(traceback.print_exc())
            await self.update()
            await asyncio.sleep(0.1)
        log.info(f"{self.id}-Spide worder is stoped.")

if __name__ == "__main__":
    spider=Woker()
    loop = asyncio.run(spider.start())