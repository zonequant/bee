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

lock = asyncio.Lock()
class Woker(object):
    update_rate=5
    prex_job_name="bee:worker:job:"
    prex_name = "bee:worker"
    def __init__(self):
        self.signal = False
        self.loop = asyncio.get_event_loop()
        self.queue=aioredis.from_url(cfg.REDIS,decode_responses=True)
        self.session=None
        self.rateLimit={}
        self.last_request={}
        self.id=get_hostname()
        log.debug(self.id)
        self.count=0
        self.last_update=0

    async def get_session(self):
        if self.session is None:
            connector = aiohttp.TCPConnector()
            self.session = aiohttp.ClientSession(connector=connector)
        return self.session

    async def update(self):
        if get_timestamp_s()-self.last_update>self.update_rate:
            data={"time":get_timestamp_s(),
                  "IP":get_host_ip()}
            await self.queue.hset(self.prex_name,self.id,json.dumps(data))
            self.last_update=get_timestamp_s()

    async def fetch(self,req):
        '''
        aiohttp获取网页源码
        '''
        # async with sem:
        method=req["method"]
        url=req["url"]
        data=req["data"]
        try:
            session=await self.get_session()
            if method=="GET":
                async with session.get(url,params=data, headers=headers) as resp:
                    req["status"]=resp.status
                    req["response"] = await resp.text()
                    return req
            else:
                async with session.post(url, body=data, headers=headers) as resp:
                    req["status"] = resp.status
                    req["response"] =await resp.text()
                    return req
        except Exception as e:
            log.error(traceback.format_exc())

    async def delay(self,key):
        timestamp=get_timestamp_s()
        if key in self.rateLimit:
            elapsed = timestamp -self.last_request.get(key,0)
            rateLimit=self.rateLimit[key].get("limit",0)
            if elapsed >= rateLimit:
                return True
            else:
                return False
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
            if await self.delay(key):     #控制爬取频率
                job=(await self.queue.lpop(in_key))
                if job:
                    job=json.loads(job)
                    log.debug(f"New Task:{key}-{job['url']}")
                    reponse=await self.fetch(job)
                    if reponse:
                        self.last_request[key]=get_timestamp_s()  #爬取后更新最后一次访问时间
                        await self.queue.lpush(out_key,json.dumps(reponse))
                        self.count=self.count+1
                        log.debug(f"Task:{key}-{job['url']} is complete.")
                    else:
                        log.debug(f"Task:{key}-{job['url']} is download error.")
                        self.queue.lpush(in_key,json.dumps(job))
                        log.debug(f"Task:{key}-{job['url']} is repush")

        except:
            log.error(traceback.format_exc())

    def stop(self):
        self.signal=False


    def run(self):
        self.signal=True
        self.loop.run_until_complete(self.start())

    async def start(self):
        """
        根据Worker_name从redis获取对应的项目列表
        根据task_name 从redis 队珍中获取爬虫任务
        """
        log.info(f"{self.id}-Spide worder is started.")

        key=self.prex_job_name+self.id
        while self.signal:
            tasks = await self.queue.hgetall(key)
            if tasks:
                try:
                    for k,v in tasks.items():
                        v=json.loads(v)
                        self.rateLimit[k]=v
                        await self.request(k)
                except:
                    log.debug(traceback.print_exc())
            await self.update()
            await asyncio.sleep(0.1)
        log.info(f"{self.id}-Spide worder is stoped.")

if __name__ == "__main__":
    spider=Woker()
    spider.run()