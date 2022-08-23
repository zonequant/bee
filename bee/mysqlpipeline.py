#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/8/13 14:27
# @Author  : Dominolu
# @File    : mysqlpipeline.py
# @Software: PyCharm
from bee.pipeline import Pipeline
import aiomysql
from  bee.sqlorm  import *
import asyncio

class Mysqlpipline(Pipeline):

    def __init__(self,loop,param):
        self.loop=loop
        self.__conn= self.loop.run_until_complete(self.create_pool(param))

    async def create_pool(self,kwargs):
        host=kwargs.get('host', 'localhost')
        port=int(kwargs.get('port', 3306))
        user=kwargs['user']
        pwd=kwargs['password']
        db=kwargs['db']
        conn = await aiomysql.connect(
            host=host,
            port=port,
            user=user,
            password=pwd,
            db=db,
            loop=self.loop
            )
        return conn

    async def process_item(self,table,item):
        data={"table":table,
              "rows":item}
        await insert(self.__conn,data)

    def select(self,sql,param):
        return  select(self.__conn,sql,param)