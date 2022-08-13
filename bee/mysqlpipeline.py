#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/8/13 14:27
# @Author  : Dominolu
# @File    : mysqlpipeline.py
# @Software: PyCharm
from bee.pipeline import Pipeline
import aiomysql
from  bee.sqlorm  import *


class Mysqlpipline(Pipeline):

    def __init__(self, table,**kwargs):
        self.table_name = table
        self.__conn=self.create_pool(**kwargs)

    async def create_pool(**kwargs):
        __pool = await aiomysql.create_pool(
            host=kwargs.get('host', 'localhost'),
            port=kwargs.get('port', 3306),
            user=kwargs['user'],
            password=kwargs['password'],
            db=kwargs['db']
        )

    def process_item(self, item):
        data={"table":self.table_name,
              "rows":item}
        await execute(self.__conn,data)
