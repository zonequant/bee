#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/10/24 21:06
# @Author  : Dominolu
# @File    : chpipeline.py
# @Software: PyCharm

from bee.pipeline import Pipeline
from clickhouse_driver import Client
from  bee.sqlorm  import *

class ClickHousepipline(Pipeline):
    def __init__(self,loop,param):
        self.loop=loop
        self.__conn= self.create_pool(param)

    def create_pool(self,kwargs):
        host=kwargs.get('host', 'localhost')
        port=int(kwargs.get('port', 3306))
        user=kwargs['user']
        pwd=kwargs['password']
        db=kwargs['db']
        conn = Client(host=host,user=user,password=pwd,database=db)
        return conn


    def process_item(self,table,item):
        sql=f"insert into {table} values"
        self.__conn.execute(sql,item,types_check=True)
