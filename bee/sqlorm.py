#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/8/13 15:55
# @Author  : Dominolu
# @File    : sqlorm.py
# @Software: PyCharm
import traceback

from loguru import logger as log

async def execute(conn,sql,param=None):
    async with conn.cursor() as cur:
        try:
            if param:
                if isinstance(param,list):
                    await cur.executemany(sql, param)
                else:
                    await cur.execute(sql,param)
            else:
                await cur.execute(sql)
            affetced = cur.rowcount
            await conn.commit()
            await cur.close()
            return affetced
        except :
            log.error(traceback.format_exc())


async def select(conn,sql,param=None):
    async with conn.cursor() as cur:
        try:
            if param:
                await cur.execute(sql,param)
            else:
                await cur.execute(sql)
            rs = await cur.fetchall()
            await cur.close()
            return list(rs)
        except:
            log.error(traceback.format_exc())


async def insert(session,items):
    """
    {"table":"table1","rows":[{"col1":"abc","col2":"abc"}]}
    """
    table=items["table"]
    rows=items["rows"]
    records=[]
    for i in rows:
        records.append(list(i.values()))

    fields = ",".join([",".join([str(k)]) for k in rows[0].keys()])
    values = ",".join([",".join(["%s"]) for k in rows[0].keys()])
    sql=f"insert into {table} ({fields}) values ({values})"
    result=await execute(session,sql,records)
    return result


def delete(session,item):
    pass