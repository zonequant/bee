#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/8/13 15:55
# @Author  : Dominolu
# @File    : sqlorm.py
# @Software: PyCharm

async def execute(session,sql,param=None):
    with await session as conn:
        try:
            cur = await conn.cursor()
            if param:
                await cur.execute(sql,param)
            else:
                await cur.execute(sql)
            affetced = cur.rowcount
            await conn.commit()
            await cur.close()
        except BaseException as e:
            raise
        return affetced

async def select(session,sql,size=None):
    with await session as conn:
        cur = await conn.cursor()
        await cur.execute(sql)
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()
        return rs

async def insert(session,items):
    """
    {"table":"table1","rows":{"col1":"abc","col2":"abc"}}
    """
    table=items["table"]
    rows=items["rows"]
    fields=""
    values=""
    for k in rows.keys():
        fields=fields+k
        values=values+",%s"
    sql=f"insert into {table} ({fields}) values ({values})"
    result=await execute(session,sql,rows)
    return result


def delete(session,item):
    pass