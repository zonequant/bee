#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/9/10 15:53
# @Author  : Dominolu
# @File    : hdf5.py
# @Software: PyCharm
import time
import traceback

import pandas as pd
import pymysql
import datetime
from bee.config import cfg
from loguru import logger as log
db=pymysql.connect(host=cfg.mysql_host,port=3306,user='root',password='root_pwd',database='bee',charset='utf8')
# sql = "select * from liquidation"
# data=pd.read_sql(sql,con=db,index_col="ts")
# store = pd.HDFStore('store.h5')
# store["df"]=data
#
#
# store.close()
#
#
# data.to_hdf("store.h5",key='df', mode='w')



def main():
    try:
        log.info("start dump trades...")
        sql="select * from trades where broker=%s and date(ts)=%s"
        del_str="delete  from trades where broker=%s and date(ts)=%s"
        dt=get_date()
        for i in dt:
            min_date=i[1]
            today = datetime.date.today()
            if min_date<today:
                f = cfg.path + "/" + str(min_date) + ".h5"
                store = pd.HDFStore(f, mode="w", complevel=9)
                data = pd.read_sql(sql, con=db, params=i,index_col="ts")
                store[i[0]] = data
                store.close()
                log.info(f"{i[0]}-{str(min_date)} dumped.")
                db.ping(reconnect=True)
                try:
                    with db.cursor() as cursor:
                        cursor.execute(del_str,i)
                        db.commit()
                except:
                    db.rollback()

    except:
        log.error(traceback.format_exc())



def get_date():
    sql="select broker,min(date(ts)) as 'dt' from trades group by broker;"
    db.ping(reconnect=True)

    with db.cursor() as cursor:
        cursor.execute(sql)
        data=cursor.fetchall()
        return list(data)


if __name__ == '__main__':
    while True:
        main()
        time.sleep(60*60)